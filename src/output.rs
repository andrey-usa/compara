//! The files a run is asked to write: the HTML report, the JSON summary and
//! the export CSVs.
//!
//! Each is written beside its name under a temporary one and moved onto the
//! name when complete. A run that fails, is stopped or fills the disk leaves
//! whatever was at the name before, never half a report, and nothing reading
//! the name sees one being written.
//!
//! A symbolic link at the name is refused, not followed. The name a run is
//! given is the one file it writes there: in a shared directory (`/tmp`, a CI
//! workspace) a link someone else left at `report.html` would otherwise turn
//! the report into an overwrite of whatever the link points to. A FIFO or a
//! device named directly (`/dev/null`) is written as it is, since it cannot be
//! replaced and writing into it is what was asked.

use std::fs::{self, File, OpenOptions, Permissions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};

/// One output being written. `commit` puts it at its name; dropped without a
/// commit, it leaves nothing behind.
pub struct Output {
    dest: PathBuf,
    /// The temporary file `commit` moves onto `dest`, and the mode of the file
    /// it replaces; `None` when `dest` is written in place (a FIFO, a device).
    temp: Option<(PathBuf, Option<Permissions>)>,
    /// Taken and closed by `commit` before the rename: Windows renames an
    /// open file only when every handle on it allows it.
    file: Option<File>,
}

fn cannot(dest: &Path, e: &io::Error) -> Error {
    Error::new(format!("cannot write {}: {e}", dest.display()))
}

/// Refuses a symbolic link at `path`, the name of a file or a directory a run
/// was told to write; anything else, or nothing there, passes. The check a
/// run makes before it reads its inputs, so a refusal costs no compare; the
/// write itself checks again.
pub fn refuse_link(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_symlink() => {
            let to = fs::read_link(path)
                .map(|t| format!(" (to {})", t.display()))
                .unwrap_or_default();
            Err(Error::new(format!(
                "{} is a symbolic link{to}; compara writes only the file it is given, \
                 never through a link: name the file itself, or remove the link",
                path.display()
            )))
        }
        _ => Ok(()),
    }
}

impl Output {
    /// Opens `dest` for writing, as described in the module comment.
    pub fn create(dest: &Path) -> Result<Output> {
        refuse_link(dest)?;
        let replaced = match fs::symlink_metadata(dest) {
            Ok(meta) if meta.is_dir() => {
                return Err(Error::new(format!(
                    "cannot write {}: it is a directory; name a file",
                    dest.display()
                )));
            }
            Ok(meta) if !meta.is_file() => {
                let file = OpenOptions::new()
                    .write(true)
                    .open(dest)
                    .map_err(|e| cannot(dest, &e))?;
                return Ok(Output {
                    dest: dest.to_path_buf(),
                    temp: None,
                    file: Some(file),
                });
            }
            Ok(meta) => Some(meta.permissions()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(cannot(dest, &e)),
        };
        let name = dest.file_name().ok_or_else(|| {
            Error::new(format!("cannot write {}: it names no file", dest.display()))
        })?;
        let dir = match dest.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        for n in 0..16 {
            let mut part = std::ffi::OsString::from(".");
            part.push(name);
            part.push(format!(".{}-{n}.part", std::process::id()));
            let temp = dir.join(part);
            // Only where nothing is, so a link or a file at the temporary name
            // is never opened either.
            match OpenOptions::new().write(true).create_new(true).open(&temp) {
                Ok(file) => {
                    return Ok(Output {
                        dest: dest.to_path_buf(),
                        temp: Some((temp, replaced)),
                        file: Some(file),
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => {
                    return Err(Error::new(format!(
                        "cannot write {}: cannot make a file beside it to write into ({e}); \
                         compara writes there and moves the file into place, so the directory \
                         must be writable",
                        dest.display()
                    )));
                }
            }
        }
        Err(Error::new(format!(
            "cannot write {}: no free temporary name beside it",
            dest.display()
        )))
    }

    /// Puts the written file at its name, in one step.
    pub fn commit(mut self) -> Result<()> {
        if let Some(mut file) = self.file.take() {
            file.flush().map_err(|e| cannot(&self.dest, &e))?;
        }
        if let Some((temp, mode)) = self.temp.take() {
            let moved = mode
                .map_or(Ok(()), |m| fs::set_permissions(&temp, m))
                .and_then(|()| fs::rename(&temp, &self.dest));
            if let Err(e) = moved {
                let _ = fs::remove_file(&temp);
                return Err(cannot(&self.dest, &e));
            }
        }
        Ok(())
    }
}

impl Output {
    fn open(&mut self) -> io::Result<&mut File> {
        self.file
            .as_mut()
            .ok_or_else(|| io::Error::other("the output is already committed"))
    }
}

impl Write for Output {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.open()?.write(buf)
    }

    fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.open()?.write_all(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.open()?.flush()
    }
}

impl Drop for Output {
    fn drop(&mut self) {
        if let Some((temp, _)) = self.temp.take() {
            drop(self.file.take());
            let _ = fs::remove_file(temp);
        }
    }
}

/// Writes `bytes` to `dest` through an [`Output`].
pub fn write(dest: &Path, bytes: &[u8]) -> Result<()> {
    let mut out = Output::create(dest)?;
    out.write_all(bytes).map_err(|e| cannot(dest, &e))?;
    out.commit()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("compara-output-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn left_in(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        names.sort();
        names
    }

    #[test]
    fn writes_a_new_file_and_replaces_an_old_one_leaving_nothing_else() {
        let dir = scratch("new");
        let dest = dir.join("report.html");
        write(&dest, b"one").unwrap();
        write(&dest, b"two").unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"two");
        assert_eq!(left_in(&dir), ["report.html"]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn an_output_never_committed_leaves_the_old_file_as_it_was() {
        let dir = scratch("abandon");
        let dest = dir.join("summary.json");
        fs::write(&dest, b"before").unwrap();
        {
            let mut out = Output::create(&dest).unwrap();
            out.write_all(b"half of a").unwrap();
        }
        assert_eq!(fs::read(&dest).unwrap(), b"before");
        assert_eq!(left_in(&dir), ["summary.json"]);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_directory_is_refused() {
        let dir = scratch("dir");
        let err = write(&dir, b"x").unwrap_err().to_string();
        assert!(err.contains("directory"), "{err}");
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_link_at_the_name_is_refused_and_its_target_left_alone() {
        let dir = scratch("link");
        let target = dir.join("precious");
        fs::write(&target, b"keep me").unwrap();
        let dest = dir.join("report.html");
        std::os::unix::fs::symlink(&target, &dest).unwrap();
        let err = write(&dest, b"report").unwrap_err().to_string();
        assert!(err.contains("symbolic link"), "{err}");
        assert_eq!(fs::read(&target).unwrap(), b"keep me");
        assert!(
            fs::symlink_metadata(&dest)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        // A dangling link too: following it would create the file it names.
        let gone = dir.join("summary.json");
        std::os::unix::fs::symlink(dir.join("nowhere"), &gone).unwrap();
        assert!(write(&gone, b"x").is_err());
        assert!(!dir.join("nowhere").exists());
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn the_replaced_file_keeps_its_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("mode");
        let dest = dir.join("report.html");
        fs::write(&dest, b"old").unwrap();
        fs::set_permissions(&dest, Permissions::from_mode(0o600)).unwrap();
        write(&dest, b"new").unwrap();
        let mode = fs::metadata(&dest).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_device_is_written_in_place() {
        write(Path::new("/dev/null"), b"discarded").unwrap();
    }
}
