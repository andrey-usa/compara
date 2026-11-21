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
//! replaced and writing into it is what was asked. A file the user may write
//! at a name that takes no new file is written in place too, as it was
//! before outputs landed in one step: in a directory they may not add to, in
//! a sticky one where the file is another user's, or where the file is
//! mounted at the name. There a failed run can leave it half written.

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
    /// The file the check found at `dest`, if one was there: written in
    /// place when the name takes no rename.
    checked: Option<fs::Metadata>,
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
        let mut checked = None;
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
                #[cfg(unix)]
                is_the_checked_file(&file, &meta).map_err(|e| cannot(dest, &e))?;
                return Ok(Output {
                    dest: dest.to_path_buf(),
                    temp: None,
                    checked: None,
                    file: Some(file),
                });
            }
            Ok(meta) => {
                // A rename ignores the mode of the file it replaces; writing
                // to the name does not. Asking the system whether this user
                // may write it keeps a write-protected file protected.
                OpenOptions::new()
                    .write(true)
                    .open(dest)
                    .map_err(|e| match e.kind() {
                        io::ErrorKind::PermissionDenied => Error::new(format!(
                            "cannot write {}: {e}; the file is write-protected, and compara \
                             replaces what is at the name: make it writable or name another file",
                            dest.display()
                        )),
                        _ => cannot(dest, &e),
                    })?;
                let mode = meta.permissions();
                checked = Some(meta);
                Some(mode)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => None,
            Err(e) => return Err(cannot(dest, &e)),
        };
        if dest.file_name().is_none() {
            return Err(Error::new(format!(
                "cannot write {}: it names no file",
                dest.display()
            )));
        }
        let dir = match dest.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        for n in 0..16 {
            // A name of its own, not one built from the output's: a file name
            // can be 255 bytes long and the temporary has to fit beside it.
            let temp = dir.join(format!(".compara-{}-{n}.part", std::process::id()));
            let mut open = OpenOptions::new();
            // Only where nothing is, so a link or a file at the temporary name
            // is never opened either.
            open.write(true).create_new(true);
            // Private while it is written when what it replaces is private;
            // the mode of the replaced file is put on it just before the move.
            #[cfg(unix)]
            if replaced.is_some() {
                std::os::unix::fs::OpenOptionsExt::mode(&mut open, 0o600);
            }
            match open.open(&temp) {
                Ok(file) => {
                    return Ok(Output {
                        dest: dest.to_path_buf(),
                        temp: Some((temp, replaced)),
                        checked,
                        file: Some(file),
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) if e.kind() == io::ErrorKind::PermissionDenied && checked.is_some() => {
                    // A directory this user may not add to, holding a file
                    // they may write: written in place, as before.
                    let file = OpenOptions::new()
                        .write(true)
                        .open(dest)
                        .map_err(|e| cannot(dest, &e))?;
                    #[cfg(unix)]
                    if let Some(meta) = &checked {
                        is_the_checked_file(&file, meta).map_err(|e| cannot(dest, &e))?;
                    }
                    file.set_len(0).map_err(|e| cannot(dest, &e))?;
                    return Ok(Output {
                        dest: dest.to_path_buf(),
                        temp: None,
                        checked: None,
                        file: Some(file),
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::NotFound => {
                    return Err(Error::new(format!(
                        "cannot write {}: the directory {} does not exist; create it first",
                        dest.display(),
                        dir.display()
                    )));
                }
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

    /// Puts the written file at its name, in one step where the name takes a
    /// rename.
    pub fn commit(mut self) -> Result<()> {
        if let Some(mut file) = self.file.take() {
            file.flush().map_err(|e| cannot(&self.dest, &e))?;
        }
        if let Some((temp, mode)) = self.temp.take() {
            let moved = mode
                .map_or(Ok(()), |m| fs::set_permissions(&temp, m))
                .and_then(|()| fs::rename(&temp, &self.dest));
            let landed = match (moved, &self.checked) {
                (Ok(()), _) => return Ok(()),
                // The name takes no rename though the file at it may be
                // written: another user's file in a sticky directory (EPERM),
                // or a file mounted at the name (EBUSY). Written in place, as
                // before outputs landed in one step.
                (Err(e), Some(meta))
                    if matches!(
                        e.kind(),
                        io::ErrorKind::PermissionDenied | io::ErrorKind::ResourceBusy
                    ) =>
                {
                    copy_in_place(&temp, &self.dest, meta)
                }
                (Err(e), _) => Err(e),
            };
            let _ = fs::remove_file(&temp);
            landed.map_err(|e| cannot(&self.dest, &e))?;
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

/// A file opened by name is the one a check of the name looked at: the same
/// device and inode. Between the check and the open the name could be swapped
/// for a link to something else; the open does not truncate, so a mismatch
/// costs nothing but the refusal.
#[cfg(unix)]
fn is_the_checked_file(file: &File, checked: &fs::Metadata) -> io::Result<()> {
    use std::os::unix::fs::MetadataExt;
    let opened = file.metadata()?;
    if opened.dev() == checked.dev() && opened.ino() == checked.ino() {
        Ok(())
    } else {
        Err(io::Error::other("it changed while it was being opened"))
    }
}

/// Writes the finished `temp` into the file at `dest`, the one the check
/// found, in place: for a name that takes no rename.
fn copy_in_place(temp: &Path, dest: &Path, checked: &fs::Metadata) -> io::Result<()> {
    let mut to = OpenOptions::new().write(true).open(dest)?;
    #[cfg(unix)]
    is_the_checked_file(&to, checked)?;
    #[cfg(not(unix))]
    let _ = checked;
    to.set_len(0)?;
    io::copy(&mut File::open(temp)?, &mut to)?;
    to.flush()
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

    #[test]
    fn a_name_of_any_valid_length_gets_a_temporary_beside_it() {
        // 255 bytes is the longest name most file systems take; the temporary
        // name must not depend on the length of the one it stands for.
        let dir = scratch("long");
        let dest = dir.join(format!("{}.html", "x".repeat(250)));
        write(&dest, b"one").unwrap();
        write(&dest, b"two").unwrap();
        assert_eq!(fs::read(&dest).unwrap(), b"two");
        assert_eq!(left_in(&dir).len(), 1);
        fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_missing_directory_is_named_as_missing() {
        let dir = scratch("missing");
        let err = write(&dir.join("nope").join("r.html"), b"x")
            .unwrap_err()
            .to_string();
        assert!(err.contains("does not exist"), "{err}");
        assert!(!err.contains("must be writable"), "{err}");
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn the_temporary_of_a_private_file_is_private_while_it_is_written() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("private");
        let dest = dir.join("report.html");
        fs::write(&dest, b"old").unwrap();
        fs::set_permissions(&dest, Permissions::from_mode(0o600)).unwrap();
        let out = Output::create(&dest).unwrap();
        let (temp, _) = out.temp.as_ref().unwrap();
        let mode = fs::metadata(temp).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "another user could read it before it is moved");
        drop(out);
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_write_protected_file_is_not_replaced() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("protected");
        let dest = dir.join("report.html");
        fs::write(&dest, b"old").unwrap();
        fs::set_permissions(&dest, Permissions::from_mode(0o444)).unwrap();
        // A user the modes do not bind (root) may write it; nothing to refuse.
        if OpenOptions::new().write(true).open(&dest).is_ok() {
            fs::remove_dir_all(dir).unwrap();
            return;
        }
        let err = write(&dest, b"new").unwrap_err().to_string();
        assert!(err.contains("write-protected"), "{err}");
        assert_eq!(fs::read(&dest).unwrap(), b"old");
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_writable_file_in_a_directory_that_takes_no_new_file_is_written_in_place() {
        use std::os::unix::fs::PermissionsExt;
        let dir = scratch("locked");
        let dest = dir.join("report.html");
        fs::write(&dest, b"the old report, longer than the new").unwrap();
        fs::set_permissions(&dir, Permissions::from_mode(0o555)).unwrap();
        // A user the modes do not bind (root) may add to it: nothing to show.
        let binds = fs::write(dir.join("probe"), b"").is_err();
        if binds {
            write(&dest, b"new").unwrap();
            assert_eq!(fs::read(&dest).unwrap(), b"new");
            assert_eq!(left_in(&dir), ["report.html"]);
        }
        fs::set_permissions(&dir, Permissions::from_mode(0o755)).unwrap();
        fs::remove_dir_all(dir).unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn a_file_opened_in_place_must_be_the_one_that_was_checked() {
        let dir = scratch("swapped");
        let (a, b) = (dir.join("a"), dir.join("b"));
        fs::write(&a, b"a").unwrap();
        fs::write(&b, b"b").unwrap();
        let checked = fs::symlink_metadata(&a).unwrap();
        let other = OpenOptions::new().write(true).open(&b).unwrap();
        let same = OpenOptions::new().write(true).open(&a).unwrap();
        assert!(is_the_checked_file(&other, &checked).is_err());
        assert!(is_the_checked_file(&same, &checked).is_ok());
        fs::remove_dir_all(dir).unwrap();
    }
}
