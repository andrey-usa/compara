//! Temporary directories left behind by runs that did not end on their own.
//!
//! A run's temporary directory goes when the run does, unless the run is
//! killed (SIGKILL, the OOM killer, an abort): then nothing of it runs to
//! remove it, and what `sortmerge` spilled -- both files' rows -- stays in
//! the temporary directory until someone looks. A run about to make a
//! temporary directory first removes such leftovers, and only those that are
//! provably compara's and provably over:
//!
//! - named as compara names them, `compara-<pid>-<8 hex>-<n>-<tag>`, with a
//!   tag whose contents are known;
//! - a directory, not a link, owned by this user and closed to every other;
//! - not locked: a run holds a lock on its directory for as long as it
//!   lives, and the kernel drops the lock however the run ends, in whatever
//!   pid namespace it ran;
//! - its process gone here;
//! - holding only the files compara writes there: no link, nothing else;
//! - nothing in it changed for a while: the directory, its subdirectories
//!   and every file in them. A directory whose name ends `-locked` was made
//!   by a run that locks it, and a minute is enough: it covers the moment
//!   between making the directory and locking it. One without was made by
//!   an earlier version, which took no lock; it may belong to a run still
//!   going in another pid namespace (containers sharing a `/tmp`), where its
//!   process id means nothing here, so it waits a day.
//!
//! Anything else is left as it is, and so is what cannot be removed; neither
//! is an error. A process id the system has handed on again reads as a live
//! run and keeps its directory until that process ends too.

use std::fs::{self, File};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};

/// How long a directory a run locks must have been left unchanged before it
/// is removed.
const SETTLED: Duration = Duration::from_secs(60);

/// The same for a directory of an earlier version, which no lock protects.
const SETTLED_UNLOCKED: Duration = Duration::from_secs(24 * 60 * 60);

/// The end of the name of a directory its run locks.
pub(super) const LOCKED: &str = "locked";

/// Removes from `dir` the temporary directories that runs which have ended
/// left behind; how many went.
pub(super) fn sweep(dir: &Path) -> usize {
    let Ok(entries) = fs::read_dir(dir) else {
        return 0;
    };
    let me = euid();
    let now = SystemTime::now();
    let mut removed = 0;
    for entry in entries.flatten() {
        let name = entry.file_name();
        let Some((pid, tag, locked)) = name.to_str().and_then(parse) else {
            continue;
        };
        let settled = if locked { SETTLED } else { SETTLED_UNLOCKED };
        if !alive(pid) && remove_if_over(&entry.path(), tag, me, now, settled) {
            removed += 1;
        }
    }
    removed
}

/// What a temporary directory compara makes is for, by its tag.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Tag {
    /// `sortmerge`'s spilled runs, under `a/` and `b/`.
    Sortmerge,
    /// An export on its way to object storage.
    Export,
    /// An output on its way to object storage or a URL.
    Out,
}

/// The process id and tag of a name `compara-<pid>-<8 hex>-<n>-<tag>`, and
/// whether it ends `-locked`.
fn parse(name: &str) -> Option<(i32, Tag, bool)> {
    let mut parts = name.strip_prefix("compara-")?.split('-');
    let (pid, salt, n, tag) = (parts.next()?, parts.next()?, parts.next()?, parts.next()?);
    let locked = match parts.next() {
        None => false,
        Some(LOCKED) => true,
        Some(_) => return None,
    };
    if parts.next().is_some() || !digits(pid) || !digits(n) {
        return None;
    }
    if salt.len() != 8 || !salt.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let pid: i32 = pid.parse().ok().filter(|p| *p > 0)?;
    let tag = match tag {
        "sortmerge" => Tag::Sortmerge,
        "export" => Tag::Export,
        "out" => Tag::Out,
        _ => return None,
    };
    Some((pid, tag, locked))
}

fn digits(s: &str) -> bool {
    !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit())
}

/// Whether a file of this name is one compara writes in a directory of
/// `tag`'s (inside `sub` of it, for sortmerge's sides).
fn ours(tag: Tag, sub: Option<&str>, name: &str) -> bool {
    match (tag, sub) {
        (Tag::Sortmerge, Some(_)) => {
            let Some(stem) = name.strip_suffix(".bin") else {
                return false;
            };
            if let Some(n) = stem.strip_prefix("run-") {
                return digits(n);
            }
            stem.strip_prefix("merge-")
                .and_then(|rest| rest.split_once('-'))
                .is_some_and(|(pass, n)| digits(pass) && digits(n))
        }
        (Tag::Export, None) => {
            matches!(name, "added.csv" | "removed.csv" | "changed.csv")
                || name
                    .strip_prefix(".compara-")
                    .and_then(|rest| rest.strip_suffix(".part"))
                    .and_then(|rest| rest.split_once('-'))
                    .is_some_and(|(pid, n)| digits(pid) && digits(n))
        }
        (Tag::Out, None) => name == "output",
        _ => false,
    }
}

/// Removes `path`, the directory of a run whose process is gone, if every
/// other check passes; whether it went.
fn remove_if_over(path: &Path, tag: Tag, me: u32, now: SystemTime, settled: Duration) -> bool {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return false;
    };
    if !meta.file_type().is_dir() || meta.uid() != me || meta.mode() & 0o077 != 0 {
        return false;
    }
    // Opened by name after those checks: it has to be the same directory.
    let Ok(dir) = File::open(path) else {
        return false;
    };
    let same = dir
        .metadata()
        .is_ok_and(|m| (m.dev(), m.ino()) == (meta.dev(), meta.ino()));
    // Refused while the run that made it lives; once taken, held until the
    // directory is gone, so two runs sweeping at once do not both work on it.
    if !same || dir.try_lock().is_err() {
        return false;
    }
    let Some((files, subdirs, newest)) = contents(path, tag) else {
        return false;
    };
    let quiet = meta
        .modified()
        .ok()
        .map(|t| newest.map_or(t, |n| n.max(t)))
        .and_then(|t| now.duration_since(t).ok())
        .is_some_and(|age| age >= settled);
    if !quiet {
        return false;
    }
    for file in &files {
        let _ = fs::remove_file(file);
    }
    for sub in &subdirs {
        let _ = fs::remove_dir(sub);
    }
    fs::remove_dir(path).is_ok()
}

/// What a directory's contents are: its files, its subdirectories, and the
/// last time any of them changed.
type Contents = (Vec<PathBuf>, Vec<PathBuf>, Option<SystemTime>);

/// The files and subdirectories of a directory of `tag`'s, when it holds
/// nothing compara does not write there, and when the last of them changed.
/// Types and times are read without following links, so a link is never
/// taken for what it points at.
fn contents(path: &Path, tag: Tag) -> Option<Contents> {
    let mut files = Vec::new();
    let mut subdirs = Vec::new();
    let mut newest: Option<SystemTime> = None;
    let mut saw = |entry: &fs::DirEntry| -> Option<()> {
        let changed = entry.metadata().ok()?.modified().ok()?;
        newest = Some(newest.map_or(changed, |t| t.max(changed)));
        Some(())
    };
    for entry in fs::read_dir(path).ok()? {
        let entry = entry.ok()?;
        let kind = entry.file_type().ok()?;
        let name = entry.file_name();
        let name = name.to_str()?;
        if kind.is_dir() && tag == Tag::Sortmerge && matches!(name, "a" | "b") {
            for inner in fs::read_dir(entry.path()).ok()? {
                let inner = inner.ok()?;
                let inner_name = inner.file_name();
                if !inner.file_type().ok()?.is_file()
                    || !ours(tag, Some(name), inner_name.to_str()?)
                {
                    return None;
                }
                saw(&inner)?;
                files.push(inner.path());
            }
            saw(&entry)?;
            subdirs.push(entry.path());
        } else if kind.is_file() && ours(tag, None, name) {
            saw(&entry)?;
            files.push(entry.path());
        } else {
            return None;
        }
    }
    Some((files, subdirs, newest))
}

unsafe extern "C" {
    fn geteuid() -> u32;
    fn kill(pid: i32, sig: std::ffi::c_int) -> std::ffi::c_int;
}

fn euid() -> u32 {
    // SAFETY: geteuid takes nothing and cannot fail.
    unsafe { geteuid() }
}

/// Whether process `pid` exists here. One that belongs to another user
/// exists too: only "no such process" (ESRCH, 3 on Linux, macOS and the
/// BSDs) says it is gone.
fn alive(pid: i32) -> bool {
    const ESRCH: i32 = 3;
    // SAFETY: signal 0 is not sent; kill only checks that the process exists
    // and may be signalled. `parse` refuses 0 and negative ids, which would
    // name process groups.
    if unsafe { kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() != Some(ESRCH)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_compara_s_own_names_are_taken() {
        assert_eq!(
            parse("compara-41-0badc0de-3-sortmerge"),
            Some((41, Tag::Sortmerge, false))
        );
        assert_eq!(
            parse("compara-41-0badc0de-3-sortmerge-locked"),
            Some((41, Tag::Sortmerge, true))
        );
        assert_eq!(
            parse("compara-41-0badc0de-0-export-locked"),
            Some((41, Tag::Export, true))
        );
        for name in [
            "compara-0-0badc0de-3-sortmerge",
            "compara--1-0badc0de-3-sortmerge",
            "compara-41-0badc0d-3-sortmerge",
            "compara-41-0badc0de-3-stdin",
            "compara-41-0badc0de-3-sortmerge-x",
            "compara-41-0badc0de-3-sortmerge-locked-locked",
            "compara-41-0badc0de-3-locked",
            "compara-99999999999-0badc0de-3-sortmerge",
            "compara-sortmerge-test-41-0",
            "compara-41-0badc0de--sortmerge",
        ] {
            assert_eq!(parse(name), None, "{name}");
        }
        assert!(ours(Tag::Sortmerge, Some("a"), "run-12.bin"));
        assert!(ours(Tag::Sortmerge, Some("b"), "merge-0-3.bin"));
        assert!(!ours(Tag::Sortmerge, None, "run-12.bin"));
        assert!(!ours(Tag::Sortmerge, Some("a"), "run-.bin"));
        assert!(!ours(Tag::Sortmerge, Some("a"), "merge-0.bin"));
        assert!(ours(Tag::Export, None, ".compara-7-0.part"));
        assert!(!ours(Tag::Export, None, "notes.csv"));
    }

    #[test]
    fn this_process_is_alive_and_an_ended_one_is_not() {
        assert!(alive(std::process::id() as i32));
        let mut child = std::process::Command::new("true").spawn().unwrap();
        let pid = child.id() as i32;
        child.wait().unwrap();
        assert!(!alive(pid));
    }
}
