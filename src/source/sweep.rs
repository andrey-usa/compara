//! Temporary files and directories left behind by runs that did not end on
//! their own.
//!
//! A run's temporary files and directories go when the run does, unless the
//! run is killed (SIGKILL, the OOM killer, an abort): then nothing of it
//! runs to remove them, and what they hold -- standard input, a download, a
//! decompressed copy, the rows `sortmerge` spilled -- stays in the temporary
//! directory until someone looks. A run about to make a temporary file or
//! directory first removes such leftovers, and only those that are provably
//! compara's and provably over:
//!
//! - named as compara names them, `compara-<pid>-<8 hex>-<n>-<tag>`, with a
//!   tag compara uses: a directory's says what it holds, a file's what it
//!   is;
//! - a directory or a file as the tag says, not a link, owned by this user
//!   and closed to every other; a file with no other name;
//! - not locked: a run holds a lock on each of its temporary files and
//!   directories for as long as it uses them, and the kernel drops the lock
//!   however the run ends, in whatever pid namespace it ran;
//! - its process gone here;
//! - for a directory, holding only the files compara writes there: no link,
//!   nothing else;
//! - nothing of it changed for a while: the file, or the directory, its
//!   subdirectories and every file in them. A name that ends `-locked` was
//!   made by a run that locks it, and a minute is enough: it covers the
//!   moment between making the file or directory and locking it. One without
//!   was made by an earlier version, which took no lock; it may belong to a
//!   run still going in another pid namespace (containers sharing a `/tmp`),
//!   where its process id means nothing here, so it waits a day.
//!
//! Anything else is left as it is, and so is what cannot be removed; neither
//! is an error. A process id the system has handed on again reads as a live
//! run and keeps its directory until that process ends too.

use std::ffi::{CStr, CString};
use std::fs::{self, File};
use std::io;
use std::mem::MaybeUninit;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// How long a directory a run locks must have been left unchanged before it
/// is removed.
const SETTLED: Duration = Duration::from_secs(60);

/// The same for a directory of an earlier version, which no lock protects.
const SETTLED_UNLOCKED: Duration = Duration::from_secs(24 * 60 * 60);

/// The end of the name of a directory its run locks.
pub(super) const LOCKED: &str = "locked";

/// Removes from `dir` the temporary files and directories that runs which
/// have ended left behind; how many went.
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
        if alive(pid) {
            continue;
        }
        let gone = if tag.is_file() {
            remove_file_if_over(&entry.path(), me, now, settled)
        } else {
            remove_if_over(&entry.path(), tag, me, now, settled)
        };
        if gone {
            removed += 1;
        }
    }
    removed
}

/// What a temporary directory or file compara makes is for, by its tag.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Tag {
    /// A directory: `sortmerge`'s spilled runs, under `a/` and `b/`.
    Sortmerge,
    /// A directory: an export on its way to object storage.
    Export,
    /// A directory: an output on its way to object storage or a URL.
    Out,
    /// A file: what came in on standard input.
    Stdin,
    /// A file: a gzip or zstd input, decompressed.
    Plain,
    /// A file: an object or a URL, downloaded.
    Download,
    /// A file: one object under a prefix, downloaded whole before it is
    /// added to the table it is part of.
    Part,
    /// A file: the parts of a table -- a local directory or glob, or a
    /// prefix in object storage -- as one.
    Combined,
}

impl Tag {
    fn is_file(self) -> bool {
        !matches!(self, Tag::Sortmerge | Tag::Export | Tag::Out)
    }
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
        "stdin" => Tag::Stdin,
        "plain" => Tag::Plain,
        "download" => Tag::Download,
        "part" => Tag::Part,
        "combined" => Tag::Combined,
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
    let Ok(dir) = reopen(path, true) else {
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
    #[cfg(test)]
    tests::at(tests::Point::Locked, path);
    // From here on the directory is the one opened, this user's and closed
    // to every other, and what is in it is read and removed through it. Its
    // name may have been swapped since, for a link to a directory of
    // someone else's: by the name, what is in that one would be removed.
    let dir = Dir(dir);
    let Some(held) = contents(&dir, tag) else {
        return false;
    };
    let quiet = meta
        .modified()
        .ok()
        .map(|t| held.newest.map_or(t, |n| n.max(t)))
        .and_then(|t| now.duration_since(t).ok())
        .is_some_and(|age| age >= settled);
    if !quiet {
        return false;
    }
    for (_, sub, files) in &held.subdirs {
        for file in files {
            sub.remove(file, false);
        }
    }
    for file in &held.files {
        dir.remove(file, false);
    }
    for (name, _, _) in &held.subdirs {
        dir.remove(name, true);
    }
    // The directory itself by its name, in the temporary directory: what a
    // swap can have put there is at most an empty directory to remove, or a
    // link, which is not followed and which this fails on; anyone who can
    // swap the name may remove either already.
    fs::remove_dir(path).is_ok()
}

/// Removes `path`, a temporary file of a run whose process is gone, if every
/// other check passes; whether it went.
fn remove_file_if_over(path: &Path, me: u32, now: SystemTime, settled: Duration) -> bool {
    let Ok(meta) = fs::symlink_metadata(path) else {
        return false;
    };
    // A file compara made has the one name it gave it: one with another is
    // somebody's file too.
    if !meta.file_type().is_file()
        || meta.uid() != me
        || meta.mode() & 0o077 != 0
        || meta.nlink() != 1
    {
        return false;
    }
    // Opened by name after those checks: it has to be the same file.
    let Ok(file) = reopen(path, false) else {
        return false;
    };
    let same = file
        .metadata()
        .is_ok_and(|m| (m.dev(), m.ino()) == (meta.dev(), meta.ino()));
    // Refused while the run that made it uses it; once taken, held until the
    // file is gone.
    if !same || file.try_lock().is_err() {
        return false;
    }
    // When it last changed is read under the lock, from the file opened.
    let quiet = file
        .metadata()
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| now.duration_since(t).ok())
        .is_some_and(|age| age >= settled);
    // By its name, in the temporary directory: a link swapped in is not
    // followed, and anyone who can swap the name may remove what is there.
    quiet && fs::remove_file(path).is_ok()
}

/// Whether `dir`, a directory a run has just made and opened, is one of
/// this user's closed to every other: what a run made is, and what another
/// user may have renamed onto its name in the meantime is not.
pub(super) fn is_own_private_dir(dir: &File) -> bool {
    dir.metadata()
        .is_ok_and(|m| m.is_dir() && m.uid() == euid() && m.mode() & 0o077 == 0)
}

/// Where a test swaps the name of a temporary directory a run has made and
/// holds open, before anything is written in it.
#[cfg(test)]
pub(super) fn made(path: &Path) {
    tests::at(tests::Point::Made, path);
}

/// Opens `path`, a directory (`dir`) or a file by the look taken at it, or
/// by its making, to see whether it is still the one looked at, or to lock
/// it. Where anyone may rename in the temporary directory (no sticky bit),
/// the name may have been swapped since: a link put there is not followed,
/// and a pipe is not waited on for a writer; the open fails, or what it
/// opens is not the one looked at.
pub(super) fn reopen(path: &Path, dir: bool) -> std::io::Result<File> {
    #[cfg(test)]
    tests::at(tests::Point::Open, path);
    let mut flags = libc::O_NOFOLLOW | libc::O_NONBLOCK;
    if dir {
        flags |= libc::O_DIRECTORY;
    }
    fs::OpenOptions::new()
        .read(true)
        .custom_flags(flags)
        .open(path)
}

/// What a directory of `tag`'s holds, read through it: the names of its
/// files, its subdirectories (opened) with the names of theirs, and the last
/// time any of them changed.
struct Contents {
    files: Vec<CString>,
    subdirs: Vec<(CString, Dir, Vec<CString>)>,
    newest: Option<SystemTime>,
}

/// What a directory of `tag`'s holds, when it is nothing compara does not
/// write there. Types and times are read without following links, so a
/// link is never taken for what it points at.
fn contents(dir: &Dir, tag: Tag) -> Option<Contents> {
    let mut held = Contents {
        files: Vec::new(),
        subdirs: Vec::new(),
        newest: None,
    };
    let mut saw = |st: &libc::stat| -> Option<()> {
        let changed = modified(st)?;
        held.newest = Some(held.newest.map_or(changed, |t| t.max(changed)));
        Some(())
    };
    let mut files = Vec::new();
    let mut subdirs = Vec::new();
    for name in dir.names()? {
        let st = dir.stat(&name)?;
        let kind = st.st_mode & libc::S_IFMT;
        let text = name.to_str().ok()?;
        if kind == libc::S_IFDIR && tag == Tag::Sortmerge && matches!(text, "a" | "b") {
            // Opened without following a link: it has to be the one looked at.
            let sub = dir.open_dir(&name)?;
            let opened = sub.stat(c".")?;
            if (opened.st_dev, opened.st_ino) != (st.st_dev, st.st_ino) {
                return None;
            }
            let mut inner_files = Vec::new();
            for inner in sub.names()? {
                let inner_st = sub.stat(&inner)?;
                if inner_st.st_mode & libc::S_IFMT != libc::S_IFREG
                    || !ours(tag, Some(text), inner.to_str().ok()?)
                {
                    return None;
                }
                saw(&inner_st)?;
                inner_files.push(inner);
            }
            saw(&st)?;
            subdirs.push((name, sub, inner_files));
        } else if kind == libc::S_IFREG && ours(tag, None, text) {
            saw(&st)?;
            files.push(name);
        } else {
            return None;
        }
    }
    held.files = files;
    held.subdirs = subdirs;
    Some(held)
}

/// When what `st` describes last changed, as `Metadata::modified` reads it.
fn modified(st: &libc::stat) -> Option<SystemTime> {
    let nanos = Duration::from_nanos(u64::try_from(st.st_mtime_nsec).ok()?);
    let secs = st.st_mtime;
    match u64::try_from(secs) {
        Ok(after) => UNIX_EPOCH.checked_add(Duration::from_secs(after)),
        Err(_) => UNIX_EPOCH.checked_sub(Duration::from_secs(secs.unsigned_abs())),
    }?
    .checked_add(nanos)
}

/// An open directory, what is in it named relative to it: whatever becomes
/// of the name it was opened by, every look, open and removal is in this
/// one.
#[derive(Debug)]
pub(super) struct Dir(pub(super) File);

impl Dir {
    fn fd(&self) -> std::ffi::c_int {
        self.0.as_raw_fd()
    }

    /// `name` in it opened with `flags` (and `mode`, for one made), never
    /// through a link at the name and closed on exec.
    fn open_at(&self, name: &CStr, flags: std::ffi::c_int, mode: libc::c_uint) -> io::Result<File> {
        // SAFETY: the name is NUL-terminated and `self.fd()` is open; the
        // mode is passed as the unsigned int a variadic argument of type
        // mode_t is promoted to, and read only with O_CREAT.
        let fd = unsafe {
            libc::openat(
                self.fd(),
                name.as_ptr(),
                flags | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                mode,
            )
        };
        if fd < 0 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: openat succeeded, so `fd` is a new descriptor no one else
        // holds; the File closes it.
        Ok(unsafe { File::from_raw_fd(fd) })
    }

    /// Makes the file `name` in it, for this user alone (0600), and opens it
    /// to write and read back: only where nothing is, so a file or a link
    /// already at the name is not opened. Every file compara writes in a
    /// temporary directory of its own is made here.
    pub(super) fn create(&self, name: &CStr) -> io::Result<File> {
        self.open_at(name, libc::O_RDWR | libc::O_CREAT | libc::O_EXCL, 0o600)
    }

    /// The file `name` in it, to read; not through a link. What is in a
    /// directory of this user's alone was put there by this user.
    pub(super) fn open(&self, name: &CStr) -> io::Result<File> {
        self.open_at(name, libc::O_RDONLY, 0)
    }

    /// Makes the directory `name` in it, for this user alone, and opens it.
    pub(super) fn make_dir(&self, name: &CStr) -> io::Result<Dir> {
        // SAFETY: the name is NUL-terminated and `self.fd()` is open.
        if unsafe { libc::mkdirat(self.fd(), name.as_ptr(), 0o700) } != 0 {
            return Err(io::Error::last_os_error());
        }
        self.open_at(
            name,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NONBLOCK,
            0,
        )
        .map(Dir)
    }

    /// Moves `from` in it to `to` in it, in one step, replacing what is at
    /// `to` (a link there is replaced, not followed).
    pub(super) fn rename(&self, from: &CStr, to: &CStr) -> io::Result<()> {
        // SAFETY: both names are NUL-terminated and `self.fd()` is open.
        let done = unsafe { libc::renameat(self.fd(), from.as_ptr(), self.fd(), to.as_ptr()) };
        if done != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Removes what is in it, and in the directories in it down to `depth`
    /// levels, through it: never by a name outside it, never through a link.
    pub(super) fn clear(&self, depth: usize) {
        for name in self.names().unwrap_or_default() {
            let is_dir = self
                .stat(&name)
                .is_some_and(|st| st.st_mode & libc::S_IFMT == libc::S_IFDIR);
            if is_dir {
                if let Some(below) = depth.checked_sub(1)
                    && let Some(sub) = self.open_dir(&name)
                {
                    sub.clear(below);
                }
                self.remove(&name, true);
            } else {
                self.remove(&name, false);
            }
        }
    }

    /// The names in it, but `.` and `..`; `None` when they cannot all be
    /// read.
    pub(super) fn names(&self) -> Option<Vec<CString>> {
        // A descriptor of the listing's own, which it reads from and closes:
        // this one keeps its place, and its lock.
        // SAFETY: the name is a NUL-terminated string; `self.fd()` is open
        // for as long as `self` lives.
        let fd = unsafe {
            libc::openat(
                self.fd(),
                c".".as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
            )
        };
        if fd < 0 {
            return None;
        }
        // SAFETY: `fd` is an open directory no one else holds; from here the
        // stream owns it, and `closedir` below closes both.
        let stream = unsafe { libc::fdopendir(fd) };
        if stream.is_null() {
            // SAFETY: fdopendir failed, so `fd` is still this function's.
            unsafe { libc::close(fd) };
            return None;
        }
        let mut names = Vec::new();
        let complete = loop {
            // The end of the listing leaves errno as it was, an error sets
            // it: it is cleared first to tell the two apart.
            set_errno(0);
            // SAFETY: `stream` is open until the `closedir` below.
            let entry = unsafe { libc::readdir(stream) };
            if entry.is_null() {
                break std::io::Error::last_os_error().raw_os_error() == Some(0);
            }
            // SAFETY: readdir returned an entry, whose name is NUL-terminated
            // and valid until the next readdir or closedir on `stream`; it is
            // copied before either.
            let name = unsafe { CStr::from_ptr((*entry).d_name.as_ptr()) };
            if name != c"." && name != c".." {
                names.push(name.to_owned());
            }
        };
        // SAFETY: `stream` came from fdopendir and is closed once, here.
        unsafe { libc::closedir(stream) };
        complete.then_some(names)
    }

    /// What `name` in it is, itself and not what a link points at (`.`:
    /// this directory).
    fn stat(&self, name: &CStr) -> Option<libc::stat> {
        let mut st = MaybeUninit::<libc::stat>::uninit();
        // SAFETY: the name is NUL-terminated, `st` has room for a stat, and
        // `self.fd()` is open.
        let done = unsafe {
            libc::fstatat(
                self.fd(),
                name.as_ptr(),
                st.as_mut_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
            )
        };
        // SAFETY: fstatat succeeded, so it filled `st` in.
        (done == 0).then(|| unsafe { st.assume_init() })
    }

    /// The directory `name` in it, as `reopen` opens one: not through a
    /// link, not waiting on a pipe.
    fn open_dir(&self, name: &CStr) -> Option<Dir> {
        // SAFETY: the name is NUL-terminated and `self.fd()` is open.
        let fd = unsafe {
            libc::openat(
                self.fd(),
                name.as_ptr(),
                libc::O_RDONLY
                    | libc::O_DIRECTORY
                    | libc::O_NOFOLLOW
                    | libc::O_NONBLOCK
                    | libc::O_CLOEXEC,
            )
        };
        // SAFETY: openat succeeded, so `fd` is a new descriptor no one else
        // holds; the File closes it.
        (fd >= 0).then(|| Dir(unsafe { File::from_raw_fd(fd) }))
    }

    /// Removes `name` from it, a directory (`dir`) or a file; whether it
    /// went.
    pub(super) fn remove(&self, name: &CStr, dir: bool) -> bool {
        let flags = if dir { libc::AT_REMOVEDIR } else { 0 };
        // SAFETY: the name is NUL-terminated and `self.fd()` is open.
        unsafe { libc::unlinkat(self.fd(), name.as_ptr(), flags) == 0 }
    }
}

/// Sets this thread's errno.
fn set_errno(value: std::ffi::c_int) {
    #[cfg(any(
        target_os = "linux",
        target_os = "hurd",
        target_os = "dragonfly",
        target_os = "emscripten"
    ))]
    // SAFETY: the address of this thread's errno, valid while it lives.
    unsafe {
        *libc::__errno_location() = value;
    }
    #[cfg(any(target_os = "android", target_os = "netbsd", target_os = "openbsd"))]
    // SAFETY: the same.
    unsafe {
        *libc::__errno() = value;
    }
    #[cfg(any(target_vendor = "apple", target_os = "freebsd"))]
    // SAFETY: the same.
    unsafe {
        *libc::__error() = value;
    }
    // Elsewhere errno is left as it is: a listing ends with what an earlier
    // call left there, when that was an error, read as one, and nothing in
    // the directory is removed.
    #[cfg(not(any(
        target_os = "linux",
        target_os = "hurd",
        target_os = "dragonfly",
        target_os = "emscripten",
        target_os = "android",
        target_os = "netbsd",
        target_os = "openbsd",
        target_vendor = "apple",
        target_os = "freebsd"
    )))]
    let _ = value;
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
    use std::cell::Cell;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};
    use std::os::unix::fs::{PermissionsExt, symlink};
    use std::path::PathBuf;
    use std::process::Command;
    use std::sync::{Mutex, mpsc};

    /// Where a test swaps a name.
    #[derive(Clone, Copy, PartialEq)]
    pub(super) enum Point {
        /// Once it has been looked at (or made), before it is opened.
        Open,
        /// Once the directory at it has been opened and locked, before what
        /// is in it is read.
        Locked,
        /// Once a new temporary directory has been made and opened, before
        /// anything is written in it.
        Made,
    }

    /// What a test does to a name at a point: what another user can do in a
    /// temporary directory open to all without the sticky bit.
    type Swap = (Point, fn(&Path));

    thread_local! {
        static SWAP: Cell<Option<Swap>> = const { Cell::new(None) };
    }

    pub(super) fn at(point: Point, path: &Path) {
        if let Some((at, swap)) = SWAP.get()
            && at == point
        {
            SWAP.set(None);
            swap(path);
        }
    }

    /// Moves what is at `path` aside, to `moved` beside it.
    fn aside(path: &Path) -> PathBuf {
        let moved = path.with_file_name("moved");
        fs::rename(path, &moved).unwrap();
        moved
    }

    fn to_pipe(path: &Path) {
        aside(path);
        let made = Command::new("mkfifo")
            .args(["-m", "600"])
            .arg(path)
            .status()
            .unwrap();
        assert!(made.success());
    }

    fn to_link(path: &Path) {
        aside(path);
        symlink("moved", path).unwrap();
    }

    /// Whether `remove` of `path`, with `swap` done to it between the look
    /// and the open, removed it: `None` when it was still waiting after 10 s.
    /// A wait on a pipe is then ended by opening its other end.
    fn raced(path: &Path, swap: fn(&Path), remove: fn(&Path) -> bool) -> Option<bool> {
        let (tx, rx) = mpsc::channel();
        let on = path.to_path_buf();
        let run = std::thread::spawn(move || {
            SWAP.set(Some((Point::Open, swap)));
            let _ = tx.send(remove(&on));
        });
        let got = rx.recv_timeout(Duration::from_secs(10)).ok();
        if got.is_none() {
            let _ = fs::OpenOptions::new()
                .write(true)
                .custom_flags(libc::O_NONBLOCK)
                .open(path);
        }
        run.join().unwrap();
        got
    }

    /// A private directory for one test, removed after.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(name: &str) -> Scratch {
            let dir = std::env::temp_dir()
                .join(format!("compara-sweep-unit-{}-{name}", std::process::id()));
            let _ = fs::remove_dir_all(&dir);
            fs::create_dir(&dir).unwrap();
            fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
            Scratch(dir)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    const OLD: Duration = Duration::from_secs(2 * 60 * 60);

    fn remove_file(path: &Path) -> bool {
        remove_file_if_over(path, euid(), SystemTime::now(), SETTLED)
    }

    fn remove_dir(path: &Path) -> bool {
        remove_if_over(path, Tag::Export, euid(), SystemTime::now(), SETTLED)
    }

    /// A dead run's temporary file, quiet for long enough to be removed.
    fn file_at(scratch: &Scratch) -> PathBuf {
        let path = scratch.0.join("compara-41-0badc0de-0-stdin-locked");
        let file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&path)
            .unwrap();
        file.set_modified(SystemTime::now() - OLD).unwrap();
        path
    }

    /// A dead run's export directory, the same.
    fn dir_at(scratch: &Scratch) -> PathBuf {
        let path = scratch.0.join("compara-41-0badc0de-0-export-locked");
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        let added = path.join("added.csv");
        fs::write(&added, "id\n").unwrap();
        for at in [&added, &path] {
            File::open(at)
                .unwrap()
                .set_modified(SystemTime::now() - OLD)
                .unwrap();
        }
        path
    }

    #[test]
    fn a_name_swapped_after_its_check_is_not_waited_on_or_followed() {
        // Left alone, each goes: what a swap changes is the name alone.
        let scratch = Scratch::new("alone");
        assert!(remove_file(&file_at(&scratch)));
        assert!(remove_dir(&dir_at(&scratch)));

        let mut got = Vec::new();
        // A pipe: opening it to read would wait for a writer for good.
        let scratch = Scratch::new("file-pipe");
        got.push((
            "file to pipe",
            raced(&file_at(&scratch), to_pipe, remove_file),
        ));
        let scratch = Scratch::new("dir-pipe");
        got.push(("dir to pipe", raced(&dir_at(&scratch), to_pipe, remove_dir)));
        // A link to the file looked at: followed, it is the same file, every
        // later check passes, and the link is what is removed.
        let scratch = Scratch::new("file-link");
        let path = file_at(&scratch);
        got.push(("file to link", raced(&path, to_link, remove_file)));
        got.push(("link left", Some(fs::symlink_metadata(&path).is_ok())));
        // A link to the directory looked at: followed, what is in it is
        // removed through the link.
        let scratch = Scratch::new("dir-link");
        got.push(("dir to link", raced(&dir_at(&scratch), to_link, remove_dir)));
        let kept = scratch.0.join("moved").join("added.csv").exists();
        got.push(("its file left", Some(kept)));
        drop(scratch);

        assert_eq!(
            got,
            [
                ("file to pipe", Some(false)),
                ("dir to pipe", Some(false)),
                ("file to link", Some(false)),
                ("link left", Some(true)),
                ("dir to link", Some(false)),
                ("its file left", Some(true)),
            ]
        );
    }

    fn remove_sortmerge(path: &Path) -> bool {
        remove_if_over(path, Tag::Sortmerge, euid(), SystemTime::now(), SETTLED)
    }

    /// Sets when what is at each of `paths` last changed to long ago, in
    /// order: a directory's after what is made in it.
    fn age(paths: &[PathBuf]) {
        for at in paths {
            File::open(at)
                .unwrap()
                .set_modified(SystemTime::now() - OLD)
                .unwrap();
        }
    }

    /// A dead run's sortmerge directory, both sides holding a run, quiet for
    /// long enough to be removed.
    fn sortmerge_at(scratch: &Scratch) -> PathBuf {
        let path = scratch.0.join("compara-41-0badc0de-0-sortmerge-locked");
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o700)).unwrap();
        let mut made = Vec::new();
        for (side, run) in [("a", "run-1.bin"), ("b", "merge-0-1.bin")] {
            fs::create_dir(path.join(side)).unwrap();
            fs::write(path.join(side).join(run), "rows").unwrap();
            made.push(path.join(side).join(run));
            made.push(path.join(side));
        }
        made.push(path.clone());
        age(&made);
        path
    }

    /// Puts at `path`, once its directory is locked, a link to `elsewhere`
    /// beside it, and moves the directory aside.
    fn to_elsewhere(path: &Path) {
        aside(path);
        symlink("elsewhere", path).unwrap();
    }

    /// A directory of someone else's beside the run's, holding what the
    /// run's would hold, as old: by the name, it would be swept instead.
    fn elsewhere(scratch: &Scratch, files: &[&str]) -> PathBuf {
        let dir = scratch.0.join("elsewhere");
        fs::create_dir(&dir).unwrap();
        let mut made = Vec::new();
        for file in files {
            let at = dir.join(file);
            if let Some(sub) = at.parent().filter(|p| *p != dir) {
                fs::create_dir_all(sub).unwrap();
            }
            fs::write(&at, "theirs").unwrap();
            made.push(at);
        }
        for file in files {
            if let Some(sub) = dir.join(file).parent().filter(|p| *p != dir) {
                made.push(sub.to_path_buf());
            }
        }
        made.push(dir.clone());
        age(&made);
        dir
    }

    #[test]
    fn what_a_locked_directory_holds_is_removed_through_it_not_by_its_name() {
        // Left alone, each goes, sortmerge's sides and all.
        let scratch = Scratch::new("held-alone");
        assert!(remove_dir(&dir_at(&scratch)));
        let path = sortmerge_at(&scratch);
        assert!(remove_sortmerge(&path));
        assert!(!path.exists());

        let mut got = Vec::new();
        for (what, remove, make, files) in [
            (
                "export",
                remove_dir as fn(&Path) -> bool,
                dir_at as fn(&Scratch) -> PathBuf,
                &["added.csv"][..],
            ),
            (
                "sortmerge",
                remove_sortmerge,
                sortmerge_at,
                &["a/run-1.bin", "b/merge-0-1.bin"][..],
            ),
        ] {
            let scratch = Scratch::new(&format!("held-{what}"));
            let path = make(&scratch);
            let theirs = elsewhere(&scratch, files);
            SWAP.set(Some((Point::Locked, to_elsewhere)));
            let removed = remove(&path);
            SWAP.set(None);
            let left = |root: &Path| files.iter().filter(|f| root.join(f).exists()).count();
            // The name is a link now and is kept; what it points at is kept
            // whole; the run's own directory, moved aside, is emptied.
            got.push((what, "removed", removed));
            got.push((what, "link kept", fs::symlink_metadata(&path).is_ok()));
            got.push((what, "theirs kept", left(&theirs) == files.len()));
            got.push((what, "its own emptied", left(&scratch.0.join("moved")) == 0));
        }
        assert_eq!(
            got,
            [
                ("export", "removed", false),
                ("export", "link kept", true),
                ("export", "theirs kept", true),
                ("export", "its own emptied", true),
                ("sortmerge", "removed", false),
                ("sortmerge", "link kept", true),
                ("sortmerge", "theirs kept", true),
                ("sortmerge", "its own emptied", true),
            ]
        );
    }

    /// Where the swaps below put a pipe or a link, for the test to open the
    /// pipe's other end and to clear up.
    static MADE_AT: Mutex<Option<PathBuf>> = Mutex::new(None);

    /// Moves a new directory aside, beside it under a name the sweep does
    /// not take; that name.
    fn new_aside(path: &Path) -> PathBuf {
        let mut moved = path.as_os_str().to_owned();
        moved.push(".moved");
        fs::rename(path, &moved).unwrap();
        *MADE_AT.lock().unwrap() = Some(path.to_path_buf());
        PathBuf::from(moved)
    }

    fn new_to_pipe(path: &Path) {
        new_aside(path);
        let made = Command::new("mkfifo")
            .args(["-m", "600"])
            .arg(path)
            .status()
            .unwrap();
        assert!(made.success());
    }

    fn new_to_link(path: &Path) {
        let moved = new_aside(path);
        symlink(moved.file_name().unwrap(), path).unwrap();
    }

    #[test]
    fn a_new_temporary_directory_swapped_before_it_is_locked_is_not_waited_on_or_followed() {
        // The process's one sweep of the real temporary directory, done
        // before any clock starts (it can take seconds on a busy machine, or
        // with a killed run's spills to remove), and before the swap is set:
        // the sweep opens names too.
        super::super::sweep_once();
        let mut got = Vec::new();
        for (what, swap) in [
            ("pipe", new_to_pipe as fn(&Path)),
            ("link", new_to_link as fn(&Path)),
        ] {
            let (tx, rx) = mpsc::channel();
            let run = std::thread::spawn(move || {
                SWAP.set(Some((Point::Open, swap)));
                // Whether what it hands back is a directory, not what was
                // swapped in.
                let made = crate::source::TempDir::new("probe")
                    .ok()
                    .map(|d| fs::symlink_metadata(d.path()).is_ok_and(|m| m.file_type().is_dir()));
                SWAP.set(None);
                let _ = tx.send(made);
            });
            let made = rx.recv_timeout(Duration::from_secs(10)).ok();
            let at = MADE_AT.lock().unwrap().take();
            if made.is_none()
                && let Some(at) = &at
            {
                let _ = fs::OpenOptions::new()
                    .write(true)
                    .custom_flags(libc::O_NONBLOCK)
                    .open(at);
            }
            run.join().unwrap();
            if let Some(at) = at {
                let _ = fs::remove_file(&at);
                let mut moved = at.into_os_string();
                moved.push(".moved");
                let _ = fs::remove_dir(moved);
            }
            got.push((what, made));
        }
        assert_eq!(
            got,
            [("pipe", Some(Some(true))), ("link", Some(Some(true)))]
        );
    }

    /// Where a swap below acts: the new directory's name, where its own
    /// went, and the directory of someone else's it is swapped for.
    static SWAPPED: Mutex<Option<(PathBuf, PathBuf, PathBuf)>> = Mutex::new(None);

    /// Moves the new directory at `path` aside; where it went.
    fn swap_aside(path: &Path) -> PathBuf {
        let mut moved = path.as_os_str().to_owned();
        moved.push(".moved");
        fs::rename(path, &moved).unwrap();
        let mut at = SWAPPED.lock().unwrap();
        let theirs = at.take().unwrap().2;
        *at = Some((path.to_path_buf(), PathBuf::from(&moved), theirs));
        PathBuf::from(moved)
    }

    fn theirs() -> PathBuf {
        SWAPPED.lock().unwrap().as_ref().unwrap().2.clone()
    }

    /// The name, once made, a link to their directory.
    fn made_to_link(path: &Path) {
        swap_aside(path);
        symlink(theirs(), path).unwrap();
    }

    /// Their directory itself moved onto the name, as another user can
    /// where the temporary directory is open to all without the sticky bit.
    fn made_to_theirs(path: &Path) {
        swap_aside(path);
        fs::rename(theirs(), path).unwrap();
    }

    fn sorted_names(dir: &Path) -> Vec<String> {
        let mut v: Vec<String> = fs::read_dir(dir)
            .map(|d| {
                d.map(|e| e.unwrap().file_name().into_string().unwrap())
                    .collect()
            })
            .unwrap_or_default();
        v.sort();
        v
    }

    /// Two files whose external sort spills runs and merges them back, and
    /// their counts: changed, added, removed.
    fn spilling_pair(scratch: &Scratch) -> (PathBuf, PathBuf, (i64, i64, i64)) {
        use std::fmt::Write as _;
        let pad = "x".repeat(40);
        let (mut a, mut b) = (String::from("id,v,pad\n"), String::from("id,v,pad\n"));
        let mut want = (0, 100, 0);
        for id in 0..50_000 {
            writeln!(a, "{id},{id},{pad}").unwrap();
            if id % 11 == 0 {
                want.2 += 1;
                continue;
            }
            let v = if id % 7 == 0 { id + 1 } else { id };
            want.0 += i64::from(id % 7 == 0);
            writeln!(b, "{id},{v},{pad}").unwrap();
        }
        for id in 50_000..50_100 {
            writeln!(b, "{id},{id},{pad}").unwrap();
        }
        let (pa, pb) = (scratch.0.join("a.csv"), scratch.0.join("b.csv"));
        fs::write(&pa, a).unwrap();
        fs::write(&pb, b).unwrap();
        (pa, pb, want)
    }

    #[test]
    fn what_a_run_writes_in_its_temporary_directory_goes_in_it_whatever_its_name_becomes() {
        use crate::engine::sortmerge;
        use crate::options::Options;
        use std::io::{Read as _, Write as _};

        super::super::sweep_once();
        let scratch = Scratch::new("written");
        let (a, b, want) = spilling_pair(&scratch);
        let small = Scratch::new("written-small");
        fs::write(small.0.join("a.csv"), "id,v\n1,a\n2,b\n").unwrap();
        fs::write(small.0.join("b.csv"), "id,v\n2,c\n3,d\n").unwrap();
        let (sa, sb) = (small.0.join("a.csv"), small.0.join("b.csv"));
        // A budget whose batches are a megabyte: each file spills runs.
        let mut spill = Options::with_key(["id"]);
        spill.memory = Some(16 << 20);

        let mut got = Vec::new();
        for (writer, point, swap, how) in [
            ("sortmerge", Point::Made, made_to_link as fn(&Path), "link"),
            ("sortmerge", Point::Made, made_to_theirs, "theirs"),
            ("export", Point::Made, made_to_link, "link"),
            ("export", Point::Made, made_to_theirs, "theirs"),
            ("out", Point::Made, made_to_link, "link"),
            ("out", Point::Made, made_to_theirs, "theirs"),
            ("out", Point::Open, made_to_theirs, "theirs before the lock"),
        ] {
            let theirs = scratch.0.join("theirs");
            fs::create_dir(&theirs).unwrap();
            fs::write(theirs.join("theirs.txt"), "theirs").unwrap();
            fs::set_permissions(&theirs, fs::Permissions::from_mode(0o777)).unwrap();
            *SWAPPED.lock().unwrap() = Some((PathBuf::new(), PathBuf::new(), theirs.clone()));
            SWAP.set(Some((point, swap)));
            // Whether the run did what it was asked, in a directory of its
            // own.
            let done = match writer {
                "sortmerge" => sortmerge::compare(&a, &b, &spill)
                    .is_ok_and(|r| (r.counts.changed, r.counts.added, r.counts.removed) == want),
                "export" => {
                    let dir = crate::source::TempDir::new("export").unwrap();
                    let mut opt = Options::with_key(["id"]);
                    opt.export_dir = Some(dir.path().to_string_lossy().into_owned());
                    opt.export_staged = Some(dir.dir().clone());
                    let ran = sortmerge::compare(&sa, &sb, &opt).is_ok();
                    let mut names = dir.dir().names().unwrap();
                    names.sort();
                    ran && names == ["added.csv", "changed.csv", "removed.csv"]
                }
                _ => {
                    let dir = crate::source::TempDir::new("out").unwrap();
                    let mut file = dir.dir().create("output").unwrap();
                    file.write_all(b"report").unwrap();
                    drop(file);
                    let mut back = String::new();
                    dir.dir()
                        .open("output")
                        .unwrap()
                        .read_to_string(&mut back)
                        .unwrap();
                    back == "report"
                }
            };
            SWAP.set(None);
            let (at, moved, _) = SWAPPED.lock().unwrap().take().unwrap();
            // Their directory, at the name or where it was, as it was.
            let kept = if fs::symlink_metadata(&theirs).is_ok() {
                sorted_names(&theirs)
            } else {
                sorted_names(&at)
            } == ["theirs.txt"];
            got.push((writer, how, done, kept));
            let _ = fs::remove_file(&at);
            let _ = fs::remove_dir_all(&at);
            let _ = fs::remove_dir_all(&moved);
            let _ = fs::remove_dir_all(&theirs);
        }
        let want: Vec<_> = got.iter().map(|&(w, h, _, _)| (w, h, true, true)).collect();
        assert_eq!(got, want);
    }

    #[test]
    fn a_listing_through_the_descriptor_is_the_listing_by_name() {
        let scratch = Scratch::new("names");
        let mut want: Vec<Vec<u8>> = (0..300)
            .map(|n| format!("file-{n:03}-{}", "x".repeat(n % 40)).into_bytes())
            .collect();
        for name in &want {
            fs::write(scratch.0.join(std::ffi::OsStr::from_bytes(name)), "").unwrap();
        }
        fs::create_dir(scratch.0.join("sub")).unwrap();
        symlink("nowhere", scratch.0.join("link")).unwrap();
        want.push(b"sub".to_vec());
        want.push(b"link".to_vec());
        want.sort();
        let by_name = || -> Vec<Vec<u8>> {
            let mut v: Vec<_> = fs::read_dir(&scratch.0)
                .unwrap()
                .map(|e| e.unwrap().file_name().into_vec())
                .collect();
            v.sort();
            v
        };
        let dir = Dir(reopen(&scratch.0, true).unwrap());
        // Twice: a listing does not move the descriptor's place.
        for _ in 0..2 {
            let mut got: Vec<Vec<u8>> = dir
                .names()
                .unwrap()
                .into_iter()
                .map(CString::into_bytes)
                .collect();
            got.sort();
            assert_eq!(got, by_name());
            assert_eq!(got, want);
        }
        let st = dir.stat(c"sub").unwrap();
        assert_eq!(st.st_mode & libc::S_IFMT, libc::S_IFDIR);
        let st = dir.stat(c"link").unwrap();
        assert_eq!(st.st_mode & libc::S_IFMT, libc::S_IFLNK);
        let st = dir.stat(c"file-007-xxxxxxx").unwrap();
        let by_path = fs::symlink_metadata(scratch.0.join("file-007-xxxxxxx")).unwrap();
        assert_eq!(modified(&st), by_path.modified().ok());
        assert!(dir.open_dir(c"link").is_none() && dir.open_dir(c"sub").is_some());
        assert!(dir.remove(c"link", false) && dir.remove(c"sub", true));
        assert!(!dir.remove(c"sub", true));
        assert!(dir.stat(c"sub").is_none());
    }

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
        for (tag, want) in [
            ("stdin", Tag::Stdin),
            ("plain", Tag::Plain),
            ("download", Tag::Download),
            ("part", Tag::Part),
            ("combined", Tag::Combined),
        ] {
            assert_eq!(
                parse(&format!("compara-41-0badc0de-3-{tag}-locked")),
                Some((41, want, true))
            );
            assert!(want.is_file());
        }
        assert!(!Tag::Sortmerge.is_file() && !Tag::Export.is_file() && !Tag::Out.is_file());
        for name in [
            "compara-0-0badc0de-3-sortmerge",
            "compara--1-0badc0de-3-sortmerge",
            "compara-41-0badc0d-3-sortmerge",
            "compara-41-0badc0de-3-probe",
            "compara-41-0badc0de-3-stdin-x",
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
