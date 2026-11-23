//! A run killed while it spills (SIGKILL, the OOM killer, an abort) leaves
//! its temporary directory behind: nothing of it runs to remove it. The next
//! run that makes one removes what such runs left, and nothing else.
#![cfg(unix)]

use std::fs;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

const BIN: &str = env!("CARGO_BIN_EXE_compara");

/// A temporary directory of the test's own, handed to the run as `TMPDIR`:
/// the sweep is never pointed at a directory other runs use.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Scratch {
        let dir =
            std::env::temp_dir().join(format!("compara-sweep-test-{}-{name}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("tmp")).unwrap();
        Scratch(dir)
    }

    fn tmp(&self) -> PathBuf {
        self.0.join("tmp")
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The pid of a process that has ended and been waited for.
fn dead_pid() -> u32 {
    let mut child = Command::new("true").spawn().unwrap();
    let pid = child.id();
    child.wait().unwrap();
    pid
}

/// A spill directory as a run of `pid` leaves it when killed mid-sort, with
/// nothing in it changed for `age`. A tag ending `-locked` is one a run that
/// locks its directory makes; without, one of an earlier version.
fn left_by(tmp: &Path, pid: u32, n: u32, tag: &str, age: Duration) -> PathBuf {
    let dir = tmp.join(format!("compara-{pid}-0badc0de-{n}-{tag}"));
    fs::create_dir(&dir).unwrap();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
    if tag.starts_with("sortmerge") {
        for side in ["a", "b"] {
            fs::create_dir(dir.join(side)).unwrap();
            fs::write(dir.join(side).join("run-0.bin"), [7u8; 4096]).unwrap();
            fs::write(dir.join(side).join("run-1.bin"), [7u8; 4096]).unwrap();
            fs::write(dir.join(side).join("merge-0-0.bin"), [7u8; 4096]).unwrap();
        }
    }
    backdate(&dir, age);
    dir
}

/// Sets the time `path` and everything under it last changed to `age` ago;
/// links are left as they are.
fn backdate(path: &Path, age: Duration) {
    let kind = fs::symlink_metadata(path).unwrap().file_type();
    if kind.is_symlink() {
        return;
    }
    if kind.is_dir() {
        for entry in fs::read_dir(path).unwrap() {
            backdate(&entry.unwrap().path(), age);
        }
    }
    let then = SystemTime::now() - age;
    fs::File::open(path).unwrap().set_modified(then).unwrap();
}

/// A comparison that spills (the batch budget is shrunk), with `tmp` as its
/// temporary directory.
fn spill(scratch: &Scratch) {
    let a = scratch.0.join("a.csv");
    let b = scratch.0.join("b.csv");
    let body: String = (0..2000).map(|i| format!("{i},{}\n", i * 3)).collect();
    fs::write(&a, format!("id,v\n{body}")).unwrap();
    fs::write(&b, format!("id,v\n{body}1,x\n")).unwrap();
    let child = Command::new(BIN)
        .args(["compare", "--engine", "sortmerge", "--key", "id"])
        .arg(&a)
        .arg(&b)
        .env("TMPDIR", scratch.tmp())
        .env("COMPARA_SORTMERGE_BATCH_BYTES", "2048")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id();
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.code().is_some_and(|c| c <= 1),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // The run's own directory went with it.
    let left: Vec<_> = fs::read_dir(scratch.tmp())
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&format!("compara-{pid}-")))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

/// Past the minute a locked run's directory waits, short of the day an
/// earlier version's waits.
const OLD: Duration = Duration::from_secs(3600);
const DAY_OLD: Duration = Duration::from_secs(25 * 3600);

#[test]
fn a_dead_run_s_spill_directory_is_removed_by_the_next_run() {
    let scratch = Scratch::new("dead");
    let dead = dead_pid();
    let sortmerge = left_by(&scratch.tmp(), dead, 0, "sortmerge-locked", OLD);
    let export = left_by(&scratch.tmp(), dead, 1, "export-locked", OLD);
    fs::write(export.join("added.csv"), "id\n1\n").unwrap();
    fs::write(export.join(".compara-12-0.part"), "id\n").unwrap();
    backdate(&export, OLD);
    let out = left_by(&scratch.tmp(), dead, 2, "out-locked", OLD);
    fs::write(out.join("output"), "{}").unwrap();
    backdate(&out, OLD);
    let runs = left_by(&scratch.tmp(), dead, 3, "sortmerge-locked", OLD);
    // Killed before it wrote anything.
    let empty = left_by(&scratch.tmp(), dead, 5, "out-locked", OLD);
    // An earlier version's, which took no lock, once nothing in it has
    // changed for a day.
    let unlocked = left_by(&scratch.tmp(), dead, 4, "sortmerge", DAY_OLD);
    spill(&scratch);
    for dir in [&sortmerge, &export, &out, &runs, &empty, &unlocked] {
        assert!(!dir.exists(), "{} is left", dir.display());
    }
}

#[test]
fn what_is_not_provably_a_dead_run_s_is_left() {
    let scratch = Scratch::new("kept");
    let tmp = scratch.tmp();
    let dead = dead_pid();
    let mut kept = Vec::new();

    // A run that is still going: this test's own process.
    kept.push(left_by(
        &tmp,
        std::process::id(),
        0,
        "sortmerge-locked",
        OLD,
    ));

    // A run still going in another pid namespace, where its pid reads as
    // gone: the lock it holds on its directory says otherwise.
    let locked = left_by(&tmp, dead, 1, "sortmerge-locked", OLD);
    let lock = fs::File::open(&locked).unwrap();
    lock.lock().unwrap();
    kept.push(locked);

    // Made a moment ago: between making its directory and locking it.
    kept.push(left_by(&tmp, dead, 2, "sortmerge-locked", Duration::ZERO));

    // Something compara does not write there.
    let foreign = left_by(&tmp, dead, 3, "sortmerge-locked", OLD);
    fs::write(foreign.join("a").join("notes.txt"), "mine").unwrap();
    backdate(&foreign, OLD);
    kept.push(foreign);
    let stray = left_by(&tmp, dead, 9, "sortmerge-locked", OLD);
    fs::write(stray.join("notes.txt"), "mine").unwrap();
    backdate(&stray, OLD);
    kept.push(stray);

    // A link inside, even to a file of the right name elsewhere.
    let linked = left_by(&tmp, dead, 4, "sortmerge-locked", OLD);
    let target = scratch.0.join("target.bin");
    fs::write(&target, "keep").unwrap();
    symlink(&target, linked.join("a").join("run-9.bin")).unwrap();
    backdate(&linked, OLD);
    kept.push(linked);

    // A link at the name, to a directory that looks like a dead run's.
    let elsewhere = left_by(&scratch.0, dead, 5, "sortmerge-locked", OLD);
    let link = tmp.join(format!("compara-{dead}-0badc0de-5-sortmerge-locked"));
    symlink(&elsewhere, &link).unwrap();
    kept.push(link);

    // Open to other users: not a directory compara made.
    let open = left_by(&tmp, dead, 6, "sortmerge-locked", OLD);
    fs::set_permissions(&open, fs::Permissions::from_mode(0o755)).unwrap();
    backdate(&open, OLD);
    kept.push(open);

    // A tag compara does not use, and a name that is only similar.
    kept.push(left_by(&tmp, dead, 7, "mine-locked", OLD));
    let similar = tmp.join(format!("compara-{dead}-backup"));
    fs::create_dir(&similar).unwrap();
    kept.push(similar);

    // A run file written a moment ago inside `a/`, where the directory
    // itself has not changed since the run made `a/` and `b/`.
    let writing = left_by(&tmp, dead, 10, "sortmerge-locked", OLD);
    fs::write(writing.join("a").join("run-2.bin"), [7u8; 64]).unwrap();
    // Only the file says so: `a/` is set back too.
    fs::File::open(writing.join("a"))
        .unwrap()
        .set_modified(SystemTime::now() - OLD)
        .unwrap();
    kept.push(writing);

    // A run file removed a moment ago (a merge pass): only `a/` says so.
    let merged = left_by(&tmp, dead, 13, "sortmerge-locked", OLD);
    fs::remove_file(merged.join("a").join("run-1.bin")).unwrap();
    kept.push(merged);

    // An earlier version's, which took no lock: its run may be going on in
    // another pid namespace, where its pid reads as gone. Kept until nothing
    // in it has changed for a day.
    kept.push(left_by(&tmp, dead, 11, "sortmerge", OLD));
    let unlocked_writing = left_by(&tmp, dead, 12, "sortmerge", DAY_OLD);
    fs::write(unlocked_writing.join("a").join("run-2.bin"), [7u8; 64]).unwrap();
    kept.push(unlocked_writing);

    // Another user's, where this test can make one.
    let theirs = left_by(&tmp, dead, 8, "sortmerge-locked", OLD);
    let chowned = Command::new("chown")
        .args(["-R", "65534:65534"])
        .arg(&theirs)
        .status()
        .is_ok_and(|s| s.success());
    if chowned {
        kept.push(theirs);
    } else {
        fs::remove_dir_all(&theirs).unwrap();
    }

    spill(&scratch);
    for path in &kept {
        assert!(
            fs::symlink_metadata(path).is_ok(),
            "{} was removed",
            path.display()
        );
    }
    assert!(target.exists() && elsewhere.join("a").join("run-0.bin").exists());
    assert!(kept[0].join("a").join("run-0.bin").exists());
    drop(lock);
}

/// The tags of the temporary files a run makes: standard input, a
/// decompressed input, a download, one object of a prefix, a table's parts
/// as one.
const FILE_TAGS: [&str; 5] = ["stdin", "plain", "download", "part", "combined"];

/// A temporary file as a run of `pid` leaves it when killed, unchanged for
/// `age`.
fn file_left_by(tmp: &Path, pid: u32, n: u32, tag: &str, age: Duration) -> PathBuf {
    let path = tmp.join(format!("compara-{pid}-0badc0de-{n}-{tag}"));
    fs::write(&path, "id,v\n1,2\n").unwrap();
    fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
    backdate(&path, age);
    path
}

/// The names in `tmp` a run of `pid` made.
fn left_of(tmp: &Path, pid: u32) -> Vec<String> {
    fs::read_dir(tmp)
        .unwrap()
        .flatten()
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with(&format!("compara-{pid}-")))
        .collect()
}

/// A comparison of standard input, which it spools to a temporary file, with
/// `tmp` as its temporary directory.
fn from_stdin(scratch: &Scratch) {
    use std::io::Write;
    let b = scratch.0.join("b.csv");
    fs::write(&b, "id,v\n1,2\n2,3\n").unwrap();
    let mut child = Command::new(BIN)
        .args(["compare", "--key", "id", "-"])
        .arg(&b)
        .env("TMPDIR", scratch.tmp())
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();
    let pid = child.id();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"id,v\n1,2\n")
        .unwrap();
    // A sweep that opened a pipe would wait on it for ever.
    let started = std::time::Instant::now();
    while child.try_wait().unwrap().is_none() {
        if started.elapsed() > Duration::from_secs(60) {
            let _ = child.kill();
            panic!("the run did not end in 60 s");
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let out = child.wait_with_output().unwrap();
    assert!(
        out.status.code().is_some_and(|c| c <= 1),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // The run's own file went with it.
    let left = left_of(&scratch.tmp(), pid);
    assert!(left.is_empty(), "{left:?}");
}

#[test]
fn a_dead_run_s_temporary_files_are_removed_by_the_next_run() {
    let scratch = Scratch::new("dead-files");
    let tmp = scratch.tmp();
    let dead = dead_pid();
    let mut gone = Vec::new();
    for (i, tag) in (0..).zip(FILE_TAGS) {
        let marked = format!("{tag}-locked");
        gone.push(file_left_by(&tmp, dead, 2 * i, &marked, OLD));
        // An earlier version's, which took no lock, once unchanged for a
        // day.
        gone.push(file_left_by(&tmp, dead, 2 * i + 1, tag, DAY_OLD));
    }
    from_stdin(&scratch);
    for path in &gone {
        assert!(!path.exists(), "{} is left", path.display());
    }
}

#[test]
fn a_temporary_file_not_provably_a_dead_run_s_is_left() {
    let scratch = Scratch::new("kept-files");
    let tmp = scratch.tmp();
    let dead = dead_pid();
    let me = std::process::id();
    let mut kept = Vec::new();
    let mut locks = Vec::new();
    for (i, tag) in (0..).zip(FILE_TAGS) {
        let marked = format!("{tag}-locked");
        // A run still going: this test's own process.
        kept.push(file_left_by(&tmp, me, 10 * i, &marked, OLD));
        // A run still going in another pid namespace, where its pid reads
        // as gone: the lock it holds on the file says otherwise.
        let locked = file_left_by(&tmp, dead, 10 * i + 1, &marked, OLD);
        let lock = fs::File::open(&locked).unwrap();
        lock.lock().unwrap();
        locks.push(lock);
        kept.push(locked);
        // Made a moment ago: between making the file and locking it.
        let fresh = Duration::ZERO;
        kept.push(file_left_by(&tmp, dead, 10 * i + 2, &marked, fresh));
        // An earlier version's, which took no lock, unchanged for less
        // than a day.
        kept.push(file_left_by(&tmp, dead, 10 * i + 3, tag, OLD));
    }

    // A link at the name, to a file that looks like a dead run's.
    let elsewhere = file_left_by(&scratch.0, dead, 90, "stdin-locked", OLD);
    let link = tmp.join(format!("compara-{dead}-0badc0de-90-stdin-locked"));
    symlink(&elsewhere, &link).unwrap();
    kept.push(link);

    // A file with another name as well: somebody's file too.
    let linked = file_left_by(&tmp, dead, 91, "download-locked", OLD);
    let other_name = scratch.0.join("mine.csv");
    fs::hard_link(&linked, &other_name).unwrap();
    backdate(&linked, OLD);
    kept.push(linked);

    // Open to other users: not a file compara made.
    let open = file_left_by(&tmp, dead, 92, "plain-locked", OLD);
    fs::set_permissions(&open, fs::Permissions::from_mode(0o644)).unwrap();
    backdate(&open, OLD);
    kept.push(open);

    // A directory where the tag says a file, and a file where it says a
    // directory.
    let dir = tmp.join(format!("compara-{dead}-0badc0de-93-stdin-locked"));
    fs::create_dir(&dir).unwrap();
    fs::set_permissions(&dir, fs::Permissions::from_mode(0o700)).unwrap();
    backdate(&dir, OLD);
    kept.push(dir);
    kept.push(file_left_by(&tmp, dead, 94, "sortmerge-locked", OLD));

    // A pipe at a file's name, which opening would wait on (before it is
    // ever asked how long it has been quiet).
    let pipe = tmp.join(format!("compara-{dead}-0badc0de-97-stdin-locked"));
    let made = Command::new("mkfifo")
        .args(["-m", "600"])
        .arg(&pipe)
        .status()
        .is_ok_and(|s| s.success());
    if made {
        kept.push(pipe);
    }

    // A tag compara does not use for a file, and an output's partial file,
    // which belongs beside the output and is never swept.
    kept.push(file_left_by(&tmp, dead, 95, "probe-locked", OLD));
    let partial = tmp.join(format!(".compara-{dead}-0.part"));
    fs::write(&partial, "id\n").unwrap();
    fs::set_permissions(&partial, fs::Permissions::from_mode(0o600)).unwrap();
    backdate(&partial, OLD);
    kept.push(partial);

    // Another user's, where this test can make one.
    let theirs = file_left_by(&tmp, dead, 96, "combined-locked", OLD);
    let chowned = Command::new("chown")
        .arg("65534:65534")
        .arg(&theirs)
        .status()
        .is_ok_and(|s| s.success());
    if chowned {
        kept.push(theirs);
    } else {
        fs::remove_file(&theirs).unwrap();
    }

    from_stdin(&scratch);
    for path in &kept {
        assert!(
            fs::symlink_metadata(path).is_ok(),
            "{} was removed",
            path.display()
        );
    }
    assert!(elsewhere.exists() && other_name.exists());
    drop(locks);
}

/// A run killed while it reads standard input (SIGKILL: nothing of it runs
/// to clean up) leaves its spooled input behind. While the run lives the
/// file is locked; once it is killed the lock is gone with it, and the next
/// run removes the file.
#[test]
fn a_killed_run_s_spooled_input_is_locked_while_it_lives_and_removed_after() {
    use std::io::Write;
    let scratch = Scratch::new("killed");
    let tmp = scratch.tmp();
    let b = scratch.0.join("b.csv");
    fs::write(&b, "id,v\n1,2\n").unwrap();
    let mut child = Command::new(BIN)
        .args(["compare", "--key", "id", "-"])
        .arg(&b)
        .env("TMPDIR", &tmp)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let pid = child.id();
    // Standard input stays open: the run waits for the rest of it.
    let mut stdin = child.stdin.take().unwrap();
    stdin.write_all(b"id,v\n1,2\n").unwrap();
    let started = std::time::Instant::now();
    let file = loop {
        if let Some(name) = left_of(&tmp, pid).pop() {
            break tmp.join(name);
        }
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "no temporary file"
        );
        std::thread::sleep(Duration::from_millis(20));
    };
    let name = file.file_name().unwrap().to_string_lossy().into_owned();
    assert!(name.ends_with("-stdin-locked"), "{name}");
    assert!(
        fs::File::open(&file).unwrap().try_lock().is_err(),
        "a live run's file is not locked"
    );
    child.kill().unwrap();
    child.wait().unwrap();
    drop(stdin);
    assert!(file.exists(), "nothing of a killed run removes its file");
    let lock = fs::File::open(&file).unwrap();
    assert!(lock.try_lock().is_ok(), "the lock outlived its run");
    drop(lock);
    // The minute the sweep waits, already past.
    backdate(&file, OLD);
    from_stdin(&scratch);
    assert!(!file.exists(), "{} is left", file.display());
}
