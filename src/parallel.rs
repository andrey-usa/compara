//! Scoped threads that degrade to doing the work here.
//!
//! [`std::thread::Scope::spawn`] panics when the OS will not give it a thread,
//! and under a memory budget that is exactly when it happens: a thread wants a
//! stack, a stack is a private anonymous mapping, and since Linux 4.7 that is
//! what `RLIMIT_DATA` bounds. So the run that has just discovered it is short of
//! memory answers with
//!
//! ```text
//! thread '<unnamed>' panicked at library/std/src/thread/scoped.rs:206:46:
//! failed to spawn thread: Os { code: 11, kind: WouldBlock }
//! ```
//!
//! and exit 101, where every other port in this repository says `error: out of
//! memory` and exits 2. With `RUST_BACKTRACE=1` set it does not even manage
//! that: printing the panic takes the backtrace lock, symbolising allocates,
//! that allocation fails too, and the allocation error hook reaches for the
//! lock its own thread is already holding. The process hangs, which on a
//! benchmark runner is worse than either — an abort at least lets the harness
//! move on to the next port.
//!
//! None of that is a failure worth reporting. A thread the OS refuses means one
//! fewer core, not a wrong answer, and this port already says so in
//! [`crate::engine`]'s sweep: *a thread that cannot be spawned is not a failure,
//! it is the same work, done here*. That was a comment describing something the
//! code did not do. This module is the part that does it.
//!
//! A spawn the OS grants can still take the process down. Before a new thread
//! runs anything, the runtime maps it an alternate signal stack (for the
//! message a stack overflow prints), and when that mapping is refused it
//! aborts: exit 134, "failed to allocate an alternative stack", with no error
//! the caller could see. The stack itself needs no new memory when the C
//! library hands back a cached one from a finished thread, so the spawn
//! succeeds after a comparison has taken the last of its heap -- measured,
//! a 2M-row JSON pair at a `ulimit -d` anywhere in a 44 KB band aborted on
//! every run. So [`spawn_scoped`] looks first: under a heap limit, a thread is
//! started only when a stack and its setting-up still fit under it, and
//! otherwise it is refused the way the OS refuses one. It then waits for the
//! thread to be set up before going on: looking alone left a burst of lanes,
//! each counted against room the one before had not taken yet, and the fused
//! join on a 200,000-row pair still aborted at 17 of 752 limits scanned.

use std::thread::{Builder, Scope, ScopedJoinHandle};

/// What a new thread maps for itself besides its stack before its work
/// starts: the alternate signal stack (16 KB here, more where the CPU's
/// signal frame is larger), the C library's first heap for the thread
/// (132 KB), and the page or two of slack the stack mapping carries.
const SET_UP: u64 = 256 << 10;

/// The stack a thread gets from [`Builder::new`]: `RUST_MIN_STACK`, read as
/// the runtime reads it, or the runtime's 2 MB.
fn stack_size() -> u64 {
    static SIZE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    *SIZE.get_or_init(|| {
        std::env::var_os("RUST_MIN_STACK")
            .and_then(|s| s.to_str()?.parse().ok())
            .unwrap_or(2 << 20)
    })
}

/// Whether a new thread fits under the heap limit: room for a stack and its
/// [`SET_UP`]; `None` without a limit (or where what is in use cannot be
/// read). Whether the C library has a stack cached is not knowable from here,
/// so one is always counted; near the limit that refuses a thread a little
/// early, which costs a core and not an answer.
fn fits() -> Option<bool> {
    let limit = crate::memory::heap_limit()?;
    let used = crate::memory::data_in_use()?;
    Some(used.saturating_add(stack_size()).saturating_add(SET_UP) <= limit)
}

/// What a refused thread is: a bare kind, since an error with a message
/// would allocate, at the limit.
fn refused() -> std::io::Error {
    std::io::ErrorKind::OutOfMemory.into()
}

/// A new thread's word that the runtime has set it up -- its signal stack,
/// its first heap -- and its work has begun. Until then the thread that
/// started it waits, so that neither its own next allocation nor the next
/// thread's check takes the room that set-up was counted on.
struct SetUp {
    done: std::sync::atomic::AtomicBool,
    starter: std::thread::Thread,
}

impl SetUp {
    fn new() -> std::sync::Arc<Self> {
        std::sync::Arc::new(SetUp {
            done: std::sync::atomic::AtomicBool::new(false),
            starter: std::thread::current(),
        })
    }

    fn finished(&self) {
        self.done.store(true, std::sync::atomic::Ordering::Release);
        self.starter.unpark();
    }

    fn wait(&self) {
        while !self.done.load(std::sync::atomic::Ordering::Acquire) {
            std::thread::park();
        }
    }
}

/// Work that is either running on its own thread or already finished.
pub enum Task<'s, T> {
    /// On a thread of its own; `join` waits for it.
    Running(ScopedJoinHandle<'s, T>),
    /// The OS refused a thread, so it ran on the caller's.
    Done(T),
}

impl<'s, T> Task<'s, T> {
    /// The result, once it exists. `Err` is a panic in the work, exactly as
    /// [`ScopedJoinHandle::join`] reports one.
    pub fn join(self) -> std::thread::Result<T> {
        match self {
            Task::Running(h) => h.join(),
            Task::Done(v) => Ok(v),
        }
    }
}

/// [`Builder::spawn_scoped`], refused up front when there is no room for the
/// thread under the heap limit, as the OS would have refused it. Under a
/// limit, returns once the thread is set up (see [`SetUp`]). Every thread here
/// is started through it or [`spawn_named`]; a refusal is handled where it
/// already was, by doing the work on the calling thread.
pub fn spawn_scoped<'s, 'e, T, F>(
    scope: &'s Scope<'s, 'e>,
    f: F,
) -> std::io::Result<ScopedJoinHandle<'s, T>>
where
    F: FnOnce() -> T + Send + 's,
    T: Send + 's,
{
    match fits() {
        None => Builder::new().spawn_scoped(scope, f),
        Some(false) => Err(refused()),
        Some(true) => {
            let set_up = SetUp::new();
            let theirs = std::sync::Arc::clone(&set_up);
            let handle = Builder::new().spawn_scoped(scope, move || {
                theirs.finished();
                f()
            })?;
            set_up.wait();
            Ok(handle)
        }
    }
}

/// [`std::thread::Builder::spawn`] of a named thread, the same way.
pub fn spawn_named<T, F>(name: &str, f: F) -> std::io::Result<std::thread::JoinHandle<T>>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    let named = Builder::new().name(name.into());
    match fits() {
        None => named.spawn(f),
        Some(false) => Err(refused()),
        Some(true) => {
            let set_up = SetUp::new();
            let theirs = std::sync::Arc::clone(&set_up);
            let handle = named.spawn(move || {
                theirs.finished();
                f()
            })?;
            set_up.wait();
            Ok(handle)
        }
    }
}

/// `f` on a scoped thread, or on this one if the OS refuses.
///
/// `f` is taken by reference and must be `Fn`, because the fallback has to be
/// able to call it after `spawn_scoped` has taken a copy: an `io::Error` does
/// not hand the closure back.
///
/// The fallback runs `f` here and now rather than at `join`, which serialises
/// this task with whatever the caller does next. That is the point — the work
/// still happens, on the thread that is already running.
pub fn spawn<'s, 'e, T, F>(scope: &'s Scope<'s, 'e>, f: &'s F) -> Task<'s, T>
where
    F: Fn() -> T + Sync + 's,
    T: Send + 's,
{
    match spawn_scoped(scope, f) {
        Ok(h) => Task::Running(h),
        Err(_) => Task::Done(f()),
    }
}

/// [`spawn`], for the common shape of one task per part index.
pub fn spawn_at<'s, 'e, T, F>(scope: &'s Scope<'s, 'e>, f: &'s F, part: usize) -> Task<'s, T>
where
    F: Fn(usize) -> T + Sync + 's,
    T: Send + 's,
{
    match spawn_scoped(scope, move || f(part)) {
        Ok(h) => Task::Running(h),
        Err(_) => Task::Done(f(part)),
    }
}

/// A value exactly one call can take.
///
/// [`spawn`] needs `Fn`, and a task that consumes something it was given -- a
/// file to prepare, a buffer to fill -- is `FnOnce`. Putting the value here
/// makes the body callable twice by type and once in fact: whichever thread gets
/// there first takes it, and the other gets `None`. Since only one of the two
/// ever runs, that second call does not happen.
pub struct Once<T>(std::sync::Mutex<Option<T>>);

impl<T> Once<T> {
    pub fn new(value: T) -> Self {
        Once(std::sync::Mutex::new(Some(value)))
    }

    /// The value, once. `None` afterwards, and `None` if the lock was poisoned
    /// by a panic -- neither is worth a panic of its own.
    pub fn take(&self) -> Option<T> {
        self.0.lock().ok().and_then(|mut held| held.take())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn spawn_runs_the_work_once() {
        let seen = AtomicUsize::new(0);
        let each = |i: usize| {
            seen.fetch_add(1, Ordering::Relaxed);
            i * 2
        };
        let out: Vec<usize> = std::thread::scope(|scope| {
            let hs: Vec<_> = (0..8).map(|i| spawn_at(scope, &each, i)).collect();
            hs.into_iter().map(|h| h.join().unwrap()).collect()
        });
        assert_eq!(out, vec![0, 2, 4, 6, 8, 10, 12, 14]);
        assert_eq!(seen.load(Ordering::Relaxed), 8);
    }

    #[test]
    fn spawn_without_an_index_works_too() {
        let f = || 41 + 1;
        let got = std::thread::scope(|scope| spawn(scope, &f).join().unwrap());
        assert_eq!(got, 42);
    }

    #[test]
    fn a_panic_in_the_work_is_reported_not_swallowed() {
        let f = || -> usize { panic!("deliberate") };
        let got = std::thread::scope(|scope| {
            let h = spawn(scope, &f);
            h.join()
        });
        assert!(got.is_err());
    }

    #[test]
    fn once_gives_the_value_up_exactly_once() {
        let held = Once::new(String::from("a.csv"));
        assert_eq!(held.take().as_deref(), Some("a.csv"));
        assert_eq!(held.take(), None);
    }

    /// `Done` is not a special case the caller has to know about: it joins the
    /// same way and carries the same value.
    #[test]
    fn an_already_finished_task_joins() {
        let t: Task<'_, u32> = Task::Done(7);
        assert_eq!(t.join().unwrap(), 7);
    }

    /// A thread the OS starts but that has no room left to set itself up runs
    /// its work here instead of taking the process down. The runtime maps an
    /// alternate signal stack for every thread it starts, on that thread and
    /// before the work, and when the mapping is refused it aborts (exit 134,
    /// "failed to allocate an alternative stack"). A stack from the C
    /// library's cache of finished threads' stacks needs no new memory, so the
    /// spawn itself succeeds at a heap limit that is already full -- which is
    /// how a comparison that had just taken the last of its heap met it.
    ///
    /// The abort is the whole process, so the case runs in a child: this test
    /// binary again, running only this test, with the limit set inside.
    #[cfg(all(target_os = "linux", target_pointer_width = "64"))]
    #[test]
    fn a_thread_with_no_room_to_start_runs_its_work_here() {
        const CHILD: &str = "COMPARA_TEST_NO_ROOM_FOR_A_THREAD";
        if std::env::var_os(CHILD).is_some() {
            no_room::run();
            return;
        }
        let exe = std::env::current_exe().expect("the test binary");
        let mut child = std::process::Command::new(exe)
            .args([
                "--exact",
                "parallel::tests::a_thread_with_no_room_to_start_runs_its_work_here",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(CHILD, "1")
            // With a backtrace asked for, the panic's report allocates at the
            // limit and deadlocks in the allocation error hook instead of
            // aborting; the deadline below is for that.
            .env("RUST_BACKTRACE", "0")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .expect("the test binary runs");
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        let status = loop {
            if let Some(status) = child.try_wait().expect("wait") {
                break Some(status);
            }
            if std::time::Instant::now() > deadline {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        };
        let mut stderr = String::new();
        if let Some(mut pipe) = child.stderr.take() {
            use std::io::Read;
            let _ = pipe.read_to_string(&mut stderr);
        }
        assert!(
            status.is_some_and(|s| s.success()),
            "the child ended with {status:?} (None: it hung):\n{stderr}"
        );
    }

    #[cfg(all(target_os = "linux", target_pointer_width = "64"))]
    mod no_room {
        use std::ffi::c_int;

        #[repr(C)]
        struct RLimit {
            cur: u64,
            max: u64,
        }

        unsafe extern "C" {
            fn getrlimit(resource: c_int, rlim: *mut RLimit) -> c_int;
            fn setrlimit(resource: c_int, rlim: *const RLimit) -> c_int;
        }

        const RLIMIT_DATA: c_int = 2;

        fn data_limit() -> RLimit {
            let mut now = RLimit { cur: 0, max: 0 };
            // SAFETY: `now` is a valid, writable rlimit for the call to fill.
            assert_eq!(unsafe { getrlimit(RLIMIT_DATA, &mut now) }, 0);
            now
        }

        fn set_soft_data_limit(cur: u64, max: u64) {
            let new = RLimit { cur, max };
            // SAFETY: a valid rlimit whose soft limit is at most the hard one.
            assert_eq!(unsafe { setrlimit(RLIMIT_DATA, &new) }, 0);
        }

        /// What `RLIMIT_DATA` is measured against: `VmData`, in bytes.
        fn data_in_use() -> u64 {
            let status = std::fs::read_to_string("/proc/self/status").expect("/proc");
            let line = status
                .lines()
                .find(|l| l.starts_with("VmData:"))
                .expect("VmData");
            let kb: String = line.chars().filter(char::is_ascii_digit).collect();
            kb.parse::<u64>().expect("a number") * 1024
        }

        pub fn run() {
            // A finished, joined thread: its stack goes to the C library's
            // cache, and the heap it allocated from keeps free room for the
            // next thread's own few allocations.
            std::thread::scope(|scope| {
                let warm = std::thread::Builder::new().spawn_scoped(scope, || {
                    std::hint::black_box(vec![1u8; 64 << 10]);
                });
                warm.expect("a thread with no limit")
                    .join()
                    .expect("joined");
            });
            let was = data_limit();
            // With room under a limit, the thread is started and waited for
            // until it is set up; the work runs there.
            set_soft_data_limit(data_in_use() + (64 << 20), was.max);
            let there = || std::thread::current().id();
            let ran_on = std::thread::scope(|scope| super::super::spawn(scope, &there).join());
            assert_ne!(ran_on.expect("the work ran"), std::thread::current().id());
            let used = data_in_use();
            // Full to the page: nothing left for a new mapping.
            set_soft_data_limit(used, was.max);
            let work = || 6 * 7;
            let got = std::thread::scope(|scope| super::super::spawn(scope, &work).join());
            set_soft_data_limit(was.cur, was.max);
            assert_eq!(got.expect("the work ran"), 42);
        }
    }
}

/// The failure a run reports when lanes take numbered jobs in order (from a
/// shared counter) and any of them may fail: the one with the lowest job
/// number, as a single lane taking the jobs in that order would meet first.
/// Without it the error depends on which lane failed first, so one input
/// gave different errors from run to run on a machine with more cores.
///
/// A lane asks [`FirstFailure::wanted`] before each job: jobs after a known
/// failure are skipped, jobs before it still run (one of them may fail
/// first, by number). Every job before the reported one has run to the end.
pub struct FirstFailure<E> {
    first: std::sync::atomic::AtomicUsize,
    error: std::sync::Mutex<Option<E>>,
}

impl<E> Default for FirstFailure<E> {
    fn default() -> Self {
        FirstFailure {
            first: std::sync::atomic::AtomicUsize::new(usize::MAX),
            error: std::sync::Mutex::new(None),
        }
    }
}

impl<E> FirstFailure<E> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether job `at` can still change the outcome: no job before it has
    /// failed.
    pub fn wanted(&self, at: usize) -> bool {
        at < self.first.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Job `at` failed with `error`; kept if no earlier job has failed.
    pub fn note(&self, at: usize, error: E) {
        let mut kept = self.error.lock().unwrap_or_else(|e| e.into_inner());
        if at < self.first.load(std::sync::atomic::Ordering::Acquire) {
            self.first.store(at, std::sync::atomic::Ordering::Release);
            *kept = Some(error);
        }
    }

    /// `Err` with the first failure by job number, if any job failed.
    pub fn into_result(self) -> Result<(), E> {
        match self.error.into_inner().unwrap_or_else(|e| e.into_inner()) {
            Some(e) => Err(e),
            None => Ok(()),
        }
    }
}

#[cfg(test)]
mod first_failure_tests {
    use super::FirstFailure;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Lanes racing through jobs where several fail: the lowest-numbered
    /// failure wins, every time, however many lanes run.
    #[test]
    fn the_lowest_numbered_failure_wins_whatever_the_timing() {
        let failing = [7usize, 3, 12];
        for lanes in [1, 2, 4, 8] {
            for _ in 0..50 {
                let (next, failed) = (AtomicUsize::new(0), FirstFailure::new());
                let ran: Vec<AtomicUsize> = (0..20).map(|_| AtomicUsize::new(0)).collect();
                std::thread::scope(|scope| {
                    for _ in 0..lanes {
                        scope.spawn(|| {
                            loop {
                                let at = next.fetch_add(1, Ordering::Relaxed);
                                if at >= 20 || !failed.wanted(at) {
                                    return;
                                }
                                // Later jobs finish sooner, so a later failure
                                // is often met first.
                                std::thread::sleep(std::time::Duration::from_micros(
                                    ((20 - at) * 50) as u64,
                                ));
                                ran[at].store(1, Ordering::Relaxed);
                                if failing.contains(&at) {
                                    failed.note(at, at);
                                    return;
                                }
                            }
                        });
                    }
                });
                assert_eq!(failed.into_result(), Err(3), "{lanes} lanes");
                assert!(
                    ran[..3].iter().all(|r| r.load(Ordering::Relaxed) == 1),
                    "jobs before it ran"
                );
            }
        }
    }
}
