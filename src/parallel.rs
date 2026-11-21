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
//! Nor is the thread that starts one the only one taking room: while a lane
//! is being set up, its siblings already at work, or the fused join's index
//! build on the main thread, could take it: the fused join still aborted in
//! 8 of 690 runs at the limits where it had aborted before. So while any
//! thread is being set up, every other thread's allocation waits for it (see
//! [`wait_for_set_up`]).

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
/// early, which costs a core and not an answer. Nor does a thread take room
/// promised to work under way ([`crate::memory::promise`]).
fn fits() -> Option<bool> {
    let limit = crate::memory::heap_limit()?;
    let used = crate::memory::data_in_use()?;
    Some(
        used.saturating_add(crate::memory::promised())
            .saturating_add(stack_size())
            .saturating_add(SET_UP)
            <= limit,
    )
}

/// Threads started under a heap limit and not yet set up; while there are
/// any, other threads' allocations wait (see [`wait_for_set_up`]).
static SETTING_UP: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

thread_local! {
    /// Whether this thread is starting one: its own allocations, for the new
    /// thread's handle and closure, do not wait for the set-up they precede.
    static STARTING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };

    /// Whether this thread is at work: one started here, from the moment its
    /// work begins, or one that has started another. A thread being set up is
    /// not, and must not wait: the runtime allocates on it before its work
    /// begins (the record of its guard page, at some starts), and waiting for
    /// its own set-up held each such start for the whole [`SET_UP_WAIT`]
    /// (measured: two in seventy starts of a 16-lane comparison, 0.4 s each).
    static RUNNING: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// How long an allocation waits for a set-up at most. A set-up is a few
/// system calls, tens of microseconds; this is for a thread the scheduler has
/// not run yet on a busy machine. Past it the allocation goes ahead, as it
/// would have without the wait.
const SET_UP_WAIT: std::time::Duration = std::time::Duration::from_millis(200);

/// Called by the global allocator ([`crate::hugepages`]) before every
/// allocation: returns at once unless a thread is being set up under a heap
/// limit, and then waits until it is. The room [`fits`] counted for the new
/// thread is taken by the thread itself and not by another's allocation in
/// the meantime -- a refused allocation is a clear error, a refused set-up is
/// the abort.
///
/// Only allocations through the global allocator wait; the C library's own
/// and raw mappings do not, the thread starting a new one does not wait for
/// its own, and a thread not at work here (see [`RUNNING`]) does not wait:
/// one being set up, or one a library started that never starts one here.
#[inline]
pub fn wait_for_set_up() {
    if SETTING_UP.load(std::sync::atomic::Ordering::Relaxed) != 0 {
        wait_for_set_up_slowly();
    }
}

#[cold]
fn wait_for_set_up_slowly() {
    wait_for_set_up_within(SET_UP_WAIT);
}

/// [`wait_for_set_up`]'s wait, bounded at `most`.
fn wait_for_set_up_within(most: std::time::Duration) {
    // `try_with`: during this thread's own teardown the flag is gone, and an
    // allocation then has nothing to wait for that it could not race anyway.
    if STARTING.try_with(std::cell::Cell::get).unwrap_or(true)
        || !RUNNING.try_with(std::cell::Cell::get).unwrap_or(false)
    {
        return;
    }
    let since = std::time::Instant::now();
    let mut turns = 0u32;
    while SETTING_UP.load(std::sync::atomic::Ordering::Acquire) != 0 && since.elapsed() < most {
        // A few turns given up, then sleeps: with more threads than cores,
        // threads that only yield keep the new one from the core its set-up
        // needs (measured: 16 lanes on 4 cores, three times the wall time).
        if turns < YIELDS {
            turns += 1;
            std::thread::yield_now();
        } else {
            std::thread::sleep(NAP);
        }
    }
}

/// How often a waiting allocation yields before it sleeps, and for how long.
const YIELDS: u32 = 4;
const NAP: std::time::Duration = std::time::Duration::from_micros(50);

/// This thread, starting another under a heap limit: counted in
/// [`SETTING_UP`] from before the room is looked at until the count is handed
/// to the new thread ([`Starting::hand_over`]), or until this is dropped -- a
/// refusal, a spawn error, a panic -- so that no way out leaves the count
/// behind, and every later allocation waiting out [`SET_UP_WAIT`].
struct Starting {
    was: bool,
    counted: bool,
}

impl Starting {
    fn begin() -> Self {
        let was = STARTING.replace(true);
        RUNNING.set(true);
        SETTING_UP.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Starting { was, counted: true }
    }

    /// The count, moved into the new thread's closure: released when the
    /// closure begins, or when it is dropped without having run.
    fn hand_over(&mut self, set_up: std::sync::Arc<SetUp>) -> Counted {
        self.counted = false;
        Counted(set_up)
    }
}

impl Drop for Starting {
    fn drop(&mut self) {
        if self.counted {
            SETTING_UP.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
        }
        STARTING.set(self.was);
    }
}

/// A thread's place in [`SETTING_UP`], held by its closure until the closure
/// begins: dropping it is the new thread's word that it is set up.
struct Counted(std::sync::Arc<SetUp>);

impl Drop for Counted {
    fn drop(&mut self) {
        self.0.finished();
    }
}

/// One start at a time under a heap limit, from the look at the room to the
/// new thread's set-up: two threads starting one each at once both saw room
/// for one, and the second set-up found none (measured: 3 of 100 runs of a
/// 16-lane comparison at one limit aborted). Held only around a start, which
/// a set-up keeps to tens of microseconds; a lock, not a count, so that it
/// allocates nothing.
fn one_start() -> std::sync::MutexGuard<'static, ()> {
    static ONE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    ONE.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
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
        SETTING_UP.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
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
    if crate::memory::heap_limit().is_none() {
        return Builder::new().spawn_scoped(scope, f);
    }
    let _one = one_start();
    let mut starting = Starting::begin();
    match fits() {
        None => {
            drop(starting);
            Builder::new().spawn_scoped(scope, f)
        }
        Some(false) => {
            drop(starting);
            Err(refused())
        }
        Some(true) => {
            let set_up = SetUp::new();
            let counted = starting.hand_over(std::sync::Arc::clone(&set_up));
            let handle = Builder::new().spawn_scoped(scope, move || {
                RUNNING.set(true);
                drop(counted);
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
    if crate::memory::heap_limit().is_none() {
        return named.spawn(f);
    }
    let _one = one_start();
    let mut starting = Starting::begin();
    match fits() {
        None => {
            drop(starting);
            named.spawn(f)
        }
        Some(false) => {
            drop(starting);
            Err(refused())
        }
        Some(true) => {
            let set_up = SetUp::new();
            let counted = starting.hand_over(std::sync::Arc::clone(&set_up));
            let handle = named.spawn(move || {
                RUNNING.set(true);
                drop(counted);
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

    /// The tests that move [`SETTING_UP`] hold the turn a start holds (see
    /// [`one_start`]), one at a time and apart from every real start: when
    /// the test process runs under a heap limit, the other tests' threads
    /// are started through the gated path and move the count too.
    fn setting_up_test() -> std::sync::MutexGuard<'static, ()> {
        one_start()
    }

    /// While a thread is being set up, another thread's allocation waits until
    /// it is, and the thread starting it does not wait for its own. The wait
    /// is given a bound no busy machine reaches, so that returning before it
    /// is the set-up's end and not the bound's.
    #[test]
    fn an_allocation_waits_for_a_thread_being_set_up() {
        use std::time::{Duration, Instant};
        let _one = setting_up_test();
        const NEVER: Duration = Duration::from_secs(60);
        let (counted, count) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel::<()>();
        let starter = std::thread::spawn(move || {
            let starting = Starting::begin();
            let at = Instant::now();
            wait_for_set_up_within(NEVER);
            let own = at.elapsed();
            counted.send(()).unwrap();
            released.recv().unwrap();
            drop(starting);
            own
        });
        count.recv().unwrap();
        // A thread not at work -- one being set up -- goes ahead.
        let idle = std::thread::spawn(|| {
            let at = Instant::now();
            wait_for_set_up_within(NEVER);
            at.elapsed()
        });
        assert!(idle.join().unwrap() < Duration::from_secs(30));
        let at = Instant::now();
        let waiter = std::thread::spawn(move || {
            RUNNING.set(true);
            wait_for_set_up_within(NEVER);
            at.elapsed()
        });
        std::thread::sleep(Duration::from_millis(50));
        release.send(()).unwrap();
        let waited = waiter.join().unwrap();
        assert!(starter.join().unwrap() < NEVER);
        assert!(waited >= Duration::from_millis(50), "{waited:?}");
        assert!(waited < NEVER, "{waited:?}");
        assert_eq!(SETTING_UP.load(std::sync::atomic::Ordering::Acquire), 0);
    }

    /// A set-up that never ends holds an allocation for the bound and no more.
    #[test]
    fn an_allocation_waits_no_longer_than_its_bound() {
        use std::time::{Duration, Instant};
        let _one = setting_up_test();
        let starting = std::thread::spawn(Starting::begin).join();
        RUNNING.set(true);
        let at = Instant::now();
        wait_for_set_up_within(Duration::from_millis(30));
        let waited = at.elapsed();
        // The guard was made on a thread that has ended; dropping it here
        // releases the count, and sets this thread's flag back to what that
        // thread's was, which is this one's too.
        drop(starting);
        assert!(waited >= Duration::from_millis(30), "{waited:?}");
        assert!(waited < Duration::from_secs(60), "{waited:?}");
        assert_eq!(SETTING_UP.load(std::sync::atomic::Ordering::Acquire), 0);
    }

    /// No way out of starting a thread leaves it counted as being set up:
    /// not a panic before the spawn, and not a closure dropped without
    /// having run (what a refused spawn does with it).
    #[test]
    fn a_start_given_up_leaves_nothing_counted() {
        use std::sync::atomic::Ordering::Acquire;
        let _one = setting_up_test();
        let panicked = std::panic::catch_unwind(|| {
            let _starting = Starting::begin();
            assert_eq!(SETTING_UP.load(Acquire), 1);
            panic!("given up between counting and spawning");
        });
        assert!(panicked.is_err());
        assert_eq!(SETTING_UP.load(Acquire), 0);
        assert!(!STARTING.get());

        let mut starting = Starting::begin();
        let set_up = SetUp::new();
        let counted = starting.hand_over(std::sync::Arc::clone(&set_up));
        let closure = move || {
            drop(counted);
        };
        drop(starting);
        assert_eq!(SETTING_UP.load(Acquire), 1, "handed over, still counted");
        drop(closure);
        assert_eq!(SETTING_UP.load(Acquire), 0);
        set_up.wait();
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
            // One start at a time: a start waits while another has the turn.
            {
                use std::sync::atomic::{AtomicBool, Ordering};
                let turn = super::super::one_start();
                let started = AtomicBool::new(false);
                std::thread::scope(|scope| {
                    let starter = std::thread::Builder::new()
                        .spawn_scoped(scope, || {
                            std::thread::scope(|inner| {
                                let h = super::super::spawn_scoped(inner, || ()).expect("room");
                                started.store(true, Ordering::Release);
                                h.join().expect("joined");
                            });
                        })
                        .expect("a thread with room for it");
                    std::thread::sleep(std::time::Duration::from_millis(50));
                    let early = started.load(Ordering::Acquire);
                    drop(turn);
                    starter.join().expect("joined");
                    assert!(!early, "a start went ahead of the one under way");
                });
                assert!(started.load(Ordering::Acquire));
            }
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
