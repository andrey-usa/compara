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

use std::thread::{Builder, Scope, ScopedJoinHandle};

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
    match Builder::new().spawn_scoped(scope, f) {
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
    match Builder::new().spawn_scoped(scope, move || f(part)) {
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
