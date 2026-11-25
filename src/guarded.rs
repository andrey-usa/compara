//! Bytes laid against a page the process may not touch, for tests.
//!
//! A vector load that runs past its slice reads whatever lies next in the
//! allocation, and nothing fails: the bytes are masked off, or the answer
//! happens not to depend on them. CI's tests are built without debug
//! assertions, so the bound each scan states as a `debug_assert!` is not
//! checked there either. Bytes that end where an unreadable page starts turn
//! that over-read into a fault in every build: a scan that reads one byte past
//! the end of a [`Guarded`] stops the test process with SIGSEGV (or SIGBUS),
//! and so does one that reads before its start where the bytes begin a page.
//!
//! Unix only, where `mmap` and `mprotect` give the page; elsewhere, and under
//! Miri (which checks every access against its allocation itself), the bytes
//! are an exact-length allocation, and the scans' own tests still compare
//! their answers.

/// How many rounds a randomized test runs: `native`, or a hundredth of it
/// under Miri, which interprets each step about a thousand times slower. The
/// edges a scan can get wrong come up in the first rounds as in the last;
/// the native count is for the rarer shapes.
pub(crate) const fn rounds(native: usize) -> usize {
    if cfg!(miri) { native / 100 } else { native }
}

/// The step through an exhaustive range: every value natively, every
/// `under_miri`th under Miri. Prime steps keep every alignment in reach.
pub(crate) const fn every(under_miri: usize) -> usize {
    if cfg!(miri) { under_miri } else { 1 }
}

/// Bytes with no readable byte right after them ([`Guarded::ending`]) or right
/// before them ([`Guarded::starting`]).
pub(crate) struct Guarded {
    #[cfg(all(unix, not(miri)))]
    map: *mut u8,
    #[cfg(all(unix, not(miri)))]
    total: usize,
    #[cfg(all(unix, not(miri)))]
    at: usize,
    #[cfg(not(all(unix, not(miri))))]
    bytes: Box<[u8]>,
    len: usize,
}

// The mapping is this value's alone, as a `Vec`'s buffer is.
unsafe impl Send for Guarded {}
unsafe impl Sync for Guarded {}

impl Guarded {
    /// `bytes`, ending where an unreadable page begins.
    pub(crate) fn ending(bytes: &[u8]) -> Guarded {
        Guarded::new(bytes, true)
    }

    /// `bytes`, beginning where an unreadable page ends.
    pub(crate) fn starting(bytes: &[u8]) -> Guarded {
        Guarded::new(bytes, false)
    }

    /// `len` zero bytes to write to, ending where an unreadable page begins.
    pub(crate) fn room(len: usize) -> Guarded {
        Guarded::new(&vec![0; len], true)
    }

    #[cfg(all(unix, not(miri)))]
    fn new(bytes: &[u8], ending: bool) -> Guarded {
        // Safety: sysconf only reads a constant of the system.
        let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
        let inner = bytes.len().div_ceil(page).max(1) * page;
        // An unreadable page on each side of the bytes' own pages.
        let total = inner + 2 * page;
        // Safety: a fresh anonymous mapping, at an address the kernel picks;
        // nothing else holds it.
        let map = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                total,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_PRIVATE | libc::MAP_ANON,
                -1,
                0,
            )
        };
        assert!(map != libc::MAP_FAILED, "mmap of {total} bytes failed");
        let map = map.cast::<u8>();
        // Safety: both ranges are whole pages of the mapping just made.
        let fenced = unsafe {
            libc::mprotect(map.cast(), page, libc::PROT_NONE) == 0
                && libc::mprotect(map.add(page + inner).cast(), page, libc::PROT_NONE) == 0
        };
        assert!(fenced, "mprotect failed");
        let at = if ending {
            page + inner - bytes.len()
        } else {
            page
        };
        // Safety: `at..at + len` lies in the readable pages, which nothing
        // else can see yet.
        unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), map.add(at), bytes.len()) };
        Guarded {
            map,
            total,
            at,
            len: bytes.len(),
        }
    }

    #[cfg(not(all(unix, not(miri))))]
    fn new(bytes: &[u8], _ending: bool) -> Guarded {
        Guarded {
            bytes: bytes.into(),
            len: bytes.len(),
        }
    }

    /// The first byte, to write through ([`Guarded::room`]).
    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.as_mut().as_mut_ptr()
    }
}

impl std::ops::Deref for Guarded {
    type Target = [u8];
    fn deref(&self) -> &[u8] {
        #[cfg(all(unix, not(miri)))]
        // Safety: `at..at + len` lies in the readable pages of the mapping,
        // which lives as long as `self`, and was written in `new`.
        return unsafe { std::slice::from_raw_parts(self.map.add(self.at), self.len) };
        #[cfg(not(all(unix, not(miri))))]
        return &self.bytes[..self.len];
    }
}

impl AsMut<[u8]> for Guarded {
    fn as_mut(&mut self) -> &mut [u8] {
        #[cfg(all(unix, not(miri)))]
        // Safety: as in `deref`, and `&mut self` makes this the only view.
        return unsafe { std::slice::from_raw_parts_mut(self.map.add(self.at), self.len) };
        #[cfg(not(all(unix, not(miri))))]
        return &mut self.bytes[..self.len];
    }
}

#[cfg(all(unix, not(miri)))]
impl Drop for Guarded {
    fn drop(&mut self) {
        // Safety: the whole mapping made in `new`, unmapped once; no slice of
        // it outlives `self`.
        unsafe { libc::munmap(self.map.cast(), self.total) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_bytes_are_kept_and_meet_a_page_at_the_named_end() {
        for len in [0, 1, 31, 32, 4095, 4096, 4097, 10_000] {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 7) as u8).collect();
            for (g, ending) in [
                (Guarded::ending(&bytes), true),
                (Guarded::starting(&bytes), false),
            ] {
                assert_eq!(&g[..], &bytes[..]);
                #[cfg(not(all(unix, not(miri))))]
                let _ = ending;
                #[cfg(all(unix, not(miri)))]
                {
                    // Safety: sysconf only reads a constant of the system.
                    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) } as usize;
                    let edge = if ending {
                        g.as_ptr() as usize + len
                    } else {
                        g.as_ptr() as usize
                    };
                    assert_eq!(edge % page, 0, "len {len}, ending {ending}");
                }
            }
        }
        let mut room = Guarded::room(100);
        // Safety: 100 bytes of room.
        unsafe { room.as_mut_ptr().add(99).write(7) };
        assert_eq!(room[99], 7);
    }

    /// The guard itself: a read of the byte after an [`Guarded::ending`], or
    /// of the byte before a [`Guarded::starting`], ends a child process with
    /// a signal.
    #[cfg(all(unix, not(miri)))]
    #[test]
    fn a_read_past_either_guarded_end_faults() {
        let ending = Guarded::ending(b"abc");
        let starting = Guarded::starting(b"abc");
        for p in [
            ending.as_ptr().wrapping_add(3),
            starting.as_ptr().wrapping_sub(1),
        ] {
            // Safety: the child only reads `p` and exits; the read is the
            // fault this test expects, in a process of its own.
            let pid = unsafe { libc::fork() };
            assert!(pid >= 0, "fork failed");
            if pid == 0 {
                unsafe {
                    std::ptr::read_volatile(p);
                    libc::_exit(0);
                }
            }
            let mut status = 0;
            // Safety: waits for the child just made.
            assert_eq!(unsafe { libc::waitpid(pid, &mut status, 0) }, pid);
            assert!(libc::WIFSIGNALED(status), "status {status}");
        }
    }
}
