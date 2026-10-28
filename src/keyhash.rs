//! The hash every engine indexes keys by, keyed per process.
//!
//! A hash here is internal: the index build and the probe only have to agree.
//! It was FNV, a word at a time from a fixed seed, and a fixed function of the
//! bytes can be steered. A file whose keys were built to share one hash put
//! every row into one probe chain: 40,000 such rows (0.9 MB) took 40 s to
//! compare where ordinary keys take 10 ms, four times longer for each doubling.
//! A seed alone does not stop that for FNV: flipping a word's top bit changes
//! its state by a constant whatever the seed, and the next word can cancel it.
//!
//! So each word is folded in with a full 64x64->128-bit multiply whose two
//! halves are xored together (the fold of foldhash and wyhash), by a multiplier
//! and from a start chosen at random for each process. What a word's change
//! does to the state then depends on the state, which depends on the secret;
//! keys can no longer be built to collide without it. On x86-64 and arm64 the
//! wide multiply is one instruction, as fast as FNV's.
//!
//! `COMPARA_HASH_SEED=<u64>` fixes the keys, for a benchmark or a bug report
//! that must run the same way twice. Nothing a run writes depends on them.

use std::sync::OnceLock;

/// The two secrets: where a hash starts, and the multiplier each word is
/// folded in by (odd, and with its top bit set, so no input bit is lost).
#[derive(Clone, Copy, Debug)]
pub struct Keys {
    pub start: u64,
    pub mul: u64,
}

static KEYS: OnceLock<Keys> = OnceLock::new();

/// This process's keys, made on first use.
#[inline]
pub fn keys() -> Keys {
    *KEYS.get_or_init(|| {
        let seed = std::env::var("COMPARA_HASH_SEED")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok());
        from_seed(seed.unwrap_or_else(random))
    })
}

/// The keys a seed stands for.
pub fn from_seed(seed: u64) -> Keys {
    Keys {
        start: mix(seed ^ 0x243f_6a88_85a3_08d3),
        mul: mix(seed ^ 0x1319_8a2e_0370_7344) | (1 << 63) | 1,
    }
}

/// Sixty-four bits from the operating system's random source, through the
/// keys std draws for its own hash maps (no extra dependency).
fn random() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u64(std::process::id() as u64);
    h.finish()
}

/// splitmix64's finaliser: spreads a seed into well-mixed keys.
fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// `a * b` as 128 bits, the halves xored: every bit of each input reaches
/// both halves of the output.
#[inline(always)]
pub fn fold(a: u64, b: u64) -> u64 {
    let p = (a as u128).wrapping_mul(b as u128);
    (p as u64) ^ ((p >> 64) as u64)
}

/// One word into a running hash.
#[inline(always)]
pub fn word(h: u64, w: u64, mul: u64) -> u64 {
    fold(h ^ w, mul)
}

/// The last `rem` bytes of `v`, one to seven of them, as a little-endian word
/// padded with zeros.
///
/// This was a copy into a zeroed buffer, and a copy of a length only known at
/// run time is a call to memcpy, once per key cell per row. A value of eight
/// bytes or more has eight real bytes ending where the tail does, so one load
/// and a shift gives the same word; a shorter one is read as two overlapping
/// halves, or as its first, middle and last byte. The Zig port does the same
/// (`scan.tailWord`).
#[inline]
pub fn tail_word(v: &[u8], rem: usize) -> u64 {
    let n = v.len();
    debug_assert!(rem > 0 && rem < 8 && rem <= n);
    if n >= 8 {
        let last = u64::from_le_bytes(v[n - 8..].try_into().unwrap());
        return last >> (8 * (8 - rem));
    }
    // Shorter than a word: nothing was folded yet, so the tail is all of it.
    if n >= 4 {
        let lo = u32::from_le_bytes(v[..4].try_into().unwrap()) as u64;
        let hi = u32::from_le_bytes(v[n - 4..].try_into().unwrap()) as u64;
        return lo | (hi << (8 * (n - 4)));
    }
    v[0] as u64 | (v[n / 2] as u64) << (8 * (n / 2)) | (v[n - 1] as u64) << (8 * (n - 1))
}
/// What an absent cell folds in: distinct from every value, the empty one
/// included (a value folds its length after its bytes).
pub const ABSENT: u64 = 0x9e37_79b9_7f4a_7c15;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_seed_gives_the_same_keys_and_the_keys_are_odd_and_wide() {
        let (a, b) = (from_seed(7), from_seed(7));
        assert_eq!((a.start, a.mul), (b.start, b.mul));
        assert_ne!(from_seed(7).mul, from_seed(8).mul);
        for s in [0, 1, u64::MAX, 0x8000_0000_0000_0000] {
            let k = from_seed(s);
            assert_eq!(k.mul & 1, 1);
            assert_eq!(k.mul >> 63, 1);
        }
    }

    #[test]
    fn a_words_effect_depends_on_the_state() {
        // Under FNV, flipping a word's top bit changed the state by one
        // constant whatever the state, so the next word could cancel it and
        // keys could be built to collide. Folded, the change depends on the
        // state, which depends on the secret start.
        let k = from_seed(42);
        let flip = |h: u64, w: u64| word(h, w, k.mul) ^ word(h, w ^ (1 << 63), k.mul);
        let first = flip(k.start, 0x4142_4344_4546_4748);
        let same = (1..10_000u64)
            .filter(|i| flip(k.start.wrapping_add(i * 0x9e37_79b9), 0x4142_4344_4546_4748) == first)
            .count();
        assert!(same < 10, "{same} of 10000 states changed alike");
    }
}
