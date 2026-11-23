//! The bytes a field points into, and the one route every comparison reads them
//! through.
//!
//! A field is an offset and a length, so something has to say what those address
//! and how the bytes are escaped. For CSV and JSON that is the mapped file; for
//! Parquet it is an arena of decoded values, which is the same thing to everyone
//! downstream. The escape rule rides along because it follows the format: CSV
//! doubles a quote, JSON puts a backslash in front of one, and a decoded Parquet
//! value is already literal.
//!
//! Hashing, equality and decoding all read a field through [`Slab::logical`], so
//! they cannot come to different conclusions about the same value — the property
//! whose absence produced two silently wrong answers in the Java port.

use std::fs::File;
use std::path::Path;

use memmap2::Mmap;

use super::field::{Field, is_escaped, is_real, len_of, offset_of};
use crate::error::{Error, Result};

/// Which escape rule the bytes behind a field follow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Dialect {
    /// A doubled quote is one quote.
    Csv,
    /// A backslash escape, `\uXXXX` included.
    Json,
    /// The bytes are the value: nothing to undo. Parquet decodes to this.
    Raw,
}

/// Where the bytes came from. A mapping is paged in on demand and evictable; an
/// arena is what a columnar format leaves behind once its pages are decoded.
enum Bytes {
    Mapped { _file: File, map: Mmap },
    Owned(Vec<u8>),
}

pub(super) struct Slab {
    bytes: Bytes,
    dialect: Dialect,
    /// Where the data starts: past a UTF-8 byte order mark, which is not part
    /// of the first header name (or of the first JSON object).
    start: usize,
    /// Whether `release` lets go of pages; see `release_as_read`.
    releasing: bool,
}

const BOM: &[u8] = b"\xEF\xBB\xBF";

/// The unit [`Slab::release`] rounds to. Where the kernel's pages are larger
/// it rounds the start down to one of its own, which can release a few bytes
/// before `from`: they are faulted back in by whatever reads them next.
/// Linux only, as the release is.
#[cfg(target_os = "linux")]
const PAGE: usize = 4096;

impl Slab {
    /// Maps a file read-only, with a hint that it will be read front to back.
    pub(super) fn map(path: &Path) -> Result<Self> {
        let file = File::open(path)
            .map_err(|e| Error::new(format!("cannot read {}: {e}", path.display())))?;
        // Safety: the file is opened read-only and not modified while mapped.
        // A concurrent truncation would be a torn read, which is the documented
        // hazard of every mapping engine here and of DuckDB's reader too.
        let map = unsafe { Mmap::map(&file) }
            .map_err(|e| Error::new(format!("cannot map {}: {e}", path.display())))?;
        // Unix only: memmap2 gates `Advice` and `advise` behind `#[cfg(unix)]`,
        // because it is `madvise` underneath and Windows has no equivalent worth
        // wrapping. The hint is an optimisation the kernel may ignore anyway, so
        // its absence costs nothing but readahead on a platform that does its
        // own. Without the gate this port does not compile on Windows at all.
        #[cfg(unix)]
        let _ = map.advise(memmap2::Advice::Sequential);
        let start = if map.starts_with(BOM) { BOM.len() } else { 0 };
        Ok(Slab {
            bytes: Bytes::Mapped { _file: file, map },
            dialect: Dialect::Csv,
            start,
            releasing: false,
        })
    }

    /// Asks for `len` bytes at `from` to be read in, and returns without
    /// waiting for them. A hint only: bytes already in memory, and bytes not
    /// mapped from a file, make it a no-op.
    pub(super) fn will_need(&self, from: usize, len: usize) {
        #[cfg(unix)]
        if let Bytes::Mapped { map, .. } = &self.bytes {
            let from = (from + self.start).min(map.len());
            let len = len.min(map.len() - from);
            if len > 0 {
                let _ = map.advise_range(memmap2::Advice::WillNeed, from, len);
            }
        }
        #[cfg(not(unix))]
        let _ = (from, len);
    }

    /// Has `release` let go of what it is given from here on: for a pair read
    /// past the memory it has, where the text engine joins inside A's sweep.
    ///
    /// A mapped file's pages count as this process's for as long as it maps
    /// them, though the kernel keeps them in its page cache either way. Under
    /// a container's limit that costs twice: reclaiming a page some process
    /// still maps means finding and unmapping it first, so with both files
    /// mapped whole the kernel works hardest on the pages it should give up
    /// most easily -- the ones the sweep has finished with. Released, they are
    /// plain page cache, the cheapest memory to take back. On a 10M-row pair
    /// in a 1 GB or 768 MB cgroup that took a few per cent to a tenth off the
    /// run.
    ///
    /// Where both files fit, the pages are left mapped: there releasing them
    /// only to fault them back in costs the run time. It would cut peak RSS
    /// (411 to about 125 MB on 1M rows) by more than it costs anywhere else,
    /// but measured whole it cost over a tenth of the wall time at 10M rows:
    /// the sweep's pages are read again by the join, and the insertion's and
    /// the join's proofs of a matching 32-bit hash read scattered rows, each
    /// of which maps back a folio of up to a few hundred kilobytes.
    ///
    /// Linux only: elsewhere `MADV_DONTNEED` is a hint about what to evict,
    /// not a release. Not for bytes this process built: there is nothing to
    /// fault them back in from.
    pub(super) fn release_as_read(&mut self) {
        self.releasing = cfg!(target_os = "linux") && matches!(self.bytes, Bytes::Mapped { .. });
    }

    /// Whether `release` lets go of pages.
    pub(super) fn releasing(&self) -> bool {
        self.releasing
    }

    /// Takes the whole pages inside `from..to` of the data out of this
    /// process, if `release_as_read` asked for that, and returns where the
    /// next range to release should start: `to` less the part page it leaves
    /// mapped (`to` itself when it releases nothing).
    ///
    /// Released pages stay in the page cache, and the next read of one maps
    /// it again: a minor fault, or a disk read if memory ran short in between,
    /// as it could have anyway. So a release changes what a later read costs,
    /// never what it reads.
    pub(super) fn release(&self, from: usize, to: usize) -> usize {
        if !self.releasing {
            return to;
        }
        #[cfg(target_os = "linux")]
        if let Bytes::Mapped { map, .. } = &self.bytes {
            let lo = (from + self.start).next_multiple_of(PAGE);
            let hi = (to + self.start).min(map.len()) & !(PAGE - 1);
            if hi > lo {
                // Safety: the mapping is shared and read-only (`Mmap::map`),
                // of a file opened read-only and not modified while mapped (see
                // `map`). Dropping its page table entries leaves the bytes in
                // the file and the page cache, and any later read of the range
                // faults the same bytes back in, so no slice into the mapping
                // ever sees different contents. (On a private mapping with
                // pages written to it would not hold: those pages would be
                // lost. This one has none.)
                let _ = unsafe {
                    map.unchecked_advise_range(memmap2::UncheckedAdvice::DontNeed, lo, hi - lo)
                };
            }
            return hi.max(from + self.start) - self.start;
        }
        let _ = from;
        to
    }

    /// Releases every whole page of the data: for a phase that reads it back
    /// at random, a few rows at a time.
    pub(super) fn release_all(&self) {
        self.release(0, self.data().len());
    }

    /// A slab over bytes this process built: the Parquet reader's arena.
    pub(super) fn owned(data: Vec<u8>, dialect: Dialect) -> Self {
        Slab {
            bytes: Bytes::Owned(data),
            dialect,
            start: 0,
            releasing: false,
        }
    }

    pub(super) fn set_dialect(&mut self, dialect: Dialect) {
        self.dialect = dialect;
    }

    pub(super) fn dialect(&self) -> Dialect {
        self.dialect
    }

    pub(super) fn data(&self) -> &[u8] {
        match &self.bytes {
            Bytes::Mapped { map, .. } => &map[self.start..],
            Bytes::Owned(v) => v,
        }
    }

    /// The field's raw span, still holding whatever escapes it was written with.
    pub(super) fn raw(&self, f: Field) -> &[u8] {
        if !is_real(f) {
            return &[];
        }
        let (at, len) = (offset_of(f), len_of(f));
        &self.data()[at..at + len]
    }

    /// The field's logical bytes: the value, with its escapes undone.
    pub(super) fn logical(&self, f: Field) -> LogicalBytes<'_> {
        LogicalBytes::new(self.raw(f), self.mode(f))
    }

    /// How the field's bytes are read: as they stand, or through its
    /// dialect's escapes, or as a JSON object or array.
    ///
    /// The JSON reader flags a nested value as escaped, because its bytes are
    /// not its value as they stand: its spacing is not part of it. A string
    /// carries the same flag when it holds a backslash, and both may open with
    /// a bracket. What tells them apart is the byte before: a string's field
    /// starts just past its opening quote, and a nested value's at its own
    /// bracket, which follows the colon or a space, never a quote.
    #[inline]
    fn mode(&self, f: Field) -> Mode {
        if !is_real(f) || !is_escaped(f) {
            return Mode::Raw;
        }
        match self.dialect {
            Dialect::Raw => Mode::Raw,
            Dialect::Csv => Mode::Csv,
            Dialect::Json => {
                let (at, data) = (offset_of(f), self.data());
                if matches!(data.get(at), Some(b'{' | b'[')) && at > 0 && data[at - 1] != b'"' {
                    Mode::Nested
                } else {
                    Mode::Json
                }
            }
        }
    }
}

/// How [`LogicalBytes`] reads a field's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// The bytes are the value.
    Raw,
    /// A quoted CSV field with a doubled quote, or text after its closing one.
    Csv,
    /// A JSON string with a backslash escape.
    Json,
    /// A JSON object or array, read as its text with the spacing between its
    /// tokens dropped and each string spelled one way; see
    /// [`LogicalBytes::next_nested`].
    Nested,
}

/// A field's bytes with its escapes undone, one byte at a time and without
/// allocating. A `\uXXXX` escape decodes to as many as four bytes, which is what
/// the small pending buffer is for.
pub(super) struct LogicalBytes<'a> {
    raw: &'a [u8],
    mode: Mode,
    at: usize,
    /// CSV: the field's closing quote has been passed, and what follows it up
    /// to the delimiter is content as it stands. Nested JSON: inside a string.
    closed: bool,
    /// Nested JSON: the last byte given outside a string.
    last: u8,
    /// Up to six bytes: `\u001f`, the longest spelling a nested string's
    /// character is given.
    pending: [u8; 6],
    pending_len: u8,
    pending_at: u8,
}

/// The characters of a JSON string with its escapes undone, `raw` being its
/// bytes between the quotes: what [`Slab::logical`] reads a string field as,
/// for a string that is not a field. The row parser reads a member name that
/// holds a backslash through this, so it finds the column [`text_of`] named.
pub(super) fn json_string(raw: &[u8]) -> LogicalBytes<'_> {
    LogicalBytes::new(raw, Mode::Json)
}

impl<'a> LogicalBytes<'a> {
    fn new(raw: &'a [u8], mode: Mode) -> Self {
        LogicalBytes {
            raw,
            mode,
            at: 0,
            closed: false,
            last: b'[',
            pending: [0; 6],
            pending_len: 0,
            pending_at: 0,
        }
    }
}

impl LogicalBytes<'_> {
    /// Returns the first of `bytes` and holds the rest.
    fn give(&mut self, bytes: &[u8]) -> u8 {
        self.pending[..bytes.len()].copy_from_slice(bytes);
        self.pending_len = bytes.len() as u8;
        self.pending_at = 1;
        bytes[0]
    }

    /// Encodes one code point, returning its first byte and holding the rest.
    ///
    /// The encoding is done by hand rather than through `char`, which refuses an
    /// unpaired surrogate: a file carrying one is malformed, but the C, C++ and
    /// Zig ports encode it rather than substituting a replacement character, and
    /// two ports that disagree about the bytes of a value would disagree about
    /// its hash.
    fn hold(&mut self, cp: u32) -> u8 {
        let mut buf = [0u8; 4];
        let n = match cp {
            0..=0x7F => {
                buf[0] = cp as u8;
                1
            }
            0x80..=0x7FF => {
                buf[0] = 0xC0 | (cp >> 6) as u8;
                buf[1] = 0x80 | (cp & 0x3F) as u8;
                2
            }
            0x800..=0xFFFF => {
                buf[0] = 0xE0 | (cp >> 12) as u8;
                buf[1] = 0x80 | ((cp >> 6) & 0x3F) as u8;
                buf[2] = 0x80 | (cp & 0x3F) as u8;
                3
            }
            _ => {
                buf[0] = 0xF0 | (cp >> 18) as u8;
                buf[1] = 0x80 | ((cp >> 12) & 0x3F) as u8;
                buf[2] = 0x80 | ((cp >> 6) & 0x3F) as u8;
                buf[3] = 0x80 | (cp & 0x3F) as u8;
                4
            }
        };
        self.give(&buf[..n])
    }
}

impl Iterator for LogicalBytes<'_> {
    type Item = u8;

    fn next(&mut self) -> Option<u8> {
        if self.pending_at < self.pending_len {
            let b = self.pending[self.pending_at as usize];
            self.pending_at += 1;
            return Some(b);
        }
        let b = *self.raw.get(self.at)?;
        self.at += 1;
        match self.mode {
            Mode::Raw => Some(b),
            Mode::Csv => {
                if b != b'"' || self.closed {
                    return Some(b);
                }
                // A quote inside a quoted body is half of a doubled pair, and
                // the second one is dropped.
                if self.raw.get(self.at) == Some(&b'"') {
                    self.at += 1;
                    return Some(b);
                }
                // Or it is the closing quote of a field with text after it
                // (`"ab"cd`, see `text::quoted_with_tail`): the quote goes,
                // and the text is content, its quotes included.
                self.closed = true;
                let b = *self.raw.get(self.at)?;
                self.at += 1;
                Some(b)
            }
            Mode::Nested => self.next_nested(b),
            Mode::Json => {
                if b != b'\\' {
                    return Some(b);
                }
                let Some(&e) = self.raw.get(self.at) else {
                    return Some(b);
                };
                self.at += 1;
                Some(match e {
                    b'n' => b'\n',
                    b't' => b'\t',
                    b'r' => b'\r',
                    b'b' => 0x08,
                    b'f' => 0x0c,
                    b'"' => b'"',
                    b'\\' => b'\\',
                    b'/' => b'/',
                    b'u' => match self.take_escape() {
                        // A code point becomes UTF-8 rather than staying as the
                        // six bytes it was written as: a JSON writer may spell a
                        // character either way, and both spellings have to
                        // compare equal.
                        Some(cp) => self.hold(cp),
                        // Not four hex digits: it was never an escape, so
                        // both bytes are the value.
                        None => self.give(b"\\u"),
                    },
                    // Not an escape this dialect defines: both bytes are content.
                    other => self.give(&[b'\\', other]),
                })
            }
        }
    }
}

impl LogicalBytes<'_> {
    /// One byte of a nested JSON value's text, `b` being the next as written.
    ///
    /// Two writers of the same records space them differently -- `jq .`
    /// indents, `jq -c` and most NDJSON writers leave no space at all -- and
    /// spell a string's characters differently: `\u00e9` or `é`, `\/` or `/`.
    /// RFC 8259 makes the space between tokens insignificant, and a character
    /// is the same character however it is escaped, so neither is a change.
    /// Read as written, the same records pretty and compact differed in every
    /// nested cell. What is left is the order of an object's keys, a repeated
    /// key and the spelling of a number, which stay as written: compara reads
    /// a top-level number as its text too, and a value read in key order has
    /// one hash that does not need the members sorted.
    ///
    /// Outside a string, space is dropped, except one where it parts two runs
    /// of number or literal bytes: `[1 2]` is not JSON, and it is not `[12]`.
    /// Inside one, an escape is undone and the character given in one
    /// spelling: UTF-8, or for the quote, the backslash and the control
    /// characters (which a string cannot hold as they are) a short escape
    /// where JSON has one and `\u00xx` where not. So a quote never stands in a
    /// string as itself, and the text keeps its tokens.
    fn next_nested(&mut self, mut b: u8) -> Option<u8> {
        if self.closed {
            match b {
                b'"' => {
                    self.closed = false;
                    self.last = b;
                }
                b'\\' => return Some(self.nested_escape()),
                _ => {}
            }
            return Some(b);
        }
        if is_space(b) {
            while self.raw.get(self.at).is_some_and(|&c| is_space(c)) {
                self.at += 1;
            }
            let next = *self.raw.get(self.at)?;
            if in_scalar(self.last) && in_scalar(next) {
                self.last = b' ';
                return Some(b' ');
            }
            self.at += 1;
            b = next;
        }
        self.closed = b == b'"';
        self.last = b;
        Some(b)
    }

    /// A nested string's escape, its backslash just read: the character it
    /// stands for, in the one spelling [`Self::next_nested`] gives it.
    fn nested_escape(&mut self) -> u8 {
        let Some(&e) = self.raw.get(self.at) else {
            return b'\\';
        };
        self.at += 1;
        let cp = match e {
            b'"' | b'\\' | b'b' | b'f' | b'n' | b'r' | b't' => return self.give(&[b'\\', e]),
            b'/' => return b'/',
            b'u' => match self.take_escape() {
                Some(cp) => cp,
                None => return self.give(b"\\u"),
            },
            other => return self.give(&[b'\\', other]),
        };
        let short = match cp {
            0x22 => b'"',
            0x5C => b'\\',
            0x08 => b'b',
            0x0C => b'f',
            0x0A => b'n',
            0x0D => b'r',
            0x09 => b't',
            0..=0x1F => {
                const HEX: &[u8; 16] = b"0123456789abcdef";
                let (hi, lo) = (HEX[(cp >> 4) as usize], HEX[(cp & 0xF) as usize]);
                return self.give(&[b'\\', b'u', b'0', b'0', hi, lo]);
            }
            _ => return self.hold(cp),
        };
        self.give(&[b'\\', short])
    }
}

/// JSON's insignificant space.
fn is_space(b: u8) -> bool {
    matches!(b, b' ' | b'\t' | b'\n' | b'\r')
}

/// Whether `b` belongs to a number or a literal: not space, punctuation or a
/// quote.
fn in_scalar(b: u8) -> bool {
    !is_space(b) && !matches!(b, b'{' | b'}' | b'[' | b']' | b',' | b':' | b'"')
}

impl LogicalBytes<'_> {
    /// The code point of the `\u` escape whose digits start at `self.at`, with a
    /// following low surrogate folded in. Leaves `at` past what it consumed.
    fn take_escape(&mut self) -> Option<u32> {
        let hi = self.take_hex4()?;
        if !(0xD800..=0xDBFF).contains(&hi) {
            return Some(hi);
        }
        // A surrogate pair is one code point written as two escapes.
        if self.raw.get(self.at) == Some(&b'\\') && self.raw.get(self.at + 1) == Some(&b'u') {
            let save = self.at;
            self.at += 2;
            match self.take_hex4() {
                Some(lo) if (0xDC00..=0xDFFF).contains(&lo) => {
                    return Some(0x10000 + ((hi - 0xD800) << 10) + (lo - 0xDC00));
                }
                _ => self.at = save,
            }
        }
        Some(hi)
    }

    fn take_hex4(&mut self) -> Option<u32> {
        let digits = self.raw.get(self.at..self.at + 4)?;
        let mut cp = 0u32;
        for &d in digits {
            cp = cp * 16 + (d as char).to_digit(16)?;
        }
        self.at += 4;
        Some(cp)
    }
}

/// Whether two fields hold the same logical bytes, without decoding either.
pub(super) fn same_bytes(a: &Slab, x: Field, b: &Slab, y: Field) -> bool {
    // Ask the two field words directly rather than building two iterators to ask
    // them. `logical()` already reports a field with no escape as plain, so the
    // memcmp below was always the path a CSV file took -- but reaching it cost
    // two `LogicalBytes` values, each slicing the slab and carrying a four-byte
    // pending buffer, to answer a question two bit tests answer. For a
    // nine-byte field that setup was most of the comparison.
    if !is_escaped(x) && !is_escaped(y) {
        return a.raw(x) == b.raw(y);
    }
    same_escaped(a, x, b, y)
}

/// [`same_bytes`] where a side carries an escape. Out of line so that the
/// plain path, nearly every cell of a CSV file, does not save and restore the
/// registers this one needs: inlined, they cost every call 16 instructions,
/// 0.85% of a turbo compare of the perf gate's 200k-row CSV pair.
#[inline(never)]
fn same_escaped(a: &Slab, x: Field, b: &Slab, y: Field) -> bool {
    // The same bytes read the same way are the same value, and two nested
    // values a writer spelled alike, as one writer does, are equal without
    // reading them a byte at a time.
    if a.mode(x) == b.mode(y) && a.raw(x) == b.raw(y) {
        return true;
    }
    a.logical(x).eq(b.logical(y))
}

/// The field decoded. Only the report sections and the normalising paths call
/// this; the fast path never builds a string for a cell at all.
pub(super) fn text_of(slab: &Slab, f: Field) -> String {
    // Asked of the field word, not of a `LogicalBytes` built to answer it: a
    // plain field, every column name the key walk reads among them, is the
    // common case.
    if slab.mode(f) == Mode::Raw {
        return String::from_utf8_lossy(slab.raw(f)).into_owned();
    }
    let bytes: Vec<u8> = slab.logical(f).collect();
    String::from_utf8_lossy(&bytes).into_owned()
}

/// [`text_of`] that refuses rather than aborting when there is no room.
///
/// The report path uses this one. The comparison path keeps [`text_of`]: it
/// decodes a cell only when `--trim` or `--ignore-case` is on, throws the string
/// away immediately, and is the hottest loop in the port -- a `Result` there
/// would be paid ten million times to report a failure that cannot happen,
/// because nothing of that size is being held.
pub(super) fn text_of_checked(slab: &Slab, f: Field, what: &str) -> crate::Result<String> {
    if slab.mode(f) == Mode::Raw {
        return match std::str::from_utf8(slab.raw(f)) {
            Ok(s) => crate::alloc::string(s, what),
            // Lossy replacement allocates its own string; it is reached only by
            // input that is not UTF-8, which is not the case a budget is tight
            // for.
            Err(_) => Ok(String::from_utf8_lossy(slab.raw(f)).into_owned()),
        };
    }
    let mut bytes: Vec<u8> = crate::alloc::sized(f_len_hint(f), what)?;
    bytes.extend(slab.logical(f));
    match String::from_utf8(bytes) {
        Ok(s) => Ok(s),
        Err(e) => Ok(String::from_utf8_lossy(e.as_bytes()).into_owned()),
    }
}

/// An upper bound on a field's decoded length: decoding only ever removes
/// bytes (a doubled quote becomes one), so the raw length is enough.
fn f_len_hint(f: Field) -> usize {
    super::field::len_of(f)
}

#[cfg(test)]
mod tests {
    use super::super::field::pack;
    use super::*;

    fn bytes(text: &[u8], dialect: Dialect) -> Vec<u8> {
        let slab = Slab::owned(text.to_vec(), dialect);
        slab.logical(pack(0, text.len() as u64, true)).collect()
    }

    fn decode(text: &[u8], dialect: Dialect) -> String {
        String::from_utf8(bytes(text, dialect)).expect("valid utf-8")
    }

    #[test]
    fn csv_drops_the_second_quote_of_a_pair() {
        assert_eq!(decode(b"a\"\"b", Dialect::Csv), "a\"b");
    }

    #[test]
    fn json_undoes_its_escapes() {
        assert_eq!(decode(br#"a\nb\"c\\d"#, Dialect::Json), "a\nb\"c\\d");
    }

    #[test]
    fn a_unicode_escape_and_the_character_itself_are_the_same_value() {
        // A writer may spell a character either way, so both spellings have to
        // decode to the same bytes.
        assert_eq!(decode(br"caf\u00e9", Dialect::Json), "caf\u{e9}");
        assert_eq!(decode(br"\ud83d\ude00", Dialect::Json), "\u{1f600}");
        // Not four hex digits, so it was never an escape.
        assert_eq!(decode(br"\uZZ", Dialect::Json), r"\uZZ");
        // A lone high surrogate has no pair to fold in and must not eat what
        // follows it. It is encoded as written, which is what the other ports
        // do; substituting a replacement character here would make the same
        // file hash differently in two ports.
        assert_eq!(bytes(br"\ud83dx", Dialect::Json), b"\xed\xa0\xbdx");
    }

    /// A JSON slab holding `{"v":<value>}`, and the field the reader makes of
    /// the value: a nested one flagged as escaped, as `text::parse_json` does.
    fn member(value: &str) -> (Slab, Field) {
        let text = format!("{{\"v\":{value}}}");
        let slab = Slab::owned(text.into_bytes(), Dialect::Json);
        (slab, pack(5, value.len() as u64, true))
    }

    fn nested(value: &str) -> String {
        let (slab, f) = member(value);
        String::from_utf8(slab.logical(f).collect()).expect("valid utf-8")
    }

    #[test]
    fn a_nested_value_is_its_text_without_the_space_between_tokens() {
        assert_eq!(
            nested("{\n  \"a\" : [ 1 ,\t2 ],\r\n  \"b\": {}\n}"),
            r#"{"a":[1,2],"b":{}}"#
        );
        // Space inside a string is the string's.
        assert_eq!(nested(r#"[ "a b" , " " ]"#), r#"["a b"," "]"#);
        // A bracket or an escaped quote inside a string is text.
        assert_eq!(nested(r#"[ "a\"] , [b" ]"#), r#"["a\"] , [b"]"#);
        // Two runs of number or literal bytes keep one space between them:
        // `[1 2]` is not JSON, and it is not `[12]` either.
        assert_eq!(nested("[1  2, true\n false]"), "[1 2,true false]");
        assert_eq!(nested("[1, 2]"), "[1,2]");
    }

    #[test]
    fn a_nested_string_is_spelled_one_way() {
        // UTF-8 for a character escaped, `/` as itself, and the one escape
        // JSON has for a quote, a backslash or a control character, whatever
        // the writer used.
        assert_eq!(
            nested(r#"["caf\u00e9", "caf\u00E9", "a\/b", "\u0041", "\ud83d\ude00"]"#),
            "[\"caf\u{e9}\",\"caf\u{e9}\",\"a/b\",\"A\",\"\u{1f600}\"]"
        );
        assert_eq!(
            nested(r#"["\u0022", "\"", "\u005C", "\\", "\u000a", "\n", "\u0009"]"#),
            r#"["\"","\"","\\","\\","\n","\n","\t"]"#
        );
        assert_eq!(
            nested(r#"["\u001F", "\u0001", "\u007f"]"#),
            "[\"\\u001f\",\"\\u0001\",\"\u{7f}\"]"
        );
        // Not an escape JSON defines, or not four hex digits: as written.
        assert_eq!(nested(r#"["\x", "\uZZ"]"#), r#"["\x","\uZZ"]"#);
    }

    #[test]
    fn a_nested_value_and_its_compact_spelling_are_the_same_bytes() {
        let (a, x) = member("{\n  \"k\": [1, \"caf\\u00e9\"]\n}");
        let (b, y) = member("{\"k\":[1,\"caf\u{e9}\"]}");
        assert!(same_bytes(&a, x, &b, y));
        assert_eq!(text_of(&a, x), text_of(&b, y));
        // Key order and a number's spelling are as written.
        let (c, z) = member("{\"k\":[1.0,\"caf\u{e9}\"]}");
        assert!(!same_bytes(&b, y, &c, z));
    }

    #[test]
    fn a_string_that_holds_json_is_a_string() {
        // `{"s":"{\"a\": 1}"}`: the string's field starts past its quote, so it
        // is decoded as a string, its space kept.
        let text = br#"{"s":"{\"a\": 1}"}"#;
        let slab = Slab::owned(text.to_vec(), Dialect::Json);
        let f = pack(6, (text.len() - 8) as u64, true);
        assert_eq!(text_of(&slab, f), r#"{"a": 1}"#);
    }

    /// What this process has resident of its mappings of `path`, in KB, from
    /// /proc/self/smaps.
    #[cfg(target_os = "linux")]
    fn mapped_kb(path: &Path) -> u64 {
        let name = path.to_str().expect("a UTF-8 temp path");
        let smaps = std::fs::read_to_string("/proc/self/smaps").expect("smaps");
        let mut ours = false;
        let mut kb = 0;
        for line in smaps.lines() {
            let first = line.split_whitespace().next().unwrap_or("");
            if first.contains('-') && first.chars().all(|c| c.is_ascii_hexdigit() || c == '-') {
                ours = line.ends_with(name);
            } else if ours && let Some(rest) = line.strip_prefix("Rss:") {
                kb += rest
                    .trim()
                    .trim_end_matches("kB")
                    .trim()
                    .parse::<u64>()
                    .unwrap_or(0);
            }
        }
        kb
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn a_released_range_leaves_the_process_and_reads_back_the_same() {
        let path = std::env::temp_dir().join(format!("compara-release-{}", std::process::id()));
        let text: Vec<u8> = (0..4u32 << 20).map(|i| b'a' + (i % 26) as u8).collect();
        std::fs::write(&path, &text).expect("a temp file");
        let mut slab = Slab::map(&path).expect("the mapping");
        assert_eq!(slab.data(), &text[..]);
        assert!(
            mapped_kb(&path) >= 4 << 10,
            "read whole, the file is resident"
        );
        assert_eq!(
            slab.release(100, 3 << 20),
            3 << 20,
            "not asked to, nothing released"
        );
        assert!(mapped_kb(&path) >= 4 << 10);
        slab.release_as_read();

        // Whole pages only: the part page at each end stays.
        assert_eq!(slab.release(100, (3 << 20) + 100), 3 << 20);
        assert_eq!(slab.release(5, 50), 5, "no whole page, nothing released");
        let left = mapped_kb(&path);
        assert!(
            left <= (1 << 10) + 8,
            "{left} KB still mapped after releasing 3 MB"
        );

        // Read again, the same bytes, faulted back in.
        assert_eq!(slab.data(), &text[..]);
        slab.release_all();
        assert!(mapped_kb(&path) <= 8);
        assert_eq!(slab.data(), &text[..]);
        drop(slab);
        let _ = std::fs::remove_file(&path);
    }
}
