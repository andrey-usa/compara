//! The two text readers: CSV, and newline-delimited JSON.
//!
//! They are one module because they are one design. A JSON value is a contiguous
//! run of bytes in the file exactly as a CSV field is, so a field stays an offset
//! and a length for both and nothing downstream has to know which format it came
//! from. What differs is how a row is walked — by column number, or by key — and
//! how a value is escaped, which [`super::slab`] settles.
//!
//! The two sides of a comparison need not agree: a CSV export compares against
//! the JSON the same pipeline emits, and the key order on each side is free to
//! differ, because the JSON reader joins by name.

#[cfg(target_arch = "x86_64")]
use super::field::Wide512;
use super::field::{
    ABSENT, Delims, Field, Scan, Swar, Wide, next_of1, next_of1_with, next_of2, next_of2_with,
    next_of3_with, pack, skip_quoted, skip_quoted_with,
};
use super::slab::{Dialect, Slab, json_string, text_of};
use crate::error::{Error, Result};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Newline-delimited JSON if the first thing that is not whitespace is a brace
/// or a bracket: a top-level JSON array is the same rows, wrapped.
///
/// A CSV header can begin with anything else, and a `{` or `[` in the first
/// column of a CSV header is not something this project has ever had to read.
pub(super) fn sniff_dialect(data: &[u8]) -> Dialect {
    let data = data.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(data);
    for &b in data.iter().take(64) {
        if json_space(b) {
            continue;
        }
        return if b == b'{' || b == b'[' {
            Dialect::Json
        } else {
            Dialect::Csv
        };
    }
    Dialect::Csv
}

fn json_space(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\r' || c == b'\n'
}

/// Whether the file's rows are framed by their braces rather than by lines:
/// a top-level `[` (the rows sit inside a JSON array, and row boundaries
/// answer to its brackets and separators), or objects one after another that
/// do not each keep to a line of their own -- `jq .` writes them so, one
/// pretty-printed object after the next. Newline-delimited JSON closes its
/// first object before that line ends and puts nothing after it there; read
/// a line at a time, a stream of pretty objects was a row for every line, and
/// two such files that differed compared as identical.
///
/// Decided by the first object, which is walked once: its own bytes, a row's
/// worth. A later object that leaves its line in a file read line by line is
/// refused by the key walk ([`json_later_keys`]).
pub(super) fn json_framed(data: &[u8]) -> bool {
    let data = data.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(data);
    let Some(first) = data.iter().position(|&b| !json_space(b)) else {
        return false;
    };
    match data[first] {
        b'[' => true,
        b'{' => {
            let end = data.len();
            let close = skip_json_nested(Swar, data, first, end);
            let line_end = next_of1(data, first, end, b'\n');
            line_end < close
                || data[close..line_end.min(end)]
                    .iter()
                    .any(|&b| !json_space(b))
        }
        _ => false,
    }
}

/// Guesses the delimiter from the header line, defaulting to a comma.
pub(super) fn detect_delimiter(header: &[u8]) -> u8 {
    let mut best = b',';
    let mut best_count = -1i64;
    for c in *b",;\t|" {
        let n = header.iter().filter(|&&b| b == c).count() as i64;
        if n > best_count {
            best = c;
            best_count = n;
        }
    }
    best
}

/// A file that can't be delimited text, said as what it is: empty, UTF-16
/// or binary data. Read as CSV, its first bytes
/// would become one strange column name and the error would blame the key.
/// `head` is the file's first bytes (up to 1 KiB).
pub(crate) fn refuse_non_text(head: &[u8], path: &Path) -> Result<()> {
    let path = path.display();
    if head.is_empty() {
        return Err(Error::input(format!(
            "{path} is empty: no header row and no data"
        )));
    }
    // A UTF-16 file's commas are two bytes each: read as UTF-8 its header is
    // noise. Its byte-order mark says what it is.
    // Without the mark, its ASCII header is every other byte zero.
    let first = &head[..head.len().min(64)];
    let zeros = first.iter().filter(|&&b| b == 0).count();
    if head.starts_with(&[0xFF, 0xFE])
        || head.starts_with(&[0xFE, 0xFF])
        || (zeros >= 4 && zeros * 3 >= first.len())
    {
        return Err(Error::input(format!(
            "{path} is UTF-16 (a UTF-16 byte-order mark, or every other byte of its header zero); \
             compara reads UTF-8: convert it (iconv -f utf-16 -t utf-8)"
        )));
    }
    // Text has tabs, line ends and the odd form feed; binary data has
    // control bytes all through it (about one in nine of random bytes).
    let control = head
        .iter()
        .filter(|&&b| (b < 0x20 && !matches!(b, b'\t' | b'\n' | b'\r' | 0x0c)) || b == 0x7f)
        .count();
    if control > (head.len() / 16).max(1) {
        return Err(Error::input(format!(
            "{path} does not look like CSV or JSON lines: its first bytes are binary data \
             (compara reads CSV, JSON lines and Parquet)"
        )));
    }
    Ok(())
}

/// The CSV header row's names, and where the first data row starts.
///
/// A file whose lines end in a carriage return alone is declined here, by
/// its first line: every scan in this engine ends a row at a line feed, so
/// such a file read as one long header and no rows, and a pair of them
/// compared as identical whatever they held. The `csv` crate's readers end
/// a row at either, so another engine can take it.
pub(super) fn csv_header(slab: &Slab, delimiter: u8, path: &Path) -> Result<(Vec<String>, usize)> {
    let data = slab.data();
    let end = data.len();
    refuse_non_text(&data[..end.min(1024)], path)?;
    let mut names = Vec::new();
    let mut pos = 0usize;
    loop {
        let (field, next, unquoted_from) = if pos < end && data[pos] == b'"' {
            let close = skip_quoted(data, pos + 1, end);
            let next = next_of2(data, close, end, delimiter, b'\n');
            let field = quoted_cell(Swar, data, pos, close, next);
            (
                quoted_at_the_end(Swar, data, pos, end)
                    .filter(|_| next >= end)
                    .unwrap_or(field),
                next,
                close,
            )
        } else {
            let next = next_of2(data, pos, end, delimiter, b'\n');
            (plain_field(data, pos, next), next, pos)
        };
        // A carriage return outside quotes belongs to a line ending: the
        // last byte before the line feed, or before the end of the file.
        let ends_line = next >= end || data[next] == b'\n';
        if let Some(at) = data[unquoted_from..next].iter().position(|&b| b == b'\r')
            && !(ends_line && unquoted_from + at + 1 == next)
        {
            return Err(Error::declined(
                format!(
                    "{} ends its lines with a carriage return alone (the classic Mac line \
                     ending), which the turbo engine does not read",
                    path.display()
                ),
                "; --engine native reads such a CSV, or give the file line feeds \
                 (tr '\\r' '\\n' < file > copy)",
            ));
        }
        names.push(text_of(slab, field));
        if next >= end {
            return Ok((names, end));
        }
        if data[next] == b'\n' {
            return Ok((names, next + 1));
        }
        pos = next + 1;
    }
}

/// A JSON file has no header row, so the column names start with the keys of
/// the first object, in the order it lists them; [`json_later_keys`] adds the
/// keys only later objects carry. A top-level array of objects answers the
/// same way; an empty one has no object to take the names from.
pub(super) fn json_header(slab: &Slab, path: &Path) -> Result<Vec<String>> {
    let data = slab.data();
    let end = data.len();
    let mut names: Vec<String> = Vec::new();
    let mut pos = 0usize;
    while pos < end && json_space(data[pos]) {
        pos += 1;
    }
    // A top-level array's opening bracket wraps the rows; it is not one.
    if pos < end && data[pos] == b'[' {
        pos += 1;
        while pos < end && json_space(data[pos]) {
            pos += 1;
        }
    }
    if pos >= end || data[pos] != b'{' {
        return Err(Error::new(format!(
            "file has no JSON object to read: {}",
            path.display()
        )));
    }
    pos += 1;
    loop {
        while pos < end && json_space(data[pos]) {
            pos += 1;
        }
        if pos >= end || data[pos] == b'}' {
            break;
        }
        if data[pos] == b',' {
            pos += 1;
            continue;
        }
        if data[pos] != b'"' {
            break;
        }
        let from = pos + 1;
        let (close, escaped) = skip_json_string(Swar, data, pos, end);
        if close <= from {
            break;
        }
        pos = close;
        while pos < end && json_space(data[pos]) {
            pos += 1;
        }
        if pos >= end || data[pos] != b':' {
            break;
        }
        pos += 1;
        while pos < end && json_space(data[pos]) {
            pos += 1;
        }
        if pos >= end {
            break;
        }
        // A key is a column with its colon and a value, as it is a member to
        // the row parser: one with no colon ends the object above, and one
        // with no value (a comma or the closing brace next) is passed over.
        // A name repeated in the object is one column (its last value wins),
        // not two.
        if !matches!(data[pos], b',' | b'}') {
            let name = text_of(slab, pack(from as u64, (close - 1 - from) as u64, escaped));
            if !names.contains(&name) {
                names.push(name);
            }
        }
        pos = match data[pos] {
            b'"' => skip_json_string(Swar, data, pos, end).0,
            b'{' | b'[' => skip_json_nested(Swar, data, pos, end),
            _ => {
                let mut at = pos;
                while at < end && data[at] != b',' && data[at] != b'}' && !json_space(data[at]) {
                    at += 1;
                }
                at
            }
        };
    }
    // An empty first object names no column, and is no error yet: the
    // objects after it may (`json_later_keys`).
    Ok(names)
}

/// The most the names of the columns found past the first object may come to.
/// A table's column names are a few kilobytes; this is a file that is not one.
const LATER_KEYS_BYTES: usize = 64 << 20;

/// What a name costs held beyond its bytes: its `String`, its slot in the
/// table that finds it again (twice while the table grows), the allocator's
/// rounding of a short string, and its place in the list the names leave in.
/// 400,000 names of seven bytes took 52-60 MB of peak RSS more than none.
/// Charged with the bytes, so a file of many short names is held to the
/// budget as one of a few long ones is.
const NAME_OVERHEAD: usize = 128;

/// The memory the names found past a file's first object may take, shared by
/// every thread that walks a piece of the file: one ceiling for the file, not
/// one for each piece, so what a file may hold does not grow with the cores
/// that read it.
///
/// The ceiling is [`LATER_KEYS_BYTES`], or a quarter of the memory the run was
/// given (`--memory`, `COMPARA_MEMORY`) or may have (its heap limit), when that
/// is less: the names are read before anything else is held, but both files'
/// are kept for the rest of the run.
pub(super) struct KeyBudget {
    limit: usize,
    used: AtomicUsize,
    /// What set a `limit` below the ceiling, and what to do about it, for the
    /// refusal to say.
    from: &'static str,
    /// Shared by pieces of a file: a piece that finds it spent stops and
    /// hands back the names it has, for the file's walk to judge them all
    /// together, rather than refusing on its own count.
    pieces: bool,
}

impl KeyBudget {
    /// The budget for one file, given the memory the run was given and its
    /// heap limit.
    pub(super) fn new(stated: Option<u64>, heap: Option<u64>) -> KeyBudget {
        let mut limit = LATER_KEYS_BYTES as u64;
        let mut from = "";
        if let Some(h) = heap.map(|h| h / 4)
            && h < limit
        {
            limit = h;
            from = ", a quarter of this process's heap limit (ulimit -d, or the container's)";
        }
        if let Some(m) = stated.map(|m| m / 4)
            && m < limit
        {
            limit = m;
            from = ", a quarter of --memory (or COMPARA_MEMORY)";
        }
        KeyBudget {
            limit: limit as usize,
            used: AtomicUsize::new(0),
            from,
            pieces: false,
        }
    }

    /// The same budget, shared by the pieces of one file walked at once.
    pub(super) fn for_pieces(self) -> KeyBudget {
        KeyBudget {
            pieces: true,
            ..self
        }
    }

    /// What a name of `bytes` is charged.
    pub(super) fn cost(bytes: usize) -> usize {
        bytes.saturating_add(NAME_OVERHEAD)
    }

    /// Whether names that together cost `total` pass the ceiling.
    pub(super) fn exceeded_by(&self, total: usize) -> bool {
        total > self.limit
    }

    /// Whether the names taken so far, on every thread together, passed the
    /// ceiling.
    pub(super) fn spent(&self) -> bool {
        self.exceeded_by(self.used.load(Ordering::Relaxed))
    }

    /// Takes room for one more name of `bytes`; false once the file's names,
    /// on every thread together, would pass the ceiling.
    fn take(&self, bytes: usize) -> bool {
        let cost = KeyBudget::cost(bytes);
        let before = self.used.fetch_add(cost, Ordering::Relaxed);
        !self.exceeded_by(before.saturating_add(cost))
    }

    /// The error for a file whose names pass the ceiling.
    pub(super) fn refusal(&self) -> Error {
        Error::new(format!(
            "the objects of this JSON file hold more than {} of different keys between them{}: \
             they are not the rows of one table{}",
            mb(self.limit),
            self.from,
            if self.from.is_empty() {
                ""
            } else {
                ", or need more memory than that"
            }
        ))
    }
}

/// `n` bytes for a message: in MB, or KB below one.
fn mb(n: usize) -> String {
    if n >= 1 << 20 {
        format!("{} MB", n >> 20)
    } else {
        format!("{} KB", n >> 10)
    }
}

/// `data.starts_with(text)` for a `text` of a few bytes, as two loads of eight
/// that overlap rather than a call: it is asked once a member of every row.
#[inline(always)]
fn starts_with_short(data: &[u8], text: &[u8]) -> bool {
    let n = text.len();
    if data.len() < n {
        return false;
    }
    if !(8..=16).contains(&n) {
        return data[..n] == *text;
    }
    let word = |s: &[u8], at: usize| u64::from_le_bytes(s[at..at + 8].try_into().unwrap());
    word(data, 0) == word(text, 0) && word(data, n - 8) == word(text, n - 8)
}

/// The keys that objects in `data[lo..hi]` carry and `known` does not hold,
/// in the order they are first met, looking at no more than `max_rows` rows.
/// Each one found is taken from `budget`, which the other pieces of the file
/// share.
///
/// A JSON table has no header: [`json_header`] takes the first object's keys,
/// and a writer that leaves a null field out of its object (Spark does, and
/// Go's `omitempty`) leaves a column out of that object whenever its first
/// row has no value there. Taking the first object's word alone, a column
/// absent from the first row was no column at all, and two files that
/// differed only in it compared as identical. So every object is asked for
/// its keys, and the columns are all of them.
///
/// The walk is the row parser's without its fields: a key is skipped as a
/// string, compared with the name the first object had at that place (the
/// common case, and then nothing more is asked of it), and its value is
/// skipped whatever it is. `lo` must be the start of a row.
///
/// It is also the one walk over every row before the comparison, so it says
/// where a file is not the JSON it is read as: a row that is not an object,
/// and, read a line at a time, an object that runs onto the next line or has
/// more after it on its own. The row parser reads each of those as some other
/// number of rows than the file holds.
pub(super) fn json_later_keys(
    slab: &Slab,
    lo: usize,
    hi: usize,
    array: bool,
    known: &[String],
    max_rows: usize,
    budget: &KeyBudget,
) -> Result<(Vec<String>, usize)> {
    let data = slab.data();
    let scan = Swar;
    let end = hi.min(data.len());
    // Each of the first object's members as a compact writer spells it,
    // `"name":`, so the common row is asked one comparison a member. A name
    // that needs an escape has no such spelling and takes the walk below.
    let spelled: Vec<Vec<u8>> = known
        .iter()
        .map(|name| {
            if name.bytes().any(|b| b == b'"' || b == b'\\' || b < 0x20) {
                return Vec::new();
            }
            let mut text = Vec::with_capacity(name.len() + 3);
            text.push(b'"');
            text.extend_from_slice(name.as_bytes());
            text.extend_from_slice(b"\":");
            text
        })
        .collect();
    // Names are looked up, not searched for: a file built to carry a new key
    // in every row would otherwise cost the square of its rows.
    let seen: HashSet<&str> = known.iter().map(String::as_str).collect();
    // Each new name once, with the place it was first met: a list beside a
    // set held every name twice.
    let mut later: HashMap<String, usize> = HashMap::new();
    let mut rows = 0usize;
    let mut pos = lo;
    'rows: while pos < end && rows < max_rows {
        while pos < end && json_space(data[pos]) {
            pos += 1;
        }
        if array && pos < end && data[pos] == b'[' {
            pos += 1;
            while pos < end && json_space(data[pos]) {
                pos += 1;
            }
        }
        if pos >= end {
            break;
        }
        if data[pos] != b'{' {
            return Err(not_a_row(data, pos, "is not a JSON object", array));
        }
        rows += 1;
        let row = pos;
        pos += 1;
        let mut closed = false;
        // A line feed met between the object's tokens: newline-delimited
        // JSON has none, and outside its strings that is the only place
        // one can be (a scalar ends at one, a nested value is searched).
        let mut lines = false;
        let mut member = 0usize; // the place of this key in the first object
        loop {
            // Compact ndjson separates members with a bare `,"`, which goes
            // straight to the next key, as it does in the row parser.
            if pos + 1 < end && data[pos] == b',' && data[pos + 1] == b'"' {
                pos += 1;
            } else {
                while pos < end {
                    match data[pos] {
                        b',' | b' ' | b'\t' | b'\r' => {}
                        b'\n' => lines = true,
                        _ => break,
                    }
                    pos += 1;
                }
            }
            if pos >= end {
                break;
            }
            if data[pos] == b'}' {
                pos += 1;
                closed = true;
                break;
            }
            if data[pos] != b'"' {
                break; // malformed: the comparison stops reading the object here too
            }
            if let Some(text) = spelled.get(member)
                && !text.is_empty()
                && pos + text.len() < end
                && starts_with_short(&data[pos..end], text)
                && !json_space(data[pos + text.len()])
            {
                member += 1;
                pos = json_value_end_lines(scan, data, pos + text.len(), end, &mut lines);
                continue;
            }
            let key_from = pos + 1;
            let (key_end, escaped) = skip_json_string(scan, data, pos, end);
            if key_end > end || key_end <= key_from {
                break;
            }
            let key = &data[key_from..key_end - 1];
            pos = key_end;
            while pos < end {
                match data[pos] {
                    b' ' | b'\t' | b'\r' => {}
                    b'\n' => lines = true,
                    _ => break,
                }
                pos += 1;
            }
            if pos >= end || data[pos] != b':' {
                break;
            }
            pos += 1;
            while pos < end {
                match data[pos] {
                    b' ' | b'\t' | b'\r' => {}
                    b'\n' => lines = true,
                    _ => break,
                }
                pos += 1;
            }
            if pos >= end {
                break;
            }
            // Only now is it a member, as it is to the row parser: a key
            // with no colon after it ends the object there, and one with no
            // value (a comma or the closing brace next), which the parser
            // reads as empty, is passed over. Neither names a column.
            if !matches!(data[pos], b',' | b'}')
                && known.get(member).map(String::as_bytes) != Some(key)
            {
                let name = text_of(
                    slab,
                    pack(key_from as u64, (key_end - 1 - key_from) as u64, escaped),
                );
                if !seen.contains(name.as_str()) && !later.contains_key(&name) {
                    // The names are the one thing here that grows with a
                    // hostile file, so they have a ceiling of their own.
                    let room = budget.take(name.len());
                    if !room && !budget.pieces {
                        return Err(budget.refusal());
                    }
                    let at = later.len();
                    later.insert(name, at);
                    // A piece keeps the name that did not fit, so the names
                    // the pieces hand back, counted once each, pass the
                    // ceiling where the file's own do.
                    if !room {
                        break 'rows;
                    }
                }
            }
            member += 1;
            pos = json_value_end_lines(scan, data, pos, end, &mut lines);
        }
        if !array && closed {
            // A line is a row: the object closes on the line it opens, with
            // nothing after it there but space.
            if lines {
                return Err(not_a_row(data, row, "runs onto the next line", false));
            }
            let line_end = next_of1_with(scan, data, pos, end, b'\n');
            if let Some(at) = data[pos..line_end].iter().position(|&b| !json_space(b)) {
                return Err(not_a_row(
                    data,
                    pos + at,
                    "follows an object on its line",
                    false,
                ));
            }
            pos = line_end.saturating_add(1);
            continue;
        }
        let next = end_of_json_row(scan, data, pos.min(end), end, array, false);
        pos = if next > pos || data.get(next) == Some(&b'{') {
            next
        } else {
            pos + 1
        };
    }
    Ok((in_order(later), rows))
}

/// [`json_value_end`], noting in `lines` a line feed inside a nested value.
/// Only a nested value is searched: a string cannot hold one, and a number or
/// a literal ends at one, which the walk then steps over as space.
#[inline(always)]
fn json_value_end_lines<S: Scan>(
    scan: S,
    d: &[u8],
    mut at: usize,
    end: usize,
    lines: &mut bool,
) -> usize {
    if at >= end {
        return end;
    }
    match d[at] {
        b'"' => skip_json_string(scan, d, at, end).0,
        b'{' | b'[' => {
            let to = skip_json_nested(scan, d, at, end);
            *lines |= next_of1_with(scan, d, at, to, b'\n') < to;
            to
        }
        _ => {
            while at < end && !matches!(d[at], b',' | b'}' | b']' | b'\n') && !json_gap(d[at]) {
                at += 1;
            }
            at
        }
    }
}

/// A row the reader cannot take as one, said with its line and what to do.
/// Only on the way out: the line is counted from the start of the file.
#[cold]
#[inline(never)]
fn not_a_row(data: &[u8], at: usize, what: &str, framed: bool) -> Error {
    let line = 1 + data[..at].iter().filter(|&&b| b == b'\n').count();
    let shown: String = String::from_utf8_lossy(&data[at..data.len().min(at + 32)])
        .chars()
        .take_while(|&c| c != '\n' && c != '\r')
        .collect();
    let how = if framed {
        "every row of a JSON array or stream of objects is an object"
    } else {
        "the first object is on one line of its own, so the file is read as newline-delimited \
         JSON, one object a line; rewrite it so with jq -c . FILE"
    };
    Error::input(format!("line {line} ({shown:?}) {what}: {how}"))
}

/// The names of a map from name to the place it was first met, in that order.
pub(super) fn in_order(names: HashMap<String, usize>) -> Vec<String> {
    let mut placed: Vec<(usize, String)> = names.into_iter().map(|(n, at)| (at, n)).collect();
    placed.sort_unstable_by_key(|(at, _)| *at);
    placed.into_iter().map(|(_, n)| n).collect()
}

/// Skips one JSON string starting at its opening quote, returning the offset one
/// past the closing quote and whether the string holds a backslash.
// Inlined: the JSON parse calls it for every key and every string value --
// a hundred million calls on the 2M ndjson pair, each a few dozen
// instructions of which the call was a fair share.
#[inline(always)]
fn skip_json_string<S: Scan>(scan: S, data: &[u8], mut at: usize, end: usize) -> (usize, bool) {
    at += 1; // the opening quote
    let mut escaped = false;
    loop {
        let stop = next_of2_with(scan, data, at, end, b'"', b'\\');
        if stop >= end {
            return (end, escaped);
        }
        if data[stop] == b'"' {
            return (stop + 1, escaped);
        }
        escaped = true;
        at = stop + 2; // the backslash and whatever it escapes
        if at > end {
            return (end, escaped);
        }
    }
}

/// The column of a framed file's first object: how far its `{` sits from the
/// start of its line, when nothing but spaces precede it there. A
/// pretty-printer indents every row of an array alike and anything nested in
/// a row further, so a line opening an object at this column is most likely a
/// row; a stream of objects starts each at the line's start. `None` when the
/// rows are not laid out one per line (`[{` on one line).
pub(super) fn json_row_indent(data: &[u8]) -> Option<usize> {
    let mut pos = usize::from(data.starts_with(b"\xEF\xBB\xBF")) * 3;
    while pos < data.len() && json_space(data[pos]) {
        pos += 1;
    }
    if pos < data.len() && data[pos] == b'[' {
        pos += 1;
        while pos < data.len() && json_space(data[pos]) {
            pos += 1;
        }
    }
    if pos >= data.len() || data[pos] != b'{' {
        return None;
    }
    let line = data[..pos]
        .iter()
        .rposition(|&b| b == b'\n')
        .map_or(0, |at| at + 1);
    data[line..pos]
        .iter()
        .all(|&b| b == b' ' || b == b'\t')
        .then_some(pos - line)
}

/// Where the rows of a top-level JSON array start, one for each of `splits`:
/// the first object at or past the split that opens at the array's own depth,
/// or nothing when no row starts past it. Increasing and distinct.
///
/// A pretty-printed array puts nested objects on lines of their own too, so the
/// line a split lands on cannot tell a row from an object inside one; only the
/// depth can, and only a walk from the start knows the depth. Braces alone
/// keep it, as in [`end_of_array_row`]: an array nested in a row sits inside the
/// row's braces. One pass on one thread, as far as the last split.
pub(super) fn json_array_row_starts(data: &[u8], from: usize, splits: &[usize]) -> Vec<usize> {
    let end = data.len();
    let mut starts: Vec<usize> = Vec::with_capacity(splits.len());
    let mut next = 0usize;
    let mut depth = 0usize;
    let mut at = from;
    while next < splits.len() && at < end {
        let b = next_of3_with(Swar, data, at, end, b'"', b'{', b'}');
        if b >= end {
            break;
        }
        match data[b] {
            b'"' => {
                at = skip_json_string(Swar, data, b, end).0;
                continue;
            }
            b'{' => {
                if depth == 0 && b >= splits[next] {
                    if starts.last().is_none_or(|&s| b > s) {
                        starts.push(b);
                    }
                    while next < splits.len() && splits[next] <= b {
                        next += 1;
                    }
                }
                depth += 1;
            }
            // A stray brace in a malformed file does not take the depth below
            // the array's.
            _ => depth = depth.saturating_sub(1),
        }
        at = b + 1;
    }
    starts
}

/// Past a nested object or array: the byte after its closing bracket, or `end`
/// where it does not close.
fn skip_json_nested<S: Scan>(scan: S, data: &[u8], mut pos: usize, end: usize) -> usize {
    let mut depth = 0i32;
    while pos < end {
        match data[pos] {
            b'"' => {
                pos = skip_json_string(scan, data, pos, end).0;
                continue;
            }
            b'{' | b'[' => depth += 1,
            b'}' | b']' => {
                depth -= 1;
                pos += 1;
                if depth <= 0 {
                    return pos;
                }
                continue;
            }
            _ => {}
        }
        pos += 1;
    }
    end
}

/// [`guard_span_with`] a register at a time: the delimiters in each register
/// are counted rather than visited, and the one wanted is picked out of the
/// last register's mask. On a twenty-column row that is six registers where the
/// field walk took nineteen steps of a few dozen instructions each, and the
/// walk ran once for every row of A the join proves -- a quarter of the whole
/// comparison's instructions.
///
/// Quoted fields are followed by quote parity: a delimiter or newline between
/// an opening quote and its closing one does not count, and a doubled quote is
/// a close and an open back to back. That is the walk's reading only while
/// every quote that opens is where the walk would open a field: at the row's
/// start, after a delimiter, or straight after a closing quote (the second half
/// of a doubled one). Any other -- a quote inside an unquoted field, which the
/// walk keeps as content -- before the answer hands the row to the walk.
///
/// `None` hands the row to the field walk: such a quote, or too few bytes left
/// for a register. The answer is otherwise the walk's: the `commas`-th
/// delimiter, or the newline where the last field ends the row, and
/// `Some(None)` where the row ends early or runs past `hi`.
#[inline(always)]
fn guard_span_counted<S: Scan>(
    scan: S,
    data: &[u8],
    lo: usize,
    hi: usize,
    delimiter: u8,
    commas: usize,
) -> Option<Option<usize>> {
    if !S::WIDE || commas == 0 {
        return None;
    }
    let mut need = commas;
    let mut at = lo;
    // Up to the first quote every delimiter counts, and most rows never meet
    // one: they pay one compare for stops and one for delimiters a register.
    loop {
        if at >= hi {
            return Some(None);
        }
        if at + S::STEP > data.len() {
            return None;
        }
        let room = hi - at;
        let valid = if room >= S::STEP {
            u64::MAX >> (64 - S::STEP)
        } else {
            (1u64 << room) - 1
        };
        let delims = scan.hits(data, at, delimiter, delimiter) & valid;
        let stops = scan.hits(data, at, b'\n', b'"') & valid;
        let before = if stops == 0 {
            u64::MAX
        } else {
            (1u64 << stops.trailing_zeros()) - 1
        };
        let counted = delims & before;
        let seen = counted.count_ones() as usize;
        if seen >= need {
            return Some(Some(at + nth_bit(counted, need) - lo));
        }
        if stops != 0 {
            let stop = at + stops.trailing_zeros() as usize;
            if data[stop] == b'\n' {
                // It ends the last field, or the row was too short.
                return Some((need - seen == 1).then_some(stop - lo));
            }
            break;
        }
        need -= seen;
        at += S::STEP;
    }
    guard_span_quoted(scan, data, lo, hi, delimiter, need, at)
}

/// [`guard_span_counted`] from the register holding the row's first quote,
/// with `need` delimiters still to find: the same count by quote parity.
#[inline(always)]
fn guard_span_quoted<S: Scan>(
    scan: S,
    data: &[u8],
    lo: usize,
    hi: usize,
    delimiter: u8,
    mut need: usize,
    mut at: usize,
) -> Option<Option<usize>> {
    let top: usize = S::STEP - 1;
    let mut inside = 0u64; // all ones while a quoted field carries over
    // Whether the byte before the register lets a quote open a field: the
    // row's start, a delimiter, or a quote -- one that closed, as an opening
    // one leaves the register's first byte inside, where a quote only closes.
    let mut opens_field = u64::from(at == lo || data[at - 1] == delimiter || data[at - 1] == b'"');
    while at < hi {
        if at + S::STEP > data.len() {
            return None;
        }
        let room = hi - at;
        let valid = if room >= S::STEP {
            u64::MAX >> (64 - S::STEP)
        } else {
            (1u64 << room) - 1
        };
        let delims = scan.hits(data, at, delimiter, delimiter) & valid;
        let quotes = scan.hits(data, at, b'"', b'"') & valid;
        let newlines = scan.hits(data, at, b'\n', b'\n') & valid;
        // Bit i set: byte i is inside quotes once it is read. A quote that
        // leaves the bit set opened, one that clears it closed.
        let quoted = prefix_xor(quotes) ^ inside;
        let closing = quotes & !quoted;
        let opening = quotes & quoted;
        let stray = opening & !(((delims | closing) << 1) | opens_field);
        let free_newline = newlines & !quoted;
        let before = if free_newline == 0 {
            u64::MAX
        } else {
            (1u64 << free_newline.trailing_zeros()) - 1
        };
        let counted = delims & !quoted & before;
        let seen = counted.count_ones() as usize;
        let (answer, found) = if seen >= need {
            let bit = nth_bit(counted, need);
            (bit, Some(at + bit - lo))
        } else if free_newline != 0 {
            // The newline: it ends the last field, or the row was too short.
            let bit = free_newline.trailing_zeros() as usize;
            (bit, (need - seen == 1).then_some(at + bit - lo))
        } else {
            (S::STEP, None)
        };
        if stray & 1u64.checked_shl(answer as u32).map_or(u64::MAX, |b| b - 1) != 0 {
            return None;
        }
        if answer < S::STEP {
            return Some(found);
        }
        need -= seen;
        opens_field = ((delims | closing) >> top) & 1;
        if quotes.count_ones() & 1 == 1 {
            inside = !inside;
        }
        at += S::STEP;
    }
    Some(None)
}

/// The offset of the `n`-th set bit of `mask`, counting from one; `mask` has at
/// least `n`.
#[inline(always)]
fn nth_bit(mut mask: u64, n: usize) -> usize {
    for _ in 1..n {
        mask &= mask - 1;
    }
    mask.trailing_zeros() as usize
}

/// A quoted CSV field, flagged when it holds a doubled quote. A quote inside the
/// body can only be half of such a pair, which is what makes the test a single
/// scan and lets the unescaping wait until the bytes are actually read.
#[inline(always)]
fn quoted_field<S: Scan>(scan: S, data: &[u8], from: usize, to: usize) -> Field {
    let escaped = next_of1_with(scan, data, from, to, b'"') < to;
    pack(from as u64, (to - from) as u64, escaped)
}

/// The quoted field that opens at `pos`, its closing quote ending at `close`
/// and its delimiter or newline at `next`. One the data ends in is
/// [`quoted_at_the_end`]'s.
#[inline(always)]
fn quoted_cell<S: Scan>(scan: S, data: &[u8], pos: usize, close: usize, next: usize) -> Field {
    // Nothing between the two, or a line ending's carriage return: the field
    // is its body.
    if next != close && (next - close > 1 || data[close] != b'\r') {
        return quoted_with_tail(data, pos, next);
    }
    quoted_field(scan, data, pos + 1, close.saturating_sub(1).max(pos + 1))
}

/// The field at `pos` when it is a quoted one the data ends in, closed by its
/// last byte or never closed; `None` for any other. Never closed, everything
/// after the opening quote is the value, the last byte and a final newline
/// included, as the `csv` crate's readers take it: the last byte was taken
/// for the closing quote and dropped. A run of quotes at the end closes the
/// field when it is odd: the pairs before the last are doubled quotes.
#[cold]
fn quoted_at_the_end<S: Scan>(scan: S, data: &[u8], pos: usize, end: usize) -> Option<Field> {
    if pos >= end || data[pos] != b'"' {
        return None;
    }
    let body = pos + 1;
    if skip_quoted_with(scan, data, body, end) < end {
        return None;
    }
    let quotes = data[body..end]
        .iter()
        .rev()
        .take_while(|&&b| b == b'"')
        .count();
    let stop = if quotes % 2 == 1 { end - 1 } else { end };
    Some(quoted_field(scan, data, body, stop))
}

/// Stores the field at `pos` again where it is [`quoted_at_the_end`]'s to
/// read, in the slots of its column. Out of line: it runs once a file.
#[cold]
#[inline(never)]
fn store_at_the_end<S: Scan>(
    scan: S,
    data: &[u8],
    pos: usize,
    end: usize,
    slots: &[u16],
    out: &mut [Field],
) {
    if let Some(field) = quoted_at_the_end(scan, data, pos, end) {
        for slot in slots {
            out[*slot as usize] = field;
        }
    }
}

/// A quoted field with text between its closing quote and its delimiter:
/// `"ab"cd`, or the `"John "Johnny" Smith"` of an export that did not double
/// its quotes. The text is part of the value, quotes and all, as the `csv`
/// crate's readers take it (`abcd`, `John Johnny" Smith"`), so every engine
/// gives the same cell. The field is everything after the opening quote, read
/// through the decoder, which drops the closing quote where it meets it.
/// Read as the body alone, two files that differed only past the closing
/// quote compared as identical.
#[cold]
fn quoted_with_tail(data: &[u8], pos: usize, next: usize) -> Field {
    let mut stop = next;
    if data[stop - 1] == b'\r' {
        stop -= 1;
    }
    pack((pos + 1) as u64, (stop - (pos + 1)) as u64, true)
}

/// An unquoted field, with a trailing carriage return stripped so CRLF behaves
/// like LF.
fn plain_field(data: &[u8], from: usize, to: usize) -> Field {
    let mut stop = to;
    if stop > from && data[stop - 1] == b'\r' {
        stop -= 1;
    }
    pack(from as u64, (stop - from) as u64, false)
}

/// Each bit of `x` replaced by the parity of it and the bits below it: for a
/// mask of quotes, the bytes from an opening quote up to (not including) its
/// closing one.
#[inline(always)]
pub(super) fn prefix_xor(mut x: u64) -> u64 {
    x ^= x << 1;
    x ^= x << 2;
    x ^= x << 4;
    x ^= x << 8;
    x ^= x << 16;
    x ^= x << 32;
    x
}

/// The offset of the newline ending the row, from `pos`, the start of one of
/// its fields: where the field walk would end it. A quote opens a quoted
/// field only where a field starts; anywhere else it is content, as in
/// `5" nail`, and the newline after it ends the row.
#[inline(always)]
fn end_of_row<S: Scan>(scan: S, data: &[u8], pos: usize, end: usize, delimiter: u8) -> usize {
    if S::WIDE {
        return end_of_row_counted(scan, data, pos, end, delimiter);
    }
    end_of_row_walked(scan, data, pos, true, end, delimiter)
}

/// [`end_of_row`] a register at a time: the first newline with an even number
/// of quotes before it. That is the walk's answer -- it jumps from each quote
/// that opens a field to the one that closes it, a doubled quote inside being
/// two of them -- without a scan per quoted field: on a row of thirteen quoted
/// fields the walk made twenty-six calls, and quoted CSV swept three times
/// slower than the same rows unquoted.
///
/// Parity is the walk's reading only while every quote that opens does so
/// where a field starts: after a delimiter, or straight after a closing quote
/// (the second half of a doubled one). A quote anywhere else is content to
/// the walk and would flip the count, so the first one met before the answer
/// hands the row to the walk. Counting it instead took the rows up to the
/// next such quote as one: a file with `5" nail` in every thousandth row was
/// read as half its rows, and a pair that differed compared as identical.
#[inline(always)]
fn end_of_row_counted<S: Scan>(
    scan: S,
    data: &[u8],
    pos: usize,
    end: usize,
    delimiter: u8,
) -> usize {
    // An `end` past the slab is answered as the walk answers it: unchanged.
    let limit = end.min(data.len());
    // Up to the first quote this is the walk -- one compare a register, and
    // most rows have no quote past their keys.
    let first = next_of2_with(scan, data, pos, limit, b'\n', b'"');
    if first >= limit {
        return end;
    }
    if data[first] == b'\n' {
        return first;
    }
    let top: usize = S::STEP - 1;
    let mut at = first;
    let mut inside = 0u64; // all ones while a quoted field carries over
    // Whether the byte before the register lets a quote open a field. No
    // quote comes before the row's first, so that one opens only at the
    // start of a field.
    let mut opens_field = u64::from(first == pos || data[first - 1] == delimiter);
    while at < limit {
        if at + S::STEP > data.len() {
            // The last bytes of the slab: the walk, from a known state.
            let mut opens = opens_field != 0;
            if inside != 0 {
                at = skip_quoted_with(scan, data, at, limit);
                opens = false;
            }
            let found = end_of_row_walked(scan, data, at, opens, limit, delimiter);
            return if found >= limit { end } else { found };
        }
        let room = limit - at;
        let valid = if room >= S::STEP {
            u64::MAX >> (64 - S::STEP)
        } else {
            (1u64 << room) - 1
        };
        // Unmasked: a delimiter past `limit` can only vouch for a quote
        // past it, and those are masked.
        let delims = scan.hits(data, at, delimiter, delimiter);
        let quotes = scan.hits(data, at, b'"', b'"') & valid;
        let newlines = scan.hits(data, at, b'\n', b'\n') & valid;
        // Bit i set: byte i is inside quotes once it is read. A quote that
        // leaves the bit set opened, one that clears it closed.
        let quoted = prefix_xor(quotes) ^ inside;
        let opening = quotes & quoted;
        // What a quote may open after: a delimiter, or a quote that closed.
        let before_field = (delims | quotes) & !opening;
        let stray = opening & !((before_field << 1) | opens_field);
        let free = newlines & !quoted;
        if stray != 0 {
            let row = if free == 0 {
                u64::MAX
            } else {
                (1u64 << free.trailing_zeros()) - 1
            };
            if stray & row != 0 {
                let found = end_of_row_walked(scan, data, pos, true, limit, delimiter);
                return if found >= limit { end } else { found };
            }
        }
        if free != 0 {
            return at + free.trailing_zeros() as usize;
        }
        opens_field = (before_field >> top) & 1;
        if quotes.count_ones() & 1 == 1 {
            inside = !inside;
        }
        at += S::STEP;
    }
    end
}

/// [`end_of_row`] as the field walk reads a row, without stopping at each
/// delimiter: from `at`, outside any quoted field, to the next newline or
/// quote. A quote opens a quoted field at the start of a field -- after a
/// delimiter, or at `at` itself when `opens` says a field starts there -- and
/// is content anywhere else.
#[inline(always)]
fn end_of_row_walked<S: Scan>(
    scan: S,
    data: &[u8],
    at: usize,
    opens: bool,
    end: usize,
    delimiter: u8,
) -> usize {
    let mut from = at;
    while from < end {
        let next = next_of2_with(scan, data, from, end, b'\n', b'"');
        if next >= end {
            return end;
        }
        if data[next] == b'\n' {
            return next;
        }
        let starts_field = if next == at {
            opens
        } else {
            data[next - 1] == delimiter
        };
        from = if starts_field {
            skip_quoted_with(scan, data, next + 1, end)
        } else {
            next + 1
        };
    }
    end
}

/// Splits rows into fields, projecting straight to the columns asked for.
///
/// The CSV form is addressed by column number: once the last needed column has
/// been read the rest of the row is skipped to its newline without its fields
/// ever being delimited, so on twenty columns keyed on the first two most of a
/// row is never looked at. The JSON form is addressed by key instead, so it
/// walks the whole object — but only once, and one hash per key rather than a
/// search per wanted column, which at twenty columns would be four hundred
/// comparisons a row.
pub(super) enum RowParser {
    Csv {
        delimiter: u8,
        last_needed: usize,
        /// Which slots each column of the file feeds. Inverted from the wanted
        /// list once, so storing a field is a lookup rather than a walk.
        slots_for: Vec<Vec<u16>>,
        /// The one slot each column feeds, or a sentinel. Almost every column
        /// feeds exactly one, and the hot path stores through this rather than
        /// chasing a pointer into `slots_for` and looping -- per column, per
        /// parse, and a row is parsed about two and a half times: once by the
        /// sweep, then again for each side of the join and once more when a
        /// probe compares keys.
        single: Vec<i32>,
    },
    Json {
        /// The key whose value belongs in each slot; `None` for a column this
        /// file does not have.
        wanted: Vec<Option<String>>,
        /// Open-addressed name to slot, so a key costs one hash.
        slots: Vec<i32>,
        slot_mask: usize,
        /// Slots below this are key columns: the first value wins, and once
        /// every slot is a filled key column the rest of the object is skipped.
        key_size: usize,
        /// What the table answers for `wanted[i]`: `i` itself unless the same
        /// name also fills an earlier slot, and -1 for a missing column or a
        /// name holding a backslash, which is never guessed. Names
        /// arrive in the same order row after row, usually slot order, so the
        /// member after slot `i` is tried as `i + 1` first, and a right guess
        /// has to give exactly the slot the table would.
        canon: Vec<i32>,
        /// The file frames its rows by their braces ([`json_framed`]): a
        /// top-level array, whose `[`, `]` and `,` separators are container
        /// syntax, or a stream of objects; either way an object can span lines.
        /// ndjson keeps the plain one-memchr row end -- it is most of the
        /// JSON this reader ever sees, and the container walk costs it
        /// three quarters again of a sweep.
        array: bool,
    },
}

/// `single[column]` when the column feeds no slot, and when it feeds more
/// than one.
const NONE: i32 = -1;
const MANY: i32 = -2;

/// A word at a time rather than a byte at a time: the first and last eight
/// bytes (four, or three single bytes, for a shorter name) and the length. A
/// byte loop is a serial multiply per byte, twenty names a row; collisions are
/// settled by comparing the name, so this only has to spread them.
fn name_hash(s: &[u8]) -> u64 {
    let n = s.len();
    let (a, b) = if n >= 8 {
        (
            u64::from_le_bytes(s[..8].try_into().unwrap()),
            u64::from_le_bytes(s[n - 8..].try_into().unwrap()),
        )
    } else if n >= 4 {
        (
            u32::from_le_bytes(s[..4].try_into().unwrap()) as u64,
            u32::from_le_bytes(s[n - 4..].try_into().unwrap()) as u64,
        )
    } else if n > 0 {
        (
            s[0] as u64 | (s[n / 2] as u64) << 8 | (s[n - 1] as u64) << 16,
            0,
        )
    } else {
        (0, 0)
    };
    let h =
        (a ^ b.wrapping_mul(0x9e37_79b9_7f4a_7c15) ^ n as u64).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    h ^ (h >> 31)
}

/// Where the wanted part of a row ends, when two files agree closely enough for
/// one to be measured against the other in bytes.
///
/// The join's expensive question is whether a matched pair differs, and it
/// answers it by parsing both rows into fields. It does not have to. If the two
/// rows are byte-identical up to the end of the last column either file wants,
/// then every column in between is byte-identical too, and no parse can say
/// otherwise -- so the pair is unchanged and the mate never needs reading.
///
/// That holds only when both sides really are the same shape: the same
/// delimiter and the same column-to-slot mapping. Two files whose headers are
/// ordered differently can carry identical bytes and mean different things,
/// which is what comparing `slots_for` rules out -- the whole mapping, not the
/// one-slot shortcut derived from it, because two columns that each feed several
/// slots can agree on how many without agreeing on which. JSON has no such
/// prefix -- its keys may come in any order -- so it never qualifies.
///
/// Returns the slot holding the last wanted column, the delimiter that ends
/// it, and the source column index of that column (so the caller can count
/// delimiters to it without parsing).
pub(super) fn shared_tail(a: &RowParser, b: &RowParser) -> Option<(usize, u8, usize)> {
    match (a, b) {
        (
            RowParser::Csv {
                delimiter: da,
                last_needed: la,
                slots_for: fa,
                ..
            },
            RowParser::Csv {
                delimiter: db,
                slots_for: fb,
                ..
            },
        ) if da == db && fa == fb => fa[*la].first().map(|slot| (*slot as usize, *da, *la)),
        _ => None,
    }
}

/// The proof's byte span without parsing the row.
///
/// `csv_span` in the join reads it off the guard field's offsets, which means
/// parsing every column first. The guard is source column `guard_src`, so its
/// end is the (`guard_src`+1)th delimiter from the row start -- countable with
/// the delimiter cursor alone, skipping a quoted field the way the row parse
/// does. Returns `None` when the row ends first (short or malformed), which
/// falls back to the parse.
///
/// The span is the field bytes, not including the delimiter that ends them --
/// the same `data[from..to]` the field-offset path builds -- so the proof's
/// boundary check in `row_matches` applies unchanged. A guard that is the
/// row's last column ends at the newline instead of a delimiter.
pub(super) fn guard_span(
    data: &[u8],
    lo: usize,
    hi: usize,
    delimiter: u8,
    commas: usize,
) -> Option<usize> {
    #[cfg(target_arch = "x86_64")]
    if let Some(w) = Wide512::detect() {
        // Safety: `w` proves the CPU has AVX-512BW.
        return unsafe { guard_span_wide512(w, data, lo, hi, delimiter, commas) };
    }
    match Wide::detect() {
        // Safety: `w` proves the CPU has AVX2, or NEON on aarch64.
        Some(w) => unsafe { guard_span_wide(w, data, lo, hi, delimiter, commas) },
        None => guard_span_with(Swar, data, lo, hi, delimiter, commas),
    }
}

/// [`guard_span_with`], compiled for AVX2 in a baseline x86-64 build, and for NEON on aarch64.
///
/// # Safety
/// Holding a `Wide` is the proof the CPU has AVX2, or NEON on aarch64; nothing else is required.
#[cfg_attr(
    all(
        target_arch = "x86_64",
        not(any(target_feature = "avx2", target_feature = "avx512bw"))
    ),
    target_feature(enable = "avx2,popcnt,bmi1")
)]
#[cfg_attr(target_arch = "aarch64", target_feature(enable = "neon"))]
// Safety: `w: Wide` proves the CPU has AVX2, BMI1 and POPCNT on x86-64, or NEON on aarch64, which this fn's `target_feature`
// enables; the body is the safe `guard_span_with`, so the feature is the only requirement.
unsafe fn guard_span_wide(
    w: Wide,
    data: &[u8],
    lo: usize,
    hi: usize,
    delimiter: u8,
    commas: usize,
) -> Option<usize> {
    guard_span_with(w, data, lo, hi, delimiter, commas)
}

/// [`guard_span_with`], compiled for AVX-512BW in a baseline x86-64 build.
///
/// # Safety
/// Holding a `Wide512` is the proof the CPU has AVX-512BW; nothing else is required.
#[cfg(target_arch = "x86_64")]
#[cfg_attr(
    not(target_feature = "avx512bw"),
    target_feature(enable = "avx512bw,popcnt,bmi1")
)]
// Safety: `w: Wide512` proves the CPU has AVX-512BW, BMI1 and POPCNT, which this fn's `target_feature`
// enables; the body is the safe `guard_span_with`, so the feature is the only requirement.
unsafe fn guard_span_wide512(
    w: Wide512,
    data: &[u8],
    lo: usize,
    hi: usize,
    delimiter: u8,
    commas: usize,
) -> Option<usize> {
    guard_span_with(w, data, lo, hi, delimiter, commas)
}

#[inline(always)]
fn guard_span_with<S: Scan>(
    scan: S,
    data: &[u8],
    lo: usize,
    hi: usize,
    delimiter: u8,
    commas: usize,
) -> Option<usize> {
    if let Some(answer) = guard_span_counted(scan, data, lo, hi, delimiter, commas) {
        return answer;
    }
    let mut at = lo;
    let mut delims = Delims::new(scan, data, lo, hi, delimiter, b'\n');
    for i in 0..commas {
        let cur = if at < hi && data[at] == b'"' {
            let close = skip_quoted_with(scan, data, at + 1, hi);
            delims.next(close)
        } else {
            delims.next(at)
        };
        if cur >= hi {
            return None;
        }
        let last = i + 1 == commas;
        if data[cur] == delimiter {
            at = cur + 1;
            if last {
                return Some(cur - lo);
            }
        } else if last && data[cur] == b'\n' {
            return Some(cur - lo);
        } else {
            return None;
        }
    }
    None
}

/// The gapped proof's plan, where a column nobody wants sits among the wanted
/// ones -- an ignored `updated_at` in the middle of the row, say.
///
/// `guard_span` covers one run: A's row from its start through the last column
/// either file wants, compared as bytes. An unwanted column inside that run that
/// differs -- and an ignored timestamp differs on every row -- fails the proof
/// every time, and every row is parsed. So the wanted columns are compared as
/// runs instead, and the unwanted ones between them skipped by field count in
/// each row on its own: an ignored value that differs cannot stop a row proving
/// equal, and one that differs in length cannot misalign what follows. `seg[j]`
/// wanted fields are followed by `gap[j]` unwanted ones; the last run has no
/// gap. The C and C++ ports have the same proof (#229, #230).
pub(super) struct Runs {
    seg: Vec<usize>,
    gap: Vec<usize>,
}

impl Runs {
    /// `None` where there is no gap, and the one-run proof applies unchanged.
    /// Only called once `shared_tail` has agreed the two sides' shapes, so A's
    /// map of wanted columns is B's too.
    pub(super) fn plan(a: &RowParser) -> Option<Runs> {
        let RowParser::Csv {
            last_needed,
            slots_for,
            ..
        } = a
        else {
            return None;
        };
        let (mut seg, mut gap) = (vec![0usize], vec![0usize]);
        for slots in &slots_for[..=*last_needed] {
            if slots.is_empty() {
                *gap.last_mut()? += 1;
                continue;
            }
            if *gap.last()? > 0 {
                seg.push(0);
                gap.push(0);
            }
            *seg.last_mut()? += 1;
        }
        (seg.len() > 1).then_some(Runs { seg, gap })
    }

    pub(super) fn len(&self) -> usize {
        self.seg.len()
    }

    /// Where each run starts in this row and how many bytes it takes, through
    /// the byte that closes it, into `out`. `false` where the row ends first,
    /// which falls back to the parse.
    pub(super) fn of_row(
        &self,
        data: &[u8],
        lo: usize,
        hi: usize,
        delimiter: u8,
        out: &mut [(usize, usize)],
    ) -> bool {
        #[cfg(target_arch = "x86_64")]
        if let Some(w) = Wide512::detect() {
            // Safety: `w` proves the CPU has AVX-512BW.
            return unsafe { self.of_row_wide512(w, data, lo, hi, delimiter, out) };
        }
        match Wide::detect() {
            // Safety: `w` proves the CPU has AVX2, or NEON on aarch64.
            Some(w) => unsafe { self.of_row_wide(w, data, lo, hi, delimiter, out) },
            None => self.of_row_with(Swar, data, lo, hi, delimiter, out),
        }
    }

    /// [`Runs::of_row_with`], compiled for AVX2 in a baseline x86-64 build, and for NEON on aarch64.
    ///
    /// # Safety
    /// Holding a `Wide` is the proof the CPU has AVX2, or NEON on aarch64; nothing else is required.
    #[cfg_attr(
        all(
            target_arch = "x86_64",
            not(any(target_feature = "avx2", target_feature = "avx512bw"))
        ),
        target_feature(enable = "avx2,popcnt,bmi1")
    )]
    #[cfg_attr(target_arch = "aarch64", target_feature(enable = "neon"))]
    // Safety: `w: Wide` proves the CPU has AVX2, BMI1 and POPCNT on x86-64, or NEON on aarch64, which this fn's `target_feature`
    // enables; the body is the safe `of_row_with`, so the feature is the only requirement.
    unsafe fn of_row_wide(
        &self,
        w: Wide,
        data: &[u8],
        lo: usize,
        hi: usize,
        delimiter: u8,
        out: &mut [(usize, usize)],
    ) -> bool {
        self.of_row_with(w, data, lo, hi, delimiter, out)
    }

    /// [`Runs::of_row_with`], compiled for AVX-512BW in a baseline x86-64 build.
    ///
    /// # Safety
    /// Holding a `Wide512` is the proof the CPU has AVX-512BW; nothing else is required.
    #[cfg(target_arch = "x86_64")]
    #[cfg_attr(
        not(target_feature = "avx512bw"),
        target_feature(enable = "avx512bw,popcnt,bmi1")
    )]
    // Safety: `w: Wide512` proves the CPU has AVX-512BW, BMI1 and POPCNT, which this fn's `target_feature`
    // enables; the body is the safe `of_row_with`, so the feature is the only requirement.
    unsafe fn of_row_wide512(
        &self,
        w: Wide512,
        data: &[u8],
        lo: usize,
        hi: usize,
        delimiter: u8,
        out: &mut [(usize, usize)],
    ) -> bool {
        self.of_row_with(w, data, lo, hi, delimiter, out)
    }

    #[inline(always)]
    fn of_row_with<S: Scan>(
        &self,
        scan: S,
        data: &[u8],
        lo: usize,
        hi: usize,
        delimiter: u8,
        out: &mut [(usize, usize)],
    ) -> bool {
        let mut delims = Delims::new(scan, data, lo, hi, delimiter, b'\n');
        let mut at = lo;
        let n = self.seg.len();
        for (j, (slot, &fields)) in out.iter_mut().zip(&self.seg).enumerate() {
            let last = j + 1 == n;
            let Some(end) = past_fields(scan, &mut delims, data, at, hi, delimiter, fields, last)
            else {
                return false;
            };
            *slot = (at, end - at);
            if last {
                break;
            }
            match past_fields(
                scan,
                &mut delims,
                data,
                end,
                hi,
                delimiter,
                self.gap[j],
                false,
            ) {
                Some(next) => at = next,
                None => return false,
            }
        }
        true
    }

    /// Whether B's row `b[lo..hi]` holds each of A's runs where its own gaps
    /// put it. A run's bytes include the byte that closes it, so equal bytes
    /// leave B at the start of its next field, as they leave A.
    pub(super) fn matches(
        &self,
        a: &[u8],
        spans: &[(usize, usize)],
        b: &[u8],
        lo: usize,
        hi: usize,
        delimiter: u8,
    ) -> bool {
        #[cfg(target_arch = "x86_64")]
        if let Some(w) = Wide512::detect() {
            // Safety: `w` proves the CPU has AVX-512BW.
            return unsafe { self.matches_wide512(w, a, spans, b, lo, hi, delimiter) };
        }
        match Wide::detect() {
            // Safety: `w` proves the CPU has AVX2, or NEON on aarch64.
            Some(w) => unsafe { self.matches_wide(w, a, spans, b, lo, hi, delimiter) },
            None => self.matches_with(Swar, a, spans, b, lo, hi, delimiter),
        }
    }

    /// [`Runs::matches_with`], compiled for AVX2 in a baseline x86-64 build, and for NEON on aarch64.
    ///
    /// # Safety
    /// Holding a `Wide` is the proof the CPU has AVX2, or NEON on aarch64; nothing else is required.
    #[allow(clippy::too_many_arguments)]
    #[cfg_attr(
        all(
            target_arch = "x86_64",
            not(any(target_feature = "avx2", target_feature = "avx512bw"))
        ),
        target_feature(enable = "avx2,popcnt,bmi1")
    )]
    #[cfg_attr(target_arch = "aarch64", target_feature(enable = "neon"))]
    // Safety: `w: Wide` proves the CPU has AVX2, BMI1 and POPCNT on x86-64, or NEON on aarch64, which this fn's `target_feature`
    // enables; the body is the safe `matches_with`, so the feature is the only requirement.
    unsafe fn matches_wide(
        &self,
        w: Wide,
        a: &[u8],
        spans: &[(usize, usize)],
        b: &[u8],
        lo: usize,
        hi: usize,
        delimiter: u8,
    ) -> bool {
        self.matches_with(w, a, spans, b, lo, hi, delimiter)
    }

    /// [`Runs::matches_with`], compiled for AVX-512BW in a baseline x86-64 build.
    ///
    /// # Safety
    /// Holding a `Wide512` is the proof the CPU has AVX-512BW; nothing else is required.
    #[allow(clippy::too_many_arguments)]
    #[cfg(target_arch = "x86_64")]
    #[cfg_attr(
        not(target_feature = "avx512bw"),
        target_feature(enable = "avx512bw,popcnt,bmi1")
    )]
    // Safety: `w: Wide512` proves the CPU has AVX-512BW, BMI1 and POPCNT, which this fn's `target_feature`
    // enables; the body is the safe `matches_with`, so the feature is the only requirement.
    unsafe fn matches_wide512(
        &self,
        w: Wide512,
        a: &[u8],
        spans: &[(usize, usize)],
        b: &[u8],
        lo: usize,
        hi: usize,
        delimiter: u8,
    ) -> bool {
        self.matches_with(w, a, spans, b, lo, hi, delimiter)
    }

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn matches_with<S: Scan>(
        &self,
        scan: S,
        a: &[u8],
        spans: &[(usize, usize)],
        b: &[u8],
        lo: usize,
        hi: usize,
        delimiter: u8,
    ) -> bool {
        let mut delims = Delims::new(scan, b, lo, hi, delimiter, b'\n');
        let mut at = lo;
        let n = self.seg.len();
        for (j, &(from, len)) in spans.iter().enumerate().take(n) {
            if len > hi - at || a[from..from + len] != b[at..at + len] {
                return false;
            }
            at += len;
            if j + 1 == n {
                break;
            }
            match past_fields(scan, &mut delims, b, at, hi, delimiter, self.gap[j], false) {
                Some(next) => at = next,
                None => return false,
            }
        }
        true
    }
}

/// Past the byte that closes the `n`th field from `at`, or `None` where the row
/// ends first. With `may_end` the last of them may close on the line ending: a
/// run that ends in the row's last column has nothing else to close it.
#[allow(clippy::too_many_arguments)]
#[inline(always)]
fn past_fields<S: Scan>(
    scan: S,
    delims: &mut Delims<'_, S>,
    data: &[u8],
    mut at: usize,
    hi: usize,
    delimiter: u8,
    n: usize,
    may_end: bool,
) -> Option<usize> {
    for i in 0..n {
        let cur = if at < hi && data[at] == b'"' {
            delims.next(skip_quoted_with(scan, data, at + 1, hi))
        } else {
            delims.next(at)
        };
        if cur >= hi {
            return None;
        }
        if data[cur] != delimiter && !(may_end && i + 1 == n && data[cur] == b'\n') {
            return None;
        }
        at = cur + 1;
    }
    Some(at)
}

impl RowParser {
    pub(super) fn csv(delimiter: u8, source: Vec<Option<usize>>) -> Self {
        let last_needed = source.iter().flatten().copied().max().unwrap_or(0);
        // Inverted once, so storing a field is a lookup rather than a walk of
        // every wanted column: twenty columns against twenty slots is four
        // hundred comparisons a row otherwise. A column can feed more than one
        // slot -- `--compare` may name a key column -- so each entry is a list,
        // and it is one element deep in every case but that one.
        let mut slots_for: Vec<Vec<u16>> = vec![Vec::new(); last_needed + 1];
        for (slot, at) in source.iter().enumerate() {
            if let Some(column) = at {
                slots_for[*column].push(slot as u16);
            }
        }
        // The one-slot shortcut, from the same inversion. NONE is a column no
        // slot wants; MANY is `--compare` naming a key column, the only way a
        // column feeds two, and worth a branch rather than a second array.
        let single = slots_for
            .iter()
            .map(|s| match s.len() {
                0 => NONE,
                1 => i32::from(s[0]),
                _ => MANY,
            })
            .collect();
        RowParser::Csv {
            delimiter,
            last_needed,
            slots_for,
            single,
        }
    }

    pub(super) fn json(wanted: Vec<Option<String>>, key_size: usize, array: bool) -> Self {
        let mut n = 16usize;
        while n < wanted.len() * 4 {
            n <<= 1;
        }
        let mut slots = vec![-1i32; n];
        let slot_mask = n - 1;
        for (i, name) in wanted.iter().enumerate() {
            let Some(name) = name else { continue };
            let mut at = (name_hash(name.as_bytes()) as usize) & slot_mask;
            while slots[at] >= 0 {
                at = (at + 1) & slot_mask;
            }
            slots[at] = i as i32;
        }
        let mut parser = RowParser::Json {
            key_size: key_size.min(wanted.len()),
            wanted,
            slots,
            slot_mask,
            canon: Vec::new(),
            array,
        };
        let canon: Vec<i32> = match &parser {
            RowParser::Json { wanted, .. } => wanted
                .iter()
                .map(|w| match w {
                    Some(w) if w.contains('\\') => -1,
                    Some(w) => parser.slot_for(w.as_bytes()).map_or(-1, |s| s as i32),
                    None => -1,
                })
                .collect(),
            RowParser::Csv { .. } => unreachable!(),
        };
        if let RowParser::Json { canon: c, .. } = &mut parser {
            *c = canon;
        }
        parser
    }

    /// Parses one row into `out`, returning the offset of the next row.
    ///
    /// A row shorter than the header leaves the missing fields [`ABSENT`], which
    /// compares as absent — a difference to report, not a file to refuse.
    pub(super) fn parse(&self, data: &[u8], start: usize, end: usize, out: &mut [Field]) -> usize {
        #[cfg(target_arch = "x86_64")]
        if let Some(w) = Wide512::detect() {
            // Safety: `w` proves the CPU has AVX-512BW.
            return unsafe { self.parse_wide512(w, data, start, end, out) };
        }
        match Wide::detect() {
            // Safety: `w` proves the CPU has AVX2, or NEON on aarch64.
            Some(w) => unsafe { self.parse_wide(w, data, start, end, out) },
            None => self.parse_with(Swar, data, start, end, out),
        }
    }

    /// [`RowParser::parse_with`], compiled for AVX2 in a baseline x86-64 build, and for NEON on aarch64, so
    /// every scan of the row inlines into it as a vector compare.
    ///
    /// # Safety
    /// Holding a `Wide` is the proof the CPU has AVX2, or NEON on aarch64; nothing else is required.
    #[cfg_attr(
        all(
            target_arch = "x86_64",
            not(any(target_feature = "avx2", target_feature = "avx512bw"))
        ),
        target_feature(enable = "avx2,popcnt,bmi1")
    )]
    #[cfg_attr(target_arch = "aarch64", target_feature(enable = "neon"))]
    // Safety: `w: Wide` proves the CPU has AVX2, BMI1 and POPCNT on x86-64, or NEON on aarch64, which this fn's `target_feature`
    // enables; the body is the safe `parse_with`, so the feature is the only requirement.
    unsafe fn parse_wide(
        &self,
        w: Wide,
        data: &[u8],
        start: usize,
        end: usize,
        out: &mut [Field],
    ) -> usize {
        self.parse_with(w, data, start, end, out)
    }

    /// [`RowParser::parse_with`], compiled for AVX-512BW in a baseline x86-64 build, so
    /// every scan of the row inlines into it as a 512-bit vector compare.
    ///
    /// # Safety
    /// Holding a `Wide512` is the proof the CPU has AVX-512BW; nothing else is required.
    #[cfg(target_arch = "x86_64")]
    #[cfg_attr(
        not(target_feature = "avx512bw"),
        target_feature(enable = "avx512bw,popcnt,bmi1")
    )]
    // Safety: `w: Wide512` proves the CPU has AVX-512BW, BMI1 and POPCNT, which this fn's `target_feature`
    // enables; the body is the safe `parse_with`, so the feature is the only requirement.
    unsafe fn parse_wide512(
        &self,
        w: Wide512,
        data: &[u8],
        start: usize,
        end: usize,
        out: &mut [Field],
    ) -> usize {
        self.parse_with(w, data, start, end, out)
    }

    #[inline(always)]
    fn parse_with<S: Scan>(
        &self,
        scan: S,
        data: &[u8],
        start: usize,
        end: usize,
        out: &mut [Field],
    ) -> usize {
        match self {
            RowParser::Csv {
                delimiter,
                last_needed,
                slots_for,
                single,
            } => self.parse_csv(
                scan,
                *delimiter,
                slots_for,
                single,
                *last_needed,
                data,
                start,
                end,
                out,
            ),
            RowParser::Json { .. } => self.parse_json(scan, data, start, end, out),
        }
    }

    #[allow(clippy::too_many_arguments)]
    #[inline(always)]
    fn parse_csv<S: Scan>(
        &self,
        scan: S,
        delimiter: u8,
        slots_for: &[Vec<u16>],
        single: &[i32],
        last_needed: usize,
        data: &[u8],
        start: usize,
        end: usize,
        out: &mut [Field],
    ) -> usize {
        // `out` is reused across rows, so a column this row does not reach has
        // to be blanked or it would show the previous row's value -- but that is
        // only the columns after the row ran out, and a well-formed row runs out
        // of nothing. Clearing all of them up front ran twenty stores per row
        // per file to no purpose.
        let blank_from = |out: &mut [Field], from: usize| {
            if from <= last_needed {
                for slots in &slots_for[from..=last_needed] {
                    for slot in slots {
                        out[*slot as usize] = ABSENT;
                    }
                }
            }
        };
        let mut pos = start;
        let mut column = 0usize;
        // One cursor for the row -- every byte scanned once, both delimiters
        // broadcast once -- but only where a scan step spans several fields. On
        // an eight-byte step it does not, and the plain scan is quicker; see
        // `Delims`.
        let mut delims = Delims::new(scan, data, start, end, delimiter, b'\n');

        while pos <= end {
            let (field, next) = if pos < end && data[pos] == b'"' {
                let close = skip_quoted_with(scan, data, pos + 1, end);
                let next = delims.next(close);
                (quoted_cell(scan, data, pos, close, next), next)
            } else {
                let next = if S::WIDE {
                    delims.next(pos)
                } else {
                    next_of2_with(scan, data, pos, end, delimiter, b'\n')
                };
                (plain_field(data, pos, next), next)
            };
            if column <= last_needed {
                let one = single[column];
                if one >= 0 {
                    out[one as usize] = field;
                } else if one == MANY {
                    for slot in &slots_for[column] {
                        out[*slot as usize] = field;
                    }
                }
            }
            column += 1;

            if next >= end {
                // The data ends here, and a quoted field it ends in is read
                // by its own rule: once a file, not once a field.
                if let Some(slots) = slots_for.get(column - 1) {
                    store_at_the_end(scan, data, pos, end, slots, out);
                }
                blank_from(out, column);
                return end;
            }
            if data[next] == b'\n' {
                blank_from(out, column);
                return next + 1;
            }
            pos = next + 1;
            // Every needed column is filled, so there is nothing to blank: the
            // rest of the row is skipped without being parsed.
            if column > last_needed {
                let eol = end_of_row(scan, data, pos, end, delimiter);
                return if eol >= end { end } else { eol + 1 };
            }
        }
        blank_from(out, column);
        end
    }

    /// Walks one JSON object, storing the values of the keys we want.
    #[inline(always)]
    fn parse_json<S: Scan>(
        &self,
        scan: S,
        data: &[u8],
        start: usize,
        end: usize,
        out: &mut [Field],
    ) -> usize {
        let RowParser::Json {
            wanted,
            key_size,
            canon,
            array,
            ..
        } = self
        else {
            unreachable!("parse_json on a CSV parser");
        };
        out.fill(ABSENT);
        let mut found = 0; // key slots filled, for the early exit below
        let mut guess = 0; // the slot the next member most likely fills
        let mut open = false; // the early exit left the object unfinished
        let mut pos = start;
        while pos < end && json_space(data[pos]) {
            pos += 1;
        }
        // The opening bracket of a top-level array is container syntax, not a
        // row: step past it and the first object starts the rows.
        if *array && pos < end && data[pos] == b'[' {
            pos += 1;
            while pos < end && json_space(data[pos]) {
                pos += 1;
            }
        }
        if pos >= end {
            return end;
        }
        if data[pos] != b'{' {
            return end_of_json_row(scan, data, pos, end, *array, false); // not an object: skip the line
        }
        pos += 1;

        loop {
            // Compact ndjson separates members with a bare `,"`, which goes
            // straight to the next key; the general path takes the comma, loops
            // back and skips space twice to get there.
            if pos + 1 < end && data[pos] == b',' && data[pos + 1] == b'"' {
                pos += 1;
            } else {
                while pos < end && json_space(data[pos]) {
                    pos += 1;
                }
                if pos >= end {
                    break;
                }
                match data[pos] {
                    b'}' => {
                        pos += 1;
                        break;
                    }
                    b',' => {
                        pos += 1;
                        continue;
                    }
                    b'"' => {}
                    _ => break, // malformed: stop reading this object
                }
            }
            let key_from = pos + 1;
            let (key_end, key_escaped) = skip_json_string(scan, data, pos, end);
            // A row cut off right after a key's opening quote has no key.
            if key_end > end || key_end <= key_from {
                break;
            }
            let key = &data[key_from..key_end - 1];
            pos = key_end;
            // And `":"` between a key and a string value, likewise.
            if pos + 1 < end && data[pos] == b':' && data[pos + 1] == b'"' {
                pos += 1;
            } else {
                while pos < end && json_space(data[pos]) {
                    pos += 1;
                }
                if pos >= end || data[pos] != b':' {
                    break;
                }
                pos += 1;
                while pos < end && json_space(data[pos]) {
                    pos += 1;
                }
                if pos >= end {
                    break;
                }
            }

            let field = if data[pos] == b'"' {
                let from = pos + 1;
                let (close, escaped) = skip_json_string(scan, data, pos, end);
                let to = close.saturating_sub(1).max(from);
                pos = close;
                Some(pack(from as u64, (to - from) as u64, escaped))
            } else if data[pos] == b'{' || data[pos] == b'[' {
                // An object or array is a cell of its JSON text. Left absent,
                // as it once was, two different objects read as the same
                // missing value and the change went unreported. Flagged as
                // escaped, so that equality, the hash and the report read it
                // past its spacing and its strings' escapes: the bytes
                // compared as they stand are only the fast answer (see
                // `slab::Slab::mode`).
                let from = pos;
                pos = skip_json_nested(scan, data, pos, end);
                Some(pack(from as u64, (pos - from) as u64, true))
            } else {
                // A number, true, false or null: it runs to the next comma,
                // brace or space.
                let from = pos;
                while pos < end && data[pos] != b',' && data[pos] != b'}' && !json_space(data[pos])
                {
                    pos += 1;
                }
                if &data[from..pos] == b"null" {
                    None
                } else {
                    Some(pack(from as u64, (pos - from) as u64, false))
                }
            };
            // A name written with an escape is looked up as what it spells.
            // It holds a backslash as written, so it can equal as written
            // only a wanted name holding one, and those are never guessed.
            if let Some(field) = field
                && let Some(slot) = match wanted.get(guess) {
                    Some(Some(w)) if w.as_bytes() == key && canon[guess] >= 0 => {
                        Some(canon[guess] as usize)
                    }
                    _ if key_escaped => self.slot_for_escaped(key),
                    _ => self.slot_for(key),
                }
            {
                guess = slot + 1;
                // First occurrence wins for a key column, and only for a key
                // column -- the rule the C port states. A JSON object is not
                // supposed to repeat a name, but the key-only parse stops as
                // soon as it has the keys, and it has to agree with the full
                // parse on what a row's key is: last-wins would let it stop on a
                // different value than the full parse ends with, a lookup that
                // misses its own row. Compared columns keep last-wins.
                if slot < *key_size {
                    if out[slot] == ABSENT {
                        out[slot] = field;
                        found += 1;
                        // Every key found and nothing else wanted: the rest of
                        // the object is bytes to skip, not fields to parse.
                        if found == wanted.len() {
                            open = true;
                            break;
                        }
                    }
                } else {
                    out[slot] = field;
                }
            }
        }
        end_of_json_row(scan, data, pos, end, *array, open)
    }

    /// [`Self::slot_for`] for a member name that holds a backslash: the slot
    /// of the name its escapes spell, as the header and the later-key walk
    /// read it ([`text_of`]). Looked up as written, `caf\u00e9` (how Python's
    /// `json.dumps` writes `café` by default) named no column, and that
    /// column's values were read on neither side.
    ///
    /// Out of line: a writer that escapes a name escapes it in every row, but
    /// most writers escape none, and their parse keeps one test of a flag it
    /// already has.
    #[cold]
    #[inline(never)]
    fn slot_for_escaped(&self, key: &[u8]) -> Option<usize> {
        // Undoing an escape never lengthens a name, so most fit here.
        let mut stack = [0u8; 128];
        let mut chars = json_string(key);
        let mut n = 0;
        while n < stack.len() {
            match chars.next() {
                Some(b) => stack[n] = b,
                None => return self.slot_for_name(&stack[..n]),
            }
            n += 1;
        }
        let mut heap = stack.to_vec();
        heap.extend(chars);
        self.slot_for_name(&heap)
    }

    /// A decoded name's slot. One that is not UTF-8 (an escaped lone
    /// surrogate) is looked up as [`text_of`] made it a column name: with
    /// the replacement character.
    fn slot_for_name(&self, name: &[u8]) -> Option<usize> {
        match std::str::from_utf8(name) {
            Ok(_) => self.slot_for(name),
            Err(_) => self.slot_for(String::from_utf8_lossy(name).as_bytes()),
        }
    }

    pub(super) fn slot_for(&self, key: &[u8]) -> Option<usize> {
        let RowParser::Json {
            wanted,
            slots,
            slot_mask,
            ..
        } = self
        else {
            return None;
        };
        let mut at = (name_hash(key) as usize) & slot_mask;
        loop {
            let i = slots[at];
            if i < 0 {
                return None;
            }
            if wanted[i as usize].as_deref().map(str::as_bytes) == Some(key) {
                return Some(i as usize);
            }
            at = (at + 1) & slot_mask;
        }
    }
}

/// Does the mate's tail name a column this run tracks?
///
/// The CSV proof rests on a column sitting at a fixed offset. JSON makes no such
/// promise: a value is found by name, and a name repeated in one object takes its
/// *last* value for a compared column — which the C and C++ ports do too, and
/// `test.sh` cross-checks — so a second `"amount"` past the diverging byte would
/// carry a value the prefix never saw.
///
/// It cannot be ruled out in general, but it can be ruled out *here*, because the
/// proof only reaches this point when the two rows agree all the way through the
/// last compared value. Whatever is left is the trailing ignored columns: a few
/// bytes. A name is a quoted string, so every quote in them is a candidate.
/// Mistaking a closing quote for an opening one costs a lookup that fails, which
/// is a fallback and not a wrong answer — and an escaped name counts as a hit for
/// the same reason, since the wanted names are held unescaped.
#[inline(always)]
fn json_tail_is_clean_with<S: Scan>(
    scan: S,
    p: &RowParser,
    d: &[u8],
    mut at: usize,
    end: usize,
) -> bool {
    while at < end {
        let q = next_of1_with(scan, d, at, end, b'"');
        if q >= end {
            return true;
        }
        let (close, escaped) = skip_json_string(scan, d, q, end);
        if escaped {
            return false;
        }
        let from = q + 1;
        let to = if close > q + 1 { close - 1 } else { q + 1 };
        if to > from && p.slot_for(&d[from..to]).is_some() {
            return false;
        }
        if close <= q {
            return false;
        }
        at = close;
    }
    true
}

/// The JSON proof, past ignored values that differ.
///
/// `a` is A's row from its start through the last value either side wants; the
/// plain proof holds when the mate opens with exactly those bytes and its tail
/// names nothing tracked. One ignored member in between whose value differs --
/// an `updated_at`, say, which differs on every row -- fails that on every row.
/// So where the two rows first differ, this finds the member of A's object that
/// byte is in. If it is the value of a name nobody tracks, the value is skipped
/// in each row on its own -- in B from the same offset, since everything before
/// it agreed, name included -- and the comparison carries on from there. A
/// difference anywhere else, in a tracked value, or in a shape the walk does not
/// follow fails the proof, and the row is parsed as before. The names before
/// each skipped value are equal bytes in both rows and the skipped values belong
/// to untracked names, so every tracked name sits at the same place with the
/// same bytes; the tail check covers what follows. The C and C++ ports have the
/// same proof (#234, #235).
pub(super) fn json_rows_match(p: &RowParser, a: &[u8], b: &[u8], b_lo: usize, b_hi: usize) -> bool {
    json_rows_match_at(p, a, 0, a.len(), b, b_lo, b_hi)
}

/// [`json_rows_match`] over A's bytes `a[a_lo..a_hi]` in place, so the scans
/// may read whole registers past the row's end.
pub(super) fn json_rows_match_at(
    p: &RowParser,
    a: &[u8],
    a_lo: usize,
    a_hi: usize,
    b: &[u8],
    b_lo: usize,
    b_hi: usize,
) -> bool {
    match Wide::detect() {
        // Safety: `w` proves the CPU has AVX2, or NEON on aarch64.
        Some(w) => unsafe { json_rows_match_wide(w, p, a, a_lo, a_hi, b, b_lo, b_hi) },
        None => json_rows_match_with(Swar, p, a, a_lo, a_hi, b, b_lo, b_hi),
    }
}

/// [`json_rows_match_with`], compiled for AVX2 in a baseline x86-64 build, and for NEON on aarch64.
///
/// # Safety
/// Holding a `Wide` is the proof the CPU has AVX2, or NEON on aarch64; nothing else is required.
#[cfg_attr(
    all(
        target_arch = "x86_64",
        not(any(target_feature = "avx2", target_feature = "avx512bw"))
    ),
    target_feature(enable = "avx2,popcnt,bmi1")
)]
#[cfg_attr(target_arch = "aarch64", target_feature(enable = "neon"))]
#[allow(clippy::too_many_arguments)]
// Safety: `w: Wide` proves the CPU has AVX2, BMI1 and POPCNT on x86-64, or NEON on aarch64, which this fn's `target_feature`
// enables; the body is the safe `json_rows_match_with`, so the feature is the only requirement.
unsafe fn json_rows_match_wide(
    w: Wide,
    p: &RowParser,
    a: &[u8],
    a_lo: usize,
    a_hi: usize,
    b: &[u8],
    b_lo: usize,
    b_hi: usize,
) -> bool {
    json_rows_match_with(w, p, a, a_lo, a_hi, b, b_lo, b_hi)
}

#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn json_rows_match_with<S: Scan>(
    scan: S,
    p: &RowParser,
    a: &[u8],
    a_lo: usize,
    a_hi: usize,
    b: &[u8],
    b_lo: usize,
    b_hi: usize,
) -> bool {
    if let Some(answer) = json_rows_match_counted(scan, p, a, a_lo, a_hi, b, b_lo, b_hi) {
        return answer;
    }
    let (mut ai, mut bi) = (a_lo, b_lo);
    let mut after_value = false;
    for _ in 0..64 {
        let left = a_hi - ai;
        let m = left.min(b_hi - bi);
        // The whole run first: equal slices are one memcmp, and they are the
        // common case -- every row whose ignored values do not differ.
        if m == left && a[ai..a_hi] == b[bi..bi + left] {
            return json_tail_is_clean_with(scan, p, b, bi + left, b_hi);
        }
        let same = common_prefix(&a[ai..ai + m], &b[bi..bi + m]);
        if same == left {
            return json_tail_is_clean_with(scan, p, b, bi + left, b_hi);
        }
        if same == m {
            return false; // B ends first
        }
        let Some((vs, ve)) = json_member_at(scan, p, a, a_hi, ai, ai + same, after_value) else {
            return false;
        };
        let b_vs = bi + (vs - ai);
        let b_ve = json_value_end(scan, b, b_vs, b_hi);
        if b_ve >= b_hi || b_ve <= b_vs {
            return false;
        }
        ai = ve;
        bi = b_ve;
        after_value = true;
    }
    false
}

/// How many leading bytes `x` and `y` share, eight at a time.
fn common_prefix(x: &[u8], y: &[u8]) -> usize {
    let n = x.len().min(y.len());
    let mut i = 0;
    while i + 8 <= n {
        let a = u64::from_le_bytes(x[i..i + 8].try_into().unwrap_or([0; 8]));
        let b = u64::from_le_bytes(y[i..i + 8].try_into().unwrap_or([0; 8]));
        if a != b {
            return i + ((a ^ b).trailing_zeros() / 8) as usize;
        }
        i += 8;
    }
    while i < n && x[i] == y[i] {
        i += 1;
    }
    i
}

fn json_gap(c: u8) -> bool {
    c == b' ' || c == b'\t' || c == b'\r'
}

/// Past one JSON value from `at`: a string, a scalar, or a nested object or
/// array. `end` where it does not close before `end`.
#[inline(always)]
fn json_value_end<S: Scan>(scan: S, d: &[u8], mut at: usize, end: usize) -> usize {
    if at >= end {
        return end;
    }
    match d[at] {
        b'"' => skip_json_string(scan, d, at, end).0,
        b'{' | b'[' => skip_json_nested(scan, d, at, end),
        _ => {
            while at < end && !matches!(d[at], b',' | b'}' | b']' | b'\n') && !json_gap(d[at]) {
                at += 1;
            }
            at
        }
    }
}

/// The member of `d`'s object whose value holds byte `diff`, walking from `at`
/// (the row start, or just past a value already skipped): its value's bounds.
/// `None` where `diff` falls in a name or the punctuation between members, in a
/// tracked member, or anywhere the walk cannot follow. A name written with an
/// escape is the name it spells, as the row parser reads it: equal bytes in
/// both rows, so the same name in both. `d` ends at the last wanted value, so
/// a value that runs to its end is not a gap.
#[inline(always)]
fn json_member_at<S: Scan>(
    scan: S,
    p: &RowParser,
    d: &[u8],
    end: usize,
    mut at: usize,
    diff: usize,
    mut after_value: bool,
) -> Option<(usize, usize)> {
    loop {
        while at < end && json_gap(d[at]) {
            at += 1;
        }
        if at >= end || at > diff || d[at] != if after_value { b',' } else { b'{' } {
            return None;
        }
        at += 1;
        while at < end && json_gap(d[at]) {
            at += 1;
        }
        if at >= end || d[at] != b'"' || at >= diff {
            return None;
        }
        let kq = at;
        let (kend, escaped) = skip_json_string(scan, d, at, end);
        if kend >= end || kend >= diff {
            return None;
        }
        at = kend;
        while at < end && json_gap(d[at]) {
            at += 1;
        }
        if at >= end || d[at] != b':' {
            return None;
        }
        at += 1;
        while at < end && json_gap(d[at]) {
            at += 1;
        }
        if at > diff {
            return None;
        }
        let v_end = json_value_end(scan, d, at, end);
        if v_end >= end || v_end <= at {
            return None;
        }
        if diff < v_end {
            let name = &d[kq + 1..kend - 1];
            let tracked = if escaped {
                p.slot_for_escaped(name)
            } else {
                p.slot_for(name)
            };
            if tracked.is_some() {
                return None;
            }
            return Some((at, v_end));
        }
        at = v_end;
        after_value = true;
    }
}

/// [`json_rows_match_with`] a register at a time, for rows with no backslash,
/// brace or bracket where they differ: one pass compares the two rows, counts
/// A's quotes and looks for those bytes, and the member a difference falls in
/// is read backwards from it instead of walked to. With no backslash every
/// quote opens or closes a string, so the count says whether the difference is
/// inside one, and the bytes just before it say whose value that is. A row of
/// twenty members differing in its last took a walk over nineteen of them for
/// every row of A the join proved: most of an NDJSON comparison's
/// instructions.
///
/// `None` hands the rows to the walk: a backslash, brace or bracket before a
/// difference (an escape or a nested value, which the count cannot read), a
/// register past the end of either slab, or a shape the backward read does not
/// follow. Otherwise the answer is the walk's.
#[inline(always)]
#[allow(clippy::too_many_arguments)]
fn json_rows_match_counted<S: Scan>(
    scan: S,
    p: &RowParser,
    a: &[u8],
    a_lo: usize,
    a_hi: usize,
    b: &[u8],
    b_lo: usize,
    b_hi: usize,
) -> Option<bool> {
    if !S::WIDE {
        return None;
    }
    // Past the object's opening brace, which is the same in both rows or the
    // rows differ there and the walk says so.
    let mut s = a_lo;
    while s < a_hi && json_gap(a[s]) {
        s += 1;
    }
    if s >= a_hi || a[s] != b'{' {
        return None;
    }
    s += 1;
    let lead = s - a_lo;
    if b_hi - b_lo < lead || a[a_lo..s] != b[b_lo..b_lo + lead] {
        return None;
    }
    let (mut ai, mut bi) = (s, b_lo + lead);
    // The lowest byte the backward read may look at: the brace, and later the
    // byte after the last value skipped.
    let mut floor = s - 1;
    for _ in 0..64 {
        let left = a_hi - ai;
        let m = left.min(b_hi - bi);
        let mut i = 0usize;
        let mut quotes = 0u32;
        let mut odd = false;
        let same = loop {
            if i >= m {
                break m;
            }
            if ai + i + S::STEP > a.len() || bi + i + S::STEP > b.len() {
                return None;
            }
            let room = m - i;
            // One bit per byte of the register, or of the bytes left: an
            // equality mask has its unused high bits clear, and inverted they
            // would read as differences.
            let valid = if room >= S::STEP {
                u64::MAX >> (64 - S::STEP)
            } else {
                (1u64 << room) - 1
            };
            let differ = !scan.same(a, ai + i, b, bi + i) & valid;
            let q = scan.hits(a, ai + i, b'"', b'"');
            let o = scan.hits(a, ai + i, b'\\', b'{') | scan.hits(a, ai + i, b'[', b'[');
            let before = if differ == 0 {
                valid
            } else {
                (1u64 << differ.trailing_zeros()) - 1
            };
            quotes += (q & before).count_ones();
            odd |= o & before != 0;
            if differ != 0 {
                break i + differ.trailing_zeros() as usize;
            }
            i += S::STEP;
        };
        if same == left {
            return Some(json_tail_is_clean_with(scan, p, b, bi + left, b_hi));
        }
        if same == m {
            return Some(false); // B ends first
        }
        if odd {
            return None;
        }
        let diff = ai + same;
        let (vs, ve) = match json_member_back(scan, p, a, a_hi, floor, diff, quotes & 1 == 1) {
            Some(Some(found)) => found,
            Some(None) => return Some(false),
            None => return None,
        };
        let b_vs = bi + (vs - ai);
        let b_ve = json_value_end(scan, b, b_vs, b.len());
        if b_ve >= b_hi || b_ve <= b_vs {
            return Some(false);
        }
        ai = ve;
        bi = b_ve;
        floor = ve;
    }
    Some(false)
}

/// The value of the member `diff` is in, read backwards from `diff`: from it
/// to the colon, then the name before that, which must be one nobody tracks.
/// `inside` says `diff` is inside a string. `floor` is the lowest byte the read
/// may look at.
///
/// `None` is a shape this does not follow; `Some(None)` a member the walk would
/// not skip either: a tracked name, or a difference in a name or between
/// members.
#[inline(always)]
fn json_member_back<S: Scan>(
    scan: S,
    p: &RowParser,
    d: &[u8],
    end: usize,
    floor: usize,
    diff: usize,
    inside: bool,
) -> Option<Option<(usize, usize)>> {
    let mut k = diff;
    let back = |k: &mut usize| -> Option<u8> {
        if *k <= floor {
            return None;
        }
        *k -= 1;
        Some(d[*k])
    };
    if inside {
        // The string's opening quote, then what comes before it.
        while back(&mut k)? != b'"' {}
        let mut c = back(&mut k)?;
        while json_gap(c) {
            c = back(&mut k)?;
        }
        if c != b':' {
            return Some(None); // a name differs
        }
    } else {
        // Only a scalar's own bytes, or space, may stand between the colon
        // and `diff`.
        loop {
            let c = back(&mut k)?;
            if c == b':' {
                break;
            }
            if !(json_gap(c) || c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b'+')) {
                return Some(None);
            }
        }
    }
    let colon = k;
    let mut c = back(&mut k)?;
    while json_gap(c) {
        c = back(&mut k)?;
    }
    if c != b'"' {
        return None;
    }
    let name_end = k;
    while back(&mut k)? != b'"' {}
    let name_start = k + 1;
    let mut c = back(&mut k)?;
    while json_gap(c) {
        c = back(&mut k)?;
    }
    if c != b',' && c != b'{' {
        return None;
    }
    if p.slot_for(&d[name_start..name_end]).is_some() {
        return Some(None);
    }
    let mut vs = colon + 1;
    while vs < end && json_gap(d[vs]) {
        vs += 1;
    }
    if vs > diff {
        return None;
    }
    // Scanned against the whole slab rather than the row, so the scan keeps
    // its registers up to the row's last bytes; a value that does not close
    // inside the row fails the check after it either way.
    let ve = json_value_end(scan, d, vs, d.len());
    if ve >= end || ve <= vs || diff >= ve {
        return Some(None);
    }
    Some(Some((vs, ve)))
}

/// The end of a row, which for newline-delimited JSON is the next newline byte
/// and nothing subtler.
///
/// This used to alternate a scan for `\n` or `"` with a walk over each string it
/// landed on, to avoid mistaking a newline inside a quoted value for the end of
/// the row. That cannot happen: RFC 8259 forbids the raw control characters
/// U+0000 to U+001F inside a string, and a newline is U+000A, so a valid JSON
/// string cannot contain one -- it must be written `\n`. The framing of ndjson
/// depends on exactly that.
///
/// The C port made this change first and its note carries the argument. Input
/// that does put a raw newline inside a string is not JSON, and this reader will
/// split the row there -- which is what every ndjson reader does, because the
/// format has no other way to say where a row ends.
/// `array` selects the walk: ndjson rows end at the newline, one memchr
/// exactly as ever, and everything below is the top-level array's syntax.
///
/// With `open`, the row's object was left unfinished -- the key-only parse
/// stops as soon as it has the keys -- and its remaining bytes run to the
/// closing brace first. Braces alone keep the depth: a `}` closes an object
/// wherever it sits, and a string hides its own braces behind `skip_json_string`.
#[inline(always)]
fn end_of_json_row<S: Scan>(
    scan: S,
    data: &[u8],
    pos: usize,
    end: usize,
    array: bool,
    open: bool,
) -> usize {
    if !array {
        let stop = next_of1_with(scan, data, pos, end, b'\n');
        return if stop >= end { end } else { stop + 1 };
    }
    end_of_array_row(scan, data, pos, end, open)
}

/// The array half of [`end_of_json_row`], kept out of its body: the ndjson
/// half is two instructions and inlines into every row's parse, and carrying
/// the container walk there cost the sweep real time.
fn end_of_array_row<S: Scan>(scan: S, data: &[u8], pos: usize, end: usize, open: bool) -> usize {
    let mut at = pos;
    if open {
        let mut depth = 1i32;
        while at < end {
            let b = next_of3_with(scan, data, at, end, b'"', b'{', b'}');
            if b >= end {
                return end;
            }
            match data[b] {
                b'"' => at = skip_json_string(scan, data, b, end).0,
                b'{' => {
                    depth += 1;
                    at = b + 1;
                }
                _ => {
                    depth -= 1;
                    at = b + 1;
                    if depth == 0 {
                        break;
                    }
                }
            }
        }
        if depth > 0 {
            return end;
        }
    }
    // Between rows a top-level array puts its own bytes: the `,` separator,
    // which the next object follows on the same line or a later one, and the
    // closing `]`, after which there are no more rows. A stream of objects
    // puts nothing but space: the next `{` is the next row, on this line or
    // another. Anything else ends at the next newline, exactly as a bare
    // ndjson row always has.
    let mut cur = at;
    while cur < end && json_space(data[cur]) {
        cur += 1;
    }
    if cur < end && data[cur] == b',' {
        cur += 1;
        while cur < end && json_space(data[cur]) {
            cur += 1;
        }
        if cur >= end || data[cur] == b']' {
            return end;
        }
        if data[cur] == b'{' {
            return cur;
        }
    } else if cur < end && data[cur] == b']' {
        return end;
    } else if cur < end && data[cur] == b'{' {
        return cur;
    }
    let stop = next_of1_with(scan, data, at, end, b'\n');
    if stop >= end { end } else { stop + 1 }
}

#[cfg(test)]
mod tests {
    use super::super::slab::Slab;
    use super::*;

    /// The counted JSON proof agrees with the member walk: rows that differ in
    /// a tracked or untracked value, a name, a member added or dropped, with
    /// spaces, escapes, nested values and numbers.
    #[test]
    fn json_rows_match_counted_agrees_with_the_walk() {
        let Some(w) = Wide::detect() else {
            return;
        };
        let p = RowParser::json(
            vec![
                Some("id".into()),
                Some("amount".into()),
                Some("note".into()),
            ],
            1,
            false,
        );
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let names = ["id", "amount", "note", "updated_at", "extra", "x"];
        let values = [
            "\"a\"",
            "\"b c\"",
            "12",
            "12.5",
            "-3",
            "true",
            "null",
            "\"q\\\"r\"",
            "{\"k\":1}",
            "[1,2]",
            "\"\"",
            "\"{x}\"",
        ];
        let gaps = ["", "", "", " ", "  "];
        let mut agreed = 0;
        for _ in 0..30_000 {
            let row = |n: usize, mutate: Option<(usize, u64)>| -> String {
                let mut out = String::from("{");
                for i in 0..n {
                    let mut name = names[(i + 3 * (n % 2)) % names.len()].to_string();
                    let mut value = values[(i * 7 + n) % values.len()].to_string();
                    if let Some((at, how)) = mutate
                        && at == i
                    {
                        match how % 4 {
                            0 => value = values[(how / 4) as usize % values.len()].to_string(),
                            1 => name = format!("{name}z"),
                            2 => {
                                value = format!("{value}{}", gaps[(how / 4) as usize % gaps.len()])
                            }
                            _ => {
                                if i > 0 {
                                    continue;
                                }
                            }
                        }
                    }
                    if i > 0 {
                        out.push(',');
                    }
                    let g = gaps[(i + n) % gaps.len()];
                    out.push_str(&format!("{g}\"{name}\"{g}:{g}{value}"));
                }
                out.push_str("}\n");
                out
            };
            let n = (next() % 8) as usize + 1;
            let a_row = row(n, None);
            let b_row = if next() % 5 == 0 {
                row(n + (next() % 2) as usize, None)
            } else {
                row(n, Some(((next() % n as u64) as usize, next())))
            };
            // Slabs with room for whole registers past the rows.
            let mut a = a_row.clone().into_bytes();
            a.extend_from_slice(&[b' '; 80]);
            let mut b = b"{\"pad\":0}\n".to_vec();
            let b_lo = b.len();
            b.extend_from_slice(b_row.as_bytes());
            let b_hi = b.len();
            b.extend_from_slice(&[b' '; 80]);
            let walk = json_rows_match_with(Swar, &p, &a, 0, a_row.len(), &b, b_lo, b_hi);
            // Safety: `w` proves AVX2.
            let counted =
                unsafe { json_rows_match_wide(w, &p, &a, 0, a_row.len(), &b, b_lo, b_hi) };
            assert_eq!(counted, walk, "A {a_row:?} B {b_row:?}");
            agreed += usize::from(walk);
        }
        assert!(
            agreed > 1000,
            "too few rows proved equal to test anything: {agreed}"
        );
    }

    /// The JSON proof holds only where the two rows parse to the same
    /// tracked values, with names spelled plainly and with escapes: a tracked
    /// name is tracked however it is written, and an untracked one is passed
    /// over however it is written. Rows differ in a value, a name's
    /// spelling, a member left out or a tracked name repeated at the end.
    #[test]
    fn the_json_proof_holds_only_where_the_parsed_rows_agree() {
        use super::super::slab::same_bytes;
        let p = RowParser::json(
            ["id", "amount", "not\u{e9}"]
                .iter()
                .map(|w| Some(w.to_string()))
                .collect(),
            1,
            false,
        );
        // Each member's spellings: tracked ones first, then two untracked.
        let spellings: [&[&str]; 5] = [
            &["id", "\\u0069d"],
            &["amount", "\\u0061mount", "am\\u006Fu\\u006et"],
            &["not\u{e9}", "not\\u00e9", "not\\u00E9"],
            &["updated_at", "upd\\u0061ted_at"],
            &["x\\/y", "x/y"],
        ];
        let values = [
            "\"a\"",
            "\"b\"",
            "1",
            "\"caf\\u00e9\"",
            "\"caf\u{e9}\"",
            "null",
        ];
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = move |n: usize| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            (seed % n as u64) as usize
        };
        let text = |members: &[(usize, usize, usize)]| {
            let body: Vec<String> = members
                .iter()
                .map(|&(m, sp, v)| format!("\"{}\":{}", spellings[m][sp], values[v]))
                .collect();
            format!("{{{}}}\n", body.join(","))
        };
        let (mut proved, mut proved_escaped) = (0, 0);
        for _ in 0..20_000 {
            let mut order = [0usize, 1, 2, 3, 4];
            for i in (1..order.len()).rev() {
                order.swap(i, next(i + 1));
            }
            let a_members: Vec<(usize, usize, usize)> = order[..2 + next(4)]
                .iter()
                .map(|&m| (m, next(spellings[m].len()), next(values.len())))
                .collect();
            let mut b_members = a_members.clone();
            let i = next(b_members.len());
            match next(5) {
                0 => b_members[i].2 = next(values.len()),
                1 => b_members[i].1 = next(spellings[b_members[i].0].len()),
                2 => {
                    b_members.remove(i);
                }
                3 => b_members.push((next(3), next(2), next(values.len()))),
                _ => {}
            }
            let (a_row, b_row) = (text(&a_members), text(&b_members));
            let mut a = a_row.clone().into_bytes();
            a.extend_from_slice(&[b' '; 80]);
            let mut b = b"{\"pad\":0}\n".to_vec();
            let b_lo = b.len();
            b.extend_from_slice(b_row.as_bytes());
            let b_hi = b.len();
            b.extend_from_slice(&[b' '; 80]);
            let holds = json_rows_match_with(Swar, &p, &a, 0, a_row.len(), &b, b_lo, b_hi);
            if let Some(w) = Wide::detect() {
                // Safety: `w` proves AVX2, or NEON on aarch64.
                let counted =
                    unsafe { json_rows_match_wide(w, &p, &a, 0, a_row.len(), &b, b_lo, b_hi) };
                assert_eq!(counted, holds, "A {a_row:?} B {b_row:?}");
            }
            if !holds {
                continue;
            }
            let (sa, sb) = (
                Slab::owned(a.clone(), Dialect::Json),
                Slab::owned(b.clone(), Dialect::Json),
            );
            let (mut fa, mut fb) = ([ABSENT; 3], [ABSENT; 3]);
            p.parse(sa.data(), 0, a_row.len(), &mut fa);
            p.parse(sb.data(), b_lo, b_hi, &mut fb);
            for k in 0..3 {
                assert!(
                    same_bytes(&sa, fa[k], &sb, fb[k]),
                    "proved, but slot {k} differs: A {a_row:?} B {b_row:?}"
                );
            }
            proved += 1;
            if a_row.contains('\\') {
                proved_escaped += 1;
            }
        }
        assert!(
            proved > 2000 && proved_escaped > 1000,
            "too few rows proved to test anything: {proved}, {proved_escaped} with an escape"
        );
    }

    /// Where the field walk ends a row, a byte at a time: a quote opens a
    /// quoted field only as a field's first byte, a doubled quote inside one
    /// is content, and after the closing quote the field runs on to its
    /// delimiter. The reference the scans are held to.
    fn row_end_by_bytes(data: &[u8], pos: usize, end: usize, delimiter: u8) -> usize {
        let end = end.min(data.len());
        let mut at = pos;
        loop {
            if at < end && data[at] == b'"' {
                at += 1;
                loop {
                    if at >= end {
                        return end;
                    }
                    if data[at] != b'"' {
                        at += 1;
                    } else if at + 1 < end && data[at + 1] == b'"' {
                        at += 2;
                    } else {
                        at += 1;
                        break;
                    }
                }
            }
            while at < end && data[at] != delimiter && data[at] != b'\n' {
                at += 1;
            }
            if at >= end || data[at] == b'\n' {
                return at;
            }
            at += 1;
        }
    }

    /// The row end is the field walk's on rows of every shape, at both
    /// widths: quoted fields, doubled quotes, and quotes that are content
    /// because they open no field (`5" nail`, `12"`, `"ab"c"d`).
    #[test]
    fn end_of_row_agrees_with_the_field_walk() {
        let wide = Wide::detect();
        let mut seed = 0x7a3c_91d2_55e1_0b4fu64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabets: [&[u8]; 3] = [b"ab,\"\"\n\rxyz", b"a,,\"\"\"\n", b"abcdefg,\t\"\n"];
        let mut literal = 0usize;
        for round in 0..60_000 {
            let alphabet = alphabets[round % alphabets.len()];
            let len = (next() % 260) as usize + 1;
            let mut data: Vec<u8> = (0..len)
                .map(|_| alphabet[(next() % alphabet.len() as u64) as usize])
                .collect();
            // Some rows as files have them: few quotes and few newlines.
            if next() % 3 == 0 {
                for b in data.iter_mut() {
                    if (*b == b'"' || *b == b'\n') && next() % 6 != 0 {
                        *b = b'q';
                    }
                }
            }
            let from = (next() % len as u64) as usize;
            let end = from + (next() % (len - from + 1) as u64) as usize;
            for delimiter in *b",\t" {
                let want = row_end_by_bytes(&data, from, end, delimiter);
                assert_eq!(
                    end_of_row(Swar, &data, from, end, delimiter),
                    want,
                    "eight bytes at a time: {:?} from {from} end {end}",
                    String::from_utf8_lossy(&data)
                );
                if let Some(w) = wide {
                    assert_eq!(
                        end_of_row_counted(w, &data, from, end, delimiter),
                        want,
                        "a register at a time: {:?} from {from} end {end}",
                        String::from_utf8_lossy(&data)
                    );
                }
                // The walk from quote to quote, which the scans used to be:
                // where it would have ended the row elsewhere, a quote that
                // is content decided it.
                let mut at = from;
                while at < end {
                    let stop = next_of2_with(Swar, &data, at, end, b'\n', b'"');
                    if stop >= end || data[stop] == b'\n' {
                        at = stop;
                        break;
                    }
                    at = skip_quoted_with(Swar, &data, stop + 1, end);
                }
                literal += usize::from(at.min(end) != want);
            }
        }
        assert!(
            literal > 1000,
            "too few rows with a quote that is content to test anything: {literal}"
        );
    }

    /// A lone quote in an unquoted field is content, and the rows after it
    /// are rows: counted as a quote that opens, it took every row up to the
    /// next one like it as its own.
    #[test]
    fn a_quote_inside_an_unquoted_field_does_not_join_rows() {
        let data = b"1,5\" nail,a\n2,ok,b\n3,3\" bolt,c\n4,\"x\ny\",d\n5,12\",e\n6,\"ab\"c\"d,f\n";
        let mut ends = Vec::new();
        let mut pos = 0;
        while pos < data.len() {
            let end = end_of_row(Swar, data, pos, data.len(), b',');
            if let Some(w) = Wide::detect() {
                assert_eq!(end_of_row_counted(w, data, pos, data.len(), b','), end);
            }
            ends.push(end);
            pos = end + 1;
        }
        // Every newline but the one inside `"x\ny"`.
        assert_eq!(ends, [11, 18, 30, 40, 48, 60]);
    }

    /// The register count agrees with the field walk on rows of every shape:
    /// quoted fields, stray quotes, CRLF, short rows, and an end inside a row.
    #[test]
    fn guard_span_counted_agrees_with_the_field_walk() {
        let Some(w) = Wide::detect() else {
            return;
        };
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let alphabet = b"abc,,,\"\n\r xyz0123";
        for _ in 0..20_000 {
            let len = (next() % 300) as usize + 1;
            let mut data: Vec<u8> = (0..len)
                .map(|_| alphabet[(next() % alphabet.len() as u64) as usize])
                .collect();
            // Most rows plain, as files are: quotes and newlines thinned out.
            if next() % 3 != 0 {
                for b in data.iter_mut() {
                    if (*b == b'"' || *b == b'\n') && next() % 8 != 0 {
                        *b = b'q';
                    }
                }
            }
            data.extend_from_slice(&[b'\n'; 70]);
            let lo = (next() % len as u64) as usize;
            let hi = lo + (next() % (len - lo + 40) as u64) as usize;
            let hi = hi.min(data.len());
            let commas = (next() % 25) as usize;
            for delimiter in *b",\t" {
                let walk = guard_span_with(Swar, &data, lo, hi, delimiter, commas);
                // Safety: `w` proves AVX2.
                let counted = unsafe { guard_span_wide(w, &data, lo, hi, delimiter, commas) };
                assert_eq!(
                    counted,
                    walk,
                    "{:?} lo {lo} hi {hi} commas {commas}",
                    String::from_utf8_lossy(&data)
                );
            }
        }
    }

    /// Well-formed quoted rows -- doubled quotes, delimiters and newlines inside
    /// quotes -- are answered by the count, not handed back to the walk.
    #[test]
    fn guard_span_counted_answers_quoted_rows() {
        let Some(w) = Wide::detect() else {
            return;
        };
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..5_000 {
            let mut row = Vec::new();
            let fields = (next() % 20) as usize + 1;
            for f in 0..fields {
                if f > 0 {
                    row.push(b',');
                }
                let body: Vec<u8> = (0..next() % 12)
                    .map(|_| b"ab,\n\"x"[(next() % 6) as usize])
                    .collect();
                if body.iter().any(|b| matches!(b, b',' | b'\n' | b'"')) || next() % 2 == 0 {
                    row.push(b'"');
                    for &b in &body {
                        row.push(b);
                        if b == b'"' {
                            row.push(b'"');
                        }
                    }
                    row.push(b'"');
                } else {
                    row.extend_from_slice(&body);
                }
            }
            row.push(b'\n');
            row.extend_from_slice(&[b'z'; 70]);
            let hi = row.len() - 70;
            let commas = (next() % 22) as usize + 1;
            let walk = guard_span_with(Swar, &row, 0, hi, b',', commas);
            assert_eq!(
                guard_span_counted(w, &row, 0, hi, b',', commas),
                Some(walk),
                "{:?} commas {commas}",
                String::from_utf8_lossy(&row)
            );
        }
    }

    fn json_slab(text: &str) -> Slab {
        let mut s = Slab::owned(text.as_bytes().to_vec(), Dialect::Json);
        s.set_dialect(Dialect::Json);
        s
    }

    #[test]
    fn a_brace_is_json_and_anything_else_is_csv() {
        assert_eq!(sniff_dialect(b"  {\"a\":1}"), Dialect::Json);
        assert_eq!(sniff_dialect(b"a,b,c\n"), Dialect::Csv);
        assert_eq!(sniff_dialect(b""), Dialect::Csv);
    }

    #[test]
    fn a_bracket_is_json_too() {
        assert_eq!(sniff_dialect(b"[{\"a\":1}]"), Dialect::Json);
        assert_eq!(sniff_dialect(b"\xEF\xBB\xBF [ {\"a\":1} ]"), Dialect::Json);
        assert_eq!(sniff_dialect(b"[]"), Dialect::Json);
    }

    #[test]
    fn an_array_s_header_is_its_first_object_s_keys() {
        let slab = json_slab("[{\"b\":1,\"a\":\"x\"}]\n");
        let names = json_header(&slab, Path::new("x")).expect("a header");
        assert_eq!(names, ["b", "a"]);
        // An empty array has no object to take the names from.
        let empty = json_slab("[]\n");
        assert!(json_header(&empty, Path::new("x")).is_err());
    }

    /// Every row's key, walked as the sweep walks: one parse call per row, a
    /// missing key marked -- a bracket read as a row would show up here.
    fn array_keys(text: &str) -> Vec<String> {
        let slab = json_slab(text);
        let parser = RowParser::json(vec![Some("k".into())], 1, true);
        let mut out = vec![ABSENT; 1];
        let mut rows = Vec::new();
        let mut at = 0;
        while at < text.len() {
            let next = parser.parse(slab.data(), at, text.len(), &mut out);
            assert!(next > at, "a row parse always advances");
            at = next;
            rows.push(if out[0] == ABSENT {
                "<none>".to_string()
            } else {
                String::from_utf8(slab.raw(out[0]).to_vec()).expect("a key")
            });
        }
        rows
    }

    #[test]
    fn rows_are_framed_by_braces_unless_the_first_object_keeps_to_its_line() {
        // Read a line at a time: the first object closes on its line, alone.
        for lines in [
            "{\"a\":1}\n{\n\"a\":2}\n",
            "\u{feff}  {\"a\":{\"b\":[1,2]}}  \r\n",
            "{\"a\":\"} {\"}\n",
            "{\"a\":1}",
            "",
            "id,v\n1,2\n",
        ] {
            assert!(!json_framed(lines.as_bytes()), "{lines:?}");
        }
        // Framed by braces: an array, an object over lines, two on a line.
        for framed in [
            "[{\"a\":1}]",
            "\n [\n{\"a\":1}\n]",
            "{\n  \"a\": 1\n}\n{\n  \"a\": 2\n}\n",
            "{\"a\":[\n1]}\n",
            "{\"a\":1} {\"a\":2}\n",
            "{\"a\":1}{\"a\":2}",
        ] {
            assert!(json_framed(framed.as_bytes()), "{framed:?}");
        }
        assert_eq!(json_row_indent(b"{\n  \"a\": 1\n}\n"), Some(0));
        assert_eq!(json_row_indent(b"[\n  {\"a\": 1}\n]"), Some(2));
        assert_eq!(json_row_indent(b"[{\"a\": 1}]"), None);
    }

    #[test]
    fn an_array_of_objects_one_per_line_reads_like_ndjson() {
        let ndjson = "{\"k\":\"a\",\"v\":1}\n{\"k\":\"b\",\"v\":2}\n";
        let array = "[\n{\"k\":\"a\",\"v\":1},\n{\"k\":\"b\",\"v\":2}\n]\n";
        assert_eq!(array_keys(ndjson), ["a", "b"]);
        assert_eq!(array_keys(array), array_keys(ndjson));
    }

    #[test]
    fn a_compact_array_splits_on_the_object_separator() {
        let array = "[{\"k\":\"a\"},{\"k\":\"b\"}, {\"k\":\"c\"}]";
        assert_eq!(array_keys(array), ["a", "b", "c"]);
    }

    #[test]
    fn a_pretty_printed_array_reads_one_object_across_lines() {
        let pretty =
            "[\n  {\n    \"k\": \"a\",\n    \"v\": 1\n  },\n  {\n    \"k\": \"b\"\n  }\n]\n";
        assert_eq!(array_keys(pretty), ["a", "b"]);
    }

    #[test]
    fn a_split_of_an_array_starts_at_a_row_not_a_nested_object() {
        // Row 0 holds a nested object on a line of its own, and a string with
        // braces and an escaped quote; row 1 opens on the line after.
        let text = "[\n  {\n    \"k\": \"{ \\\" }\",\n    \"m\": {\n      \"x\": 1\n    }\n  },\n  {\n    \"k\": \"b\"\n  }\n]\n";
        let data = text.as_bytes();
        let nested = text.find("{\n      \"x\"").expect("the nested object");
        let row1 = text.rfind("  {\n    \"k\": \"b\"").expect("row 1") + 2;
        // A split before row 0 starts there; anywhere in row 0, the nested
        // object's line included, at row 1; one past row 1, nowhere.
        assert_eq!(json_array_row_starts(data, 0, &[3]), [4]);
        for split in [5, nested - 1, nested, nested + 1, row1] {
            assert_eq!(
                json_array_row_starts(data, 0, &[split]),
                [row1],
                "split {split}"
            );
        }
        assert!(json_array_row_starts(data, 0, &[row1 + 1]).is_empty());
        // Two splits on the same row give it once.
        assert_eq!(
            json_array_row_starts(data, 0, &[nested, nested + 2]),
            [row1]
        );
        assert_eq!(json_row_indent(data), Some(2));
        assert_eq!(json_row_indent(b"[{\"k\":1}]"), None);
        // A stream's rows start at their line's start; only a file framed by
        // braces asks (newline-delimited JSON has a row a line).
        assert_eq!(json_row_indent(b"{\"k\":1}\n"), Some(0));
    }

    #[test]
    fn an_array_with_nested_values_and_escaped_braces_reads_alike() {
        // Nested values are not cells, exactly as in ndjson, and braces inside
        // a string are not the object's end.
        let ndjson =
            "{\"k\":\"a } {\",\"v\":{\"n\":[1,{\"x\":\"}\\\"\"}]}}\n{\"k\":\"b\",\"v\":2}\n";
        let array =
            "[{\"k\":\"a } {\",\"v\":{\"n\":[1,{\"x\":\"}\\\"\"}]}},\n{\"k\":\"b\",\"v\":2}]";
        assert_eq!(array_keys(ndjson), ["a } {", "b"]);
        assert_eq!(array_keys(array), array_keys(ndjson));
    }

    #[test]
    fn the_key_only_and_full_parses_agree_across_an_array() {
        // The early exit leaves the object unfinished; both parses must still
        // find the same rows and the same keys.
        let array = "[\n  {\n    \"k\": \"a\",\n    \"v\": \"w x\",\n    \"n\": {\"deep\": [1, 2]}\n  },\n  {\n    \"k\": \"b\"\n  }\n]\n";
        let slab = json_slab(array);
        let full = RowParser::json(vec![Some("k".into()), Some("v".into())], 1, true);
        let keys = RowParser::json(vec![Some("k".into())], 1, true);
        let mut out = vec![ABSENT; 2];
        let next = full.parse(slab.data(), 0, array.len(), &mut out);
        assert_eq!(slab.raw(out[0]), b"a");
        assert_eq!(slab.raw(out[1]), b"w x");
        let mut key = vec![ABSENT; 1];
        assert_eq!(keys.parse(slab.data(), 0, array.len(), &mut key), next);
        assert_eq!(slab.raw(key[0]), b"a");
        let last = keys.parse(slab.data(), next, array.len(), &mut key);
        assert_eq!(slab.raw(key[0]), b"b");
        assert_eq!(last, array.len());
    }

    #[test]
    fn the_header_is_the_first_object_s_keys_in_order() {
        let slab = json_slab("{\"b\":1,\"a\":\"x\",\"n\":null}\n");
        let names = json_header(&slab, Path::new("x")).expect("a header");
        assert_eq!(names, ["b", "a", "n"]);
    }

    #[test]
    fn values_are_read_by_key_whatever_order_they_come_in() {
        let text = "{\"k\":\"1\",\"v\":\"a\"}\n{\"v\":\"b\",\"k\":\"2\"}\n";
        let slab = json_slab(text);
        let parser = RowParser::json(vec![Some("k".into()), Some("v".into())], 0, false);
        let mut out = vec![ABSENT; 2];
        let next = parser.parse(slab.data(), 0, text.len(), &mut out);
        assert_eq!(slab.raw(out[0]), b"1");
        assert_eq!(slab.raw(out[1]), b"a");
        parser.parse(slab.data(), next, text.len(), &mut out);
        assert_eq!(slab.raw(out[0]), b"2");
        assert_eq!(slab.raw(out[1]), b"b");
    }

    #[test]
    fn a_name_is_read_as_what_its_escapes_spell() {
        // How writers spell names: Python's json.dumps escapes every
        // non-ASCII character, one past U+FFFF as a surrogate pair; PHP
        // escapes `/`. A name too long for the lookup's stack buffer, and an
        // escaped lone surrogate, which the header names lossily. A name
        // holding the six bytes `\u00e9` itself (`\\u00e9` as written) is a
        // column of its own, and a name that spells it with an escape does
        // not land there.
        let long = "\u{e9}".repeat(100);
        let text = format!(
            "{{\"caf\\u00e9\":\"1\",\"a\\/b\":\"2\",\"\\ud83d\\ude00\":\"3\",\"q\\\"\":\"4\",\
             \"{}\":\"5\",\"caf\\\\u00e9\":\"6\",\"\\ud800\":\"7\",\"plain\":\"8\"}}\n\
             {{\"caf\u{e9}\":\"9\",\"caf\\u00E9\":\"10\",\"plain\":\"11\"}}\n",
            "\\u00e9".repeat(100)
        );
        let slab = json_slab(&text);
        let names = json_header(&slab, Path::new("x")).expect("a header");
        let lone = String::from_utf8_lossy(b"\xed\xa0\x80").into_owned();
        assert_eq!(
            names,
            [
                "caf\u{e9}",
                "a/b",
                "\u{1f600}",
                "q\"",
                &long,
                "caf\\u00e9",
                &lone,
                "plain"
            ]
        );
        for key_size in [0, 1] {
            let parser =
                RowParser::json(names.iter().cloned().map(Some).collect(), key_size, false);
            let mut out = vec![ABSENT; names.len()];
            let next = parser.parse(slab.data(), 0, text.len(), &mut out);
            let got: Vec<&[u8]> = out.iter().map(|&f| slab.raw(f)).collect();
            assert_eq!(got, [b"1", b"2", b"3", b"4", b"5", b"6", b"7", b"8"]);
            // The same name raw and then escaped: the escaped one is the
            // last value, and the literal `\u00e9` column has none.
            parser.parse(slab.data(), next, text.len(), &mut out);
            let got: Vec<&[u8]> = out.iter().map(|&f| slab.raw(f)).collect();
            let none: &[u8] = b"";
            let want: [&[u8]; 8] = if key_size == 1 {
                [b"9", none, none, none, none, none, none, b"11"]
            } else {
                [b"10", none, none, none, none, none, none, b"11"]
            };
            assert_eq!(got, want, "key size {key_size}");
        }
    }

    #[test]
    fn null_and_a_missing_key_are_absent_and_a_nested_value_is_its_text() {
        let text = "{\"k\":\"1\",\"v\":null,\"w\":{\"deep\":[1,\"]}\"]},\"x\":[1, 2],\"y\":3}\n";
        let slab = json_slab(text);
        let parser = RowParser::json(
            vec![
                Some("k".into()),
                Some("v".into()),
                Some("w".into()),
                Some("x".into()),
                Some("missing".into()),
                Some("y".into()),
            ],
            0,
            false,
        );
        let mut out = vec![0; 6];
        let next = parser.parse(slab.data(), 0, text.len(), &mut out);
        assert_eq!(slab.raw(out[0]), b"1");
        assert_eq!([out[1], out[4]], [ABSENT; 2]);
        // A bracket inside a string does not close the object.
        assert_eq!(slab.raw(out[2]), b"{\"deep\":[1,\"]}\"]}");
        assert_eq!(slab.raw(out[3]), b"[1, 2]");
        assert_eq!(slab.raw(out[5]), b"3");
        assert_eq!(next, text.len());
    }

    #[test]
    fn a_newline_inside_a_string_does_not_end_the_row() {
        let text = "{\"k\":\"a\\nb\",\"v\":\"1\"}\n{\"k\":\"c\",\"v\":\"2\"}\n";
        let slab = json_slab(text);
        let parser = RowParser::json(vec![Some("k".into()), Some("v".into())], 0, false);
        let mut out = vec![ABSENT; 2];
        let next = parser.parse(slab.data(), 0, text.len(), &mut out);
        assert_eq!(slab.logical(out[0]).collect::<Vec<u8>>(), b"a\nb");
        parser.parse(slab.data(), next, text.len(), &mut out);
        assert_eq!(slab.raw(out[0]), b"c");
    }

    #[test]
    fn a_repeated_key_the_key_only_and_full_parses_agree() {
        // The first `k` is the row's key in both parses -- the key-only one
        // stops there -- while a repeated compared column keeps its last value.
        let text = "{\"k\":\"1\",\"v\":\"a\",\"k\":\"2\",\"v\":\"b\"}\n{\"k\":\"3\"}\n";
        let slab = json_slab(text);
        let full = RowParser::json(vec![Some("k".into()), Some("v".into())], 1, false);
        let keys = RowParser::json(vec![Some("k".into())], 1, false);
        let mut out = vec![ABSENT; 2];
        let next = full.parse(slab.data(), 0, text.len(), &mut out);
        assert_eq!(slab.raw(out[0]), b"1");
        assert_eq!(slab.raw(out[1]), b"b");
        let mut key = vec![ABSENT; 1];
        assert_eq!(keys.parse(slab.data(), 0, text.len(), &mut key), next);
        assert_eq!(slab.raw(key[0]), b"1");
        keys.parse(slab.data(), next, text.len(), &mut key);
        assert_eq!(slab.raw(key[0]), b"3");
    }

    /// A row parses to the same fields at either scan width. Only runs where
    /// the CPU has the vector width.
    #[test]
    fn both_widths_parse_alike() {
        let Some(w) = Wide::detect() else { return };
        let long = "x".repeat(70);
        let mut csv = format!(
            "id,name,note,amount\n1,alpha,\"a, \"\"quoted\"\" note\",10.5\n\
             2,{long},plain,7\n3,,\"\",\n4,\"{long},\n{long}\",z,1\n5,a,b\n"
        );
        // Field lengths that walk a delimiter through every byte of a vector
        // step, with a quoted field now and then.
        for i in 0..200usize {
            let field = |n: usize| "y".repeat(n % 41);
            let note = if i % 5 == 0 {
                format!("\"{},\"\"{}\"", field(i * 3), field(i))
            } else {
                field(i * 3)
            };
            csv += &format!("{i},{},{note},{}\n", field(i * 7), field(i * 13));
        }
        let json = format!(
            "{{\"id\":\"1\",\"name\":\"a\\\"b\",\"skip\":{{\"n\":[1,{{\"m\":\"}}\"}}]}},\"amount\":2}}\n\
             {{\"amount\":null,\"name\":\"{long}\",\"id\":\"2\",\"id\":\"3\"}}\n\
             {{\"id\":\"4\",\"note\":\"{long}\\n{long}\"}}\n"
        );
        let parsers = [
            (
                RowParser::csv(b',', vec![Some(0), Some(1), Some(2), Some(3)]),
                csv.as_bytes(),
            ),
            (
                RowParser::csv(b',', vec![Some(0), None, Some(2), Some(0)]),
                csv.as_bytes(),
            ),
            (
                RowParser::json(
                    vec![
                        Some("id".into()),
                        Some("name".into()),
                        Some("amount".into()),
                        Some("note".into()),
                    ],
                    1,
                    false,
                ),
                json.as_bytes(),
            ),
        ];
        for (parser, data) in &parsers {
            let n = 4;
            let mut at = 0;
            while at < data.len() {
                let mut a = vec![ABSENT; n];
                let mut b = vec![ABSENT; n];
                let next = parser.parse_with(Swar, data, at, data.len(), &mut a);
                assert_eq!(parser.parse_with(w, data, at, data.len(), &mut b), next);
                assert_eq!(a, b, "the row at {at}");
                assert!(next > at);
                at = next;
            }
        }
    }

    #[test]
    fn a_name_in_two_slots_is_found_where_the_table_would_find_it() {
        // `k` fills slot 0 and slot 2, as when a key column is also named as a
        // compared one. After `v` fills slot 1 the next member is guessed as
        // slot 2, and a right guess has to answer what the table does: slot 0.
        let text = "{\"k\":\"1\",\"v\":\"a\",\"k\":\"2\"}\n";
        let slab = json_slab(text);
        let parser = RowParser::json(
            vec![Some("k".into()), Some("v".into()), Some("k".into())],
            1,
            false,
        );
        let mut out = vec![ABSENT; 3];
        parser.parse(slab.data(), 0, text.len(), &mut out);
        assert_eq!(slab.raw(out[0]), b"1");
        assert_eq!(slab.raw(out[1]), b"a");
        assert_eq!(out[2], ABSENT);
    }
}
