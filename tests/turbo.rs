//! The traps specific to a byte-level engine.
//!
//! `compare.rs` runs turbo alongside every other engine, which covers the
//! ordinary path. What it does not probe is the two ways this design goes wrong
//! quietly, both of which the Java port shipped: a field reaching the hash by
//! one route and the comparison by another, and a key sitting close enough to
//! the end of the file that a wide read cannot be used for it.

use std::fs;
use std::path::PathBuf;

use compara::engine::compare;
use compara::options::{Engine, Options};

struct Fixture {
    dir: PathBuf,
    a: PathBuf,
    b: PathBuf,
}

impl Fixture {
    fn new(a_body: &str, b_body: &str) -> Self {
        let dir = std::env::temp_dir().join(format!(
            "compara-turbo-test-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).expect("temp dir");
        let a = dir.join("a.csv");
        let b = dir.join("b.csv");
        fs::write(&a, a_body).expect("write a");
        fs::write(&b, b_body).expect("write b");
        Fixture { dir, a, b }
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

/// Runs one comparison twice — once on turbo, once on the reference engine —
/// and holds them to the same answer.
fn agrees(a_body: &str, b_body: &str, key: &[&str], trim: bool, ignore_case: bool) {
    let f = Fixture::new(a_body, b_body);
    let mut results = Vec::new();
    for engine in [Engine::Native, Engine::Turbo] {
        let mut opt = Options::with_key(key.iter().copied());
        opt.engine = engine.label().to_string();
        opt.trim = trim;
        opt.ignore_case = ignore_case;
        let r = compare(&f.a, &f.b, &mut opt).expect("compare");
        results.push(
            serde_json::json!({
                "counts": r.counts, "columns": r.columns, "changed": r.changed,
                "added": r.added, "removed": r.removed, "dup_a": r.dup_a, "dup_b": r.dup_b,
            })
            .to_string(),
        );
    }
    assert_eq!(results[1], results[0], "turbo diverges from native");
}

/// A key in the last bytes of the file. The Java engine hashed such a key by a
/// different route than one in the middle, so the join missed the last row and
/// reported it as removed from one side and added to the other. Both files here
/// are well formed and hold exactly the keys K1 and K2; only the number of bytes
/// trailing the final key differs.
#[test]
fn a_key_near_the_end_of_the_file_still_matches() {
    agrees(
        "a,k,c\nx,K1,c1\ny,K2,cc\n",
        "a,k,c\nx,K1,c1\ny,K2,cccccccc\n",
        &["k"],
        false,
        false,
    );
}

/// The same, swept across the eight-byte boundary, so the engine cannot be right
/// for one trailing length and wrong for the next.
#[test]
fn the_key_position_in_the_file_never_changes_the_answer() {
    for trailing in 0..=12 {
        let tail = "c".repeat(trailing);
        agrees(
            "a,k,c\nx,K1,c1\ny,K2,z\n",
            &format!("a,k,c\nx,K1,c1\ny,K2,z{tail}\n"),
            &["k"],
            false,
            false,
        );
    }
}

/// Case folding outside ASCII can change a string's length, so an engine that
/// folds bytes for the hash and characters for the comparison puts the two
/// spellings in different buckets. U+212A KELVIN SIGN is three bytes and folds
/// to the one byte "k".
#[test]
fn ignore_case_folds_non_ascii_keys_consistently() {
    agrees(
        "k,v\nCAFÉ,x\nplain,y\n",
        "k,v\ncafé,z\nplain,y\n",
        &["k"],
        false,
        true,
    );
    agrees(
        "k,v\n\u{212a},x\nplain,y\n",
        "k,v\nk,z\nplain,y\n",
        &["k"],
        false,
        true,
    );
}

/// A doubled quote is the one value that is not a slice of the file. This engine
/// flags such a field and drops the second quote of each pair when the bytes are
/// read, so the value never exists anywhere as a contiguous run.
#[test]
fn doubled_quotes_are_unescaped_on_read() {
    agrees(
        "k,v,w\n1,\"a\"\"b\",\n2,\"has,comma\",x\n3,\"two\nlines\",y\n",
        "k,v,w\n1,\"a\"\"b\",z\n2,\"has,comma\",x\n3,\"two\nlines\",y\n",
        &["k"],
        false,
        false,
    );
    // And as a key, where it decides matching rather than just a cell value.
    agrees(
        "k,v\n\"a\"\"b\",one\nplain,two\n",
        "k,v\n\"a\"\"b\",CHANGED\nplain,two\n",
        &["k"],
        false,
        false,
    );
}

/// CRLF, a row shorter than the header, a row longer than it, and a trailing
/// blank line — the shapes a hand-written scanner gets wrong.
#[test]
fn awkward_row_shapes_match_the_reference() {
    agrees(
        "k,v,w\r\n1,x,y\r\n2,p,q\r\n",
        "k,v,w\r\n1,x,CHANGED\r\n2,p,q\r\n",
        &["k"],
        false,
        false,
    );
    agrees(
        "k,v,w\n1,x,y\n2,short\n",
        "k,v,w\n1,x,CHANGED\n2,short\n",
        &["k"],
        false,
        false,
    );
    agrees(
        "k,v,w\n1,x,y\n2,p,q,EXTRA\n",
        "k,v,w\n1,x,CHANGED\n2,p,q,EXTRA\n",
        &["k"],
        false,
        false,
    );
    agrees(
        "k,v,w\n1,x,y\n\n2,p,q\n",
        "k,v,w\n1,x,CHANGED\n\n2,p,q\n",
        &["k"],
        false,
        false,
    );
}

/// A duplicate key joins on its first occurrence, and the repeats are counted
/// rather than joined — the same rule as every other engine.
#[test]
fn duplicate_keys_join_on_the_first_occurrence() {
    agrees(
        "k,v\n1,first\n1,second\n2,b\n",
        "k,v\n1,first\n2,b\n",
        &["k"],
        false,
        false,
    );
}

/// An over-long field in a column the sweep no longer parses.
///
/// The sweep reads each row with a parser configured for the key columns alone,
/// so it never packs the compared ones and cannot see that one of them is past
/// what a field word holds. What catches it instead is arithmetic: a field
/// cannot be longer than the row around it, so a row over the cap is re-read in
/// full. This checks the refusal still happens, and that it happens for a
/// non-key column — the case the cheap check does not cover on its own.
#[test]
fn an_over_long_field_is_still_refused_when_it_is_not_a_key() {
    let big = "x".repeat((1 << 23) + 8); // one field past the twenty-three-bit length
    let a = format!("k,v\nK1,{big}\n");
    let b = format!("k,v\nK1,{big}\n");
    let f = Fixture::new(&a, &b);
    let mut opt = Options::with_key(["k"]);
    opt.engine = Engine::Turbo.label().to_string();
    let err = compare(&f.a, &f.b, &mut opt).expect_err("an over-long field must be refused");
    assert!(
        err.to_string().contains("more than this engine packs"),
        "unexpected error: {err}"
    );
}

/// The same row length, split so that no single field is over the cap. The
/// row-length guard fires here and must then find nothing, because re-reading a
/// long row is only a way to look, not a way to refuse.
#[test]
fn a_long_row_of_short_fields_is_fine() {
    let cell = "x".repeat(1 << 20);
    let row: Vec<String> = (0..12).map(|_| cell.clone()).collect();
    let head: Vec<String> = (0..12).map(|i| format!("c{i}")).collect();
    let body = format!("k,{}\nK1,{}\n", head.join(","), row.join(","));
    let f = Fixture::new(&body, &body);
    let mut opt = Options::with_key(["k"]);
    opt.engine = Engine::Turbo.label().to_string();
    let r = compare(&f.a, &f.b, &mut opt).expect("a long row of short fields is legal");
    assert_eq!(r.counts.matched, 1);
    assert_eq!(r.counts.changed, 0);
}

/// The same, with columns the comparison was told to skip.
fn agrees_ignoring(a_body: &str, b_body: &str, key: &[&str], ignore: &[&str]) {
    let f = Fixture::new(a_body, b_body);
    let mut results = Vec::new();
    for engine in [Engine::Native, Engine::Turbo] {
        let mut opt = Options::with_key(key.iter().copied());
        opt.engine = engine.label().to_string();
        opt.ignore = ignore.iter().map(|s| s.to_string()).collect();
        let r = compare(&f.a, &f.b, &mut opt).expect("compare");
        results.push(
            serde_json::json!({
                "counts": r.counts, "columns": r.columns, "changed": r.changed,
                "added": r.added, "removed": r.removed, "dup_a": r.dup_a, "dup_b": r.dup_b,
            })
            .to_string(),
        );
    }
    assert_eq!(results[1], results[0], "turbo diverges from native");
}

// ---------------------------------------------------------------------------
// The byte-span shortcut: a matched pair whose rows open with the same bytes is
// unchanged without either row being parsed. Everything below is a way for that
// to be true of the bytes and false of the data.
// ---------------------------------------------------------------------------

/// A prefix is not a column. `12,3` opens `12,34`, and a comparison that stopped
/// at the shorter run would call two different rows identical, so the run has to
/// end on a field boundary in both files.
#[test]
fn a_row_that_merely_opens_with_the_other_is_not_the_same_row() {
    agrees(
        "k,v\nK1,3\nK2,x\n",
        "k,v\nK1,34\nK2,x\n",
        &["k"],
        false,
        false,
    );
    // And the other way round, so it cannot be right in one direction only.
    agrees(
        "k,v\nK1,34\nK2,x\n",
        "k,v\nK1,3\nK2,x\n",
        &["k"],
        false,
        false,
    );
}

/// A quoted last column ends on its closing quote, not on the delimiter, so the
/// byte run derived from the field would stop one short and match a row that
/// carries different bytes after it. The shortcut has to decline these.
#[test]
fn a_quoted_last_column_does_not_shorten_the_run() {
    agrees(
        "k,v\nK1,\"a\"\nK2,x\n",
        "k,v\nK1,\"ab\"\nK2,x\n",
        &["k"],
        false,
        false,
    );
    agrees(
        "k,v\nK1,\"a,b\"\nK2,x\n",
        "k,v\nK1,\"a,c\"\nK2,x\n",
        &["k"],
        false,
        false,
    );
}

/// An ignored column inside the run is still inside it, so the bytes differ on a
/// pair that has not changed by the comparison's own definition. Reporting it as
/// changed would be wrong; the shortcut simply does not fire and the columns are
/// compared as before.
#[test]
fn a_column_being_ignored_does_not_make_its_bytes_matter() {
    agrees_ignoring(
        "k,skip,v\nK1,one,x\nK2,a,y\n",
        "k,skip,v\nK1,two,x\nK2,a,y\n",
        &["k"],
        &["skip"],
    );
}

/// Ignored and trailing, which is what the benchmark does: the run stops before
/// it, so rows differing only there take the shortcut and must still be counted
/// unchanged.
#[test]
fn an_ignored_trailing_column_is_outside_the_run() {
    agrees_ignoring(
        "k,v,ts\nK1,x,1\nK2,y,2\n",
        "k,v,ts\nK1,x,9\nK2,y,8\n",
        &["k"],
        &["ts"],
    );
}

/// Two files carrying the same bytes in a different order mean different things.
/// The shortcut is only allowed when both sides read the same columns from the
/// same positions.
#[test]
fn the_same_bytes_in_a_different_column_order_are_not_the_same_row() {
    agrees(
        "k,one,two\nK1,a,b\nK2,p,q\n",
        "k,two,one\nK1,a,b\nK2,p,q\n",
        &["k"],
        false,
        false,
    );
}

/// A row that stops before the last wanted column has no field to end the run
/// at, so there is nothing to compare bytes against.
#[test]
fn a_row_short_of_the_last_column_still_compares() {
    agrees(
        "k,v,w\nK1,x\nK2,y,z\n",
        "k,v,w\nK1,x,w1\nK2,y,z\n",
        &["k"],
        false,
        false,
    );
}

/// An empty last column ends the run at a delimiter with nothing before it,
/// which is a real field and a real boundary.
#[test]
fn an_empty_last_column_is_a_boundary_like_any_other() {
    agrees("k,v\nK1,\nK2,y\n", "k,v\nK1,\nK2,z\n", &["k"], false, false);
    agrees(
        "k,v\nK1,\nK2,y\n",
        "k,v\nK1,q\nK2,y\n",
        &["k"],
        false,
        false,
    );
}

/// The last row of a file ends at the end of the file rather than at a newline,
/// which is the other way a run can finish.
#[test]
fn a_final_row_without_a_newline_takes_the_run_to_the_end() {
    agrees("k,v\nK1,x\nK2,y", "k,v\nK1,x\nK2,z", &["k"], false, false);
    agrees("k,v\nK1,x\nK2,y", "k,v\nK1,x\nK2,y", &["k"], false, false);
}

/// Normalisation makes rows equal that are not byte-equal, never the reverse, so
/// the shortcut stays sound underneath it -- but only if the slow path still
/// runs for the pairs whose bytes differ.
#[test]
fn trimming_and_folding_still_decide_the_pairs_the_bytes_did_not() {
    agrees(
        "k,v\nK1, x \nK2,y\n",
        "k,v\nK1,x\nK2,y\n",
        &["k"],
        true,
        false,
    );
    agrees(
        "k,v\nK1,X\nK2,y\n",
        "k,v\nK1,x\nK2,y\n",
        &["k"],
        false,
        true,
    );
}

/// The case the run's own end has to be checked for, which a boundary in the
/// other file cannot catch. A's last column is quoted and holds a delimiter, so
/// the field ends on its closing quote; B carries the same bytes but never
/// closes the quote, and the byte where A's run stops is a newline in B. The
/// bytes line up and a boundary sits at the end of both, yet the rows differ.
///
/// Only the counts are held to the reference here. The input is malformed, and
/// the two engines already render an unterminated field's value differently --
/// turbo drops the trailing newline the reference keeps -- which is older than
/// this shortcut and not what the test is about. What matters is that the pair
/// is still counted as changed rather than waved through as identical bytes.
#[test]
fn a_run_ending_inside_a_quoted_field_is_not_a_run() {
    let f = Fixture::new("k,v\nK1,\"a,b\"\nK2,x\n", "k,v\nK1,\"a,b\nK2,x\n");
    let mut counts = Vec::new();
    for engine in [Engine::Native, Engine::Turbo] {
        let mut opt = Options::with_key(["k"]);
        opt.engine = engine.label().to_string();
        let r = compare(&f.a, &f.b, &mut opt).expect("compare");
        counts.push(serde_json::json!(r.counts).to_string());
    }
    assert_eq!(counts[1], counts[0], "turbo diverges from native");
    assert!(
        counts[0].contains("\"changed\":1"),
        "expected a changed pair"
    );
}

// ---------------------------------------------------------------------------
// Chunk boundaries. Nothing above reaches them: the sweep only splits a file
// past `CHUNKING_THRESHOLD`, which is 4 MB, and every fixture above is bytes.
// ---------------------------------------------------------------------------

/// Builds a file big enough for the sweep to split, whose rows are mostly
/// quoted field — so a split point has a good chance of landing inside one.
///
/// A newline inside a quoted field is not a row boundary, and a thread starting
/// mid-file cannot tell whether it is inside one except by what came before it.
/// Get that wrong and a row is cut in half: the counts move, which is what these
/// tests read.
fn quoted_bulk(rows: usize, marker: &str) -> String {
    let mut s = String::from("k,note,v\n");
    for i in 0..rows {
        // 200-odd bytes of quoted field, holding both a newline and a delimiter.
        s.push_str(&format!(
            "K{i},\"line one of {i}\nline two, with a comma\n{}\",{}\n",
            "padding, and more padding\n".repeat(6),
            if i == rows / 2 { marker } else { "same" }
        ));
    }
    s
}

/// The sweep's split points must land on real row boundaries in a file where
/// most bytes are inside quotes, and the answer must not depend on how many
/// threads looked for them.
#[test]
fn chunk_boundaries_land_between_rows_in_a_quoted_file() {
    let a = quoted_bulk(40_000, "same");
    let b = quoted_bulk(40_000, "different");
    assert!(
        a.len() > 8 << 20,
        "fixture must exceed the chunking threshold, got {} bytes",
        a.len()
    );
    let f = Fixture::new(&a, &b);

    let mut reference = None;
    for threads in [1usize, 2, 3, 4, 8] {
        let mut opt = Options::with_key(["k"]);
        opt.engine = Engine::Turbo.label().to_string();
        opt.threads = Some(threads);
        let r = compare(&f.a, &f.b, &mut opt).expect("compare");
        let got = serde_json::json!({"counts": r.counts, "columns": r.columns}).to_string();
        // One row differs, and every row must survive as one row.
        assert!(
            got.contains("\"changed\":1") && got.contains("\"a_rows\":40000"),
            "threads {threads}: rows were cut apart -- {got}"
        );
        match &reference {
            None => reference = Some(got),
            Some(first) => assert_eq!(&got, first, "threads {threads} disagrees"),
        }
    }
}

/// A split guessed at the next newline and wrong must not be able to fail the
/// run.
///
/// The one quoted field here is legal -- just under the packed field limit --
/// and holds no newline but its last byte, and it straddles the middle of the
/// file. So the guessed split lands on its closing quote. A chunk that starts
/// there reads that quote as the opening of a key field, finds no other quote
/// in the rest of the file, and calls everything to the end one field: over the
/// limit, which is an error. The guess is wrong, so the chunk before it says so
/// and the sweep runs again on counted bounds -- and the error, which belongs to
/// no row of the file, must never be reported.
#[test]
fn a_wrong_split_guess_cannot_fail_the_run() {
    let long = 7_900_000;
    let mut a = String::from("k,note,v\n");
    for i in 0..150_000 {
        a.push_str(&format!("P{i},short,1\n"));
    }
    a.push_str("K0,\"");
    a.push_str(&"a".repeat(long));
    a.push_str("\n\",x\n");
    // More than the field limit after the closing quote, and less than there
    // is before it, so the middle stays inside the field.
    let tail_from = a.len();
    while a.len() < tail_from + 8_600_000 {
        let i = a.len();
        a.push_str(&format!("Q{i},plain,1\n"));
    }
    let mid = a.len() / 2;
    let field = a.find("K0,\"").expect("the long row");
    assert!(
        field < mid && mid < field + long,
        "the long field must straddle the middle: {field}..{} against {mid}",
        field + long
    );
    let b = a.replacen("P7,short,1", "P7,short,2", 1);
    let f = Fixture::new(&a, &b);

    let mut reference = None;
    for threads in [1usize, 4] {
        let mut opt = Options::with_key(["k"]);
        opt.engine = Engine::Turbo.label().to_string();
        opt.threads = Some(threads);
        let r = compare(&f.a, &f.b, &mut opt).unwrap_or_else(|e| panic!("threads {threads}: {e}"));
        let got = serde_json::json!({"counts": r.counts, "columns": r.columns}).to_string();
        assert!(got.contains("\"changed\":1"), "threads {threads}: {got}");
        match &reference {
            None => reference = Some(got),
            Some(first) => assert_eq!(&got, first, "threads {threads} disagrees"),
        }
    }
}

/// A quote opens a quoted field only where a field starts. Anywhere else it
/// is content -- an inch mark, as in `5" nail` -- and the row ends at its own
/// newline. The row end was found by counting quotes, so each such quote took
/// every row up to the next one like it as part of its own: four rows read as
/// two, and a pair that differed in the swallowed rows compared as identical.
#[test]
fn a_quote_that_is_content_does_not_join_rows() {
    let a = "id,name,x\n1,5\" nail,a\n2,ok,b\n3,3\" bolt,c\n4,ok,d\n";
    let b = "id,name,x\n1,5\" nail,a\n2,CHANGED,b\n3,3\" bolt,c\n4,ok,d\n";
    agrees(a, b, &["id"], false, false);
    // With the column left out its fields are skipped, not walked.
    agrees_ignoring(a, &b.replace("ok,d", "ok,D"), &["id"], &["name"]);
    // A field that ends in one, and one in the key itself.
    agrees(
        "id,size,x\n1,12\",a\n2,3,b\n3,7\",c\n",
        "id,size,x\n1,12\",a\n2,4,b\n3,7\",c\n",
        &["id"],
        false,
        false,
    );
    agrees(
        "size,x\n12\",a\n3,b\n7\",c\n",
        "size,x\n12\",a\n3,B\n7\",c\n",
        &["size"],
        false,
        false,
    );

    let f = Fixture::new(a, b);
    let mut opt = Options::with_key(["id"]);
    opt.engine = Engine::Turbo.label().to_string();
    let r = compare(&f.a, &f.b, &mut opt).expect("compare");
    assert_eq!(
        (r.counts.a_rows, r.counts.b_rows, r.counts.changed),
        (4, 4, 1)
    );
}

/// Text between a field's closing quote and its delimiter is part of the
/// value, as the reference engine reads it: `"ab"cd` is `abcd`, and an export
/// that did not double its inner quotes keeps everything it wrote. Read as
/// the quoted part alone, two files that differed only after the closing
/// quote compared as identical.
#[test]
fn text_after_a_closing_quote_is_part_of_the_value() {
    agrees(
        "id,v\n1,\"ab\"cd\n2,b\n",
        "id,v\n1,\"ab\"xy\n2,b\n",
        &["id"],
        false,
        false,
    );
    agrees(
        "id,v\n1,\"ab\"cd\n2,b\n",
        "id,v\n1,abcd\n2,b\n",
        &["id"],
        false,
        false,
    );
    let a = "id,name,x\n1,\"John \"Johnny\" Smith\",a\n2,\"5\" nail\",b\n3,\"ok\",c\n";
    let b = "id,name,x\n1,\"John \"Jack\" Smith\",a\n2,\"5\" bolt\",b\n3,\"ok\",c\n";
    agrees(a, b, &["id"], false, false);
    agrees(a, b, &["id"], true, true);
    // In the key, with CRLF, and as the row's last field.
    agrees(
        "k,v\r\n\"a\"1,x\r\n\"a\"2,y\r\n3,\"p\"q\r\n",
        "k,v\r\n\"a\"1,x\r\n\"a\"2,Y\r\n3,\"p\"r\r\n",
        &["k"],
        false,
        false,
    );

    let f = Fixture::new(a, b);
    let mut opt = Options::with_key(["id"]);
    opt.engine = Engine::Turbo.label().to_string();
    let r = compare(&f.a, &f.b, &mut opt).expect("compare");
    assert_eq!((r.counts.a_rows, r.counts.changed), (3, 2));
    let row = serde_json::to_string(&r.changed.rows[0]).expect("a row");
    assert_eq!(
        row,
        r#"["1",[[0,"John Johnny\" Smith\"","John Jack\" Smith\""]]]"#
    );
}

/// A quoted field the file ends in -- its quote never closed -- is everything
/// after the opening quote, the last byte and a final newline included. The
/// last byte was taken for the closing quote and dropped.
#[test]
fn a_quoted_field_the_file_ends_in_is_read_whole() {
    for tail in ["", "\n", "\r\n", "\"\"", "\"\"\n"] {
        agrees(
            &format!("id,v\n1,a\n2,\"open{tail}"),
            &format!("id,v\n1,a\n2,\"open{tail}"),
            &["id"],
            false,
            false,
        );
        agrees(
            &format!("id,v\n1,a\n2,\"open{tail}"),
            "id,v\n1,a\n2,open\n",
            &["id"],
            false,
            false,
        );
    }
    // The same bytes in the other file, where the field runs on: equal bytes
    // up to the end of the first file say nothing about the rest.
    agrees(
        "id,v,w\n1,a,\"",
        "id,v,w\n1,a,\"\n2,b,c\n",
        &["id"],
        false,
        false,
    );
    agrees(
        "id,v,w\n1,a,\"x",
        "id,v,w\n1,a,\"x\n2,b,c\n",
        &["id"],
        false,
        false,
    );
}

/// The counted split is by quote parity, and one quote that is content puts
/// the count off by one for the rest of the file. Here the guessed split is
/// wrong -- the middle of the file is inside a quoted field of many lines --
/// and the count agrees with the guess, having met an inch mark and the
/// field's opening quote. Those bounds went unchecked, and the lines of the
/// field past the split were read as rows: 203,217 of a 200,000-row file at
/// four threads.
#[test]
fn counted_bounds_are_checked_when_a_quote_is_content() {
    let rows = 150_000;
    let note: String = (0..12_000)
        .map(|k| format!("line {k} of the note\n"))
        .collect();
    let build = |changed: &str| {
        let mut s = String::from("k,note,v\n");
        s.push_str("K0,5\" nail,1\n");
        let mut placed = false;
        for i in 1..rows {
            // The note goes where it will sit across the middle of the file.
            if !placed && s.len() + note.len() / 2 >= 3 << 20 {
                s.push_str(&format!("N{i},\"{note}\",1\n"));
                placed = true;
            } else {
                let v = if i == rows - 5 { changed } else { "1" };
                s.push_str(&format!("P{i},an ordinary row of text to pad,{v}\n"));
            }
        }
        while s.len() < 6 << 20 {
            let i = s.len();
            s.push_str(&format!("Q{i},an ordinary row of text to pad,1\n"));
        }
        s
    };
    let (a, b) = (build("1"), build("2"));
    let field = a.find(",\"line 0 of").expect("the note") + 2;
    let mid = a.len() / 2;
    assert!(
        field < mid && mid < field + note.len(),
        "the note must straddle the middle: {field}..{} against {mid}",
        field + note.len()
    );
    let want = a.matches("\nK").count()
        + a.matches("\nN").count()
        + a.matches("\nP").count()
        + a.matches("\nQ").count();
    let f = Fixture::new(&a, &b);

    for threads in [1usize, 4, 8, 16] {
        let mut opt = Options::with_key(["k"]);
        opt.engine = Engine::Turbo.label().to_string();
        opt.threads = Some(threads);
        let r = compare(&f.a, &f.b, &mut opt).unwrap_or_else(|e| panic!("threads {threads}: {e}"));
        assert_eq!(
            (r.counts.a_rows, r.counts.b_rows, r.counts.changed),
            (want as i64, want as i64, 1),
            "threads {threads}"
        );
    }
}

/// One pair of newline-delimited JSON files, since `Fixture` writes `.csv`.
fn json_fixture(a_body: &str, b_body: &str) -> Fixture {
    let dir = std::env::temp_dir().join(format!(
        "compara-json-proof-{}-{}",
        std::process::id(),
        COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    fs::create_dir_all(&dir).expect("temp dir");
    let a = dir.join("a.ndjson");
    let b = dir.join("b.ndjson");
    fs::write(&a, a_body).expect("write a");
    fs::write(&b, b_body).expect("write b");
    Fixture { dir, a, b }
}

/// A pretty-printed top-level JSON array, `indent` spaces a level, of rows
/// with an object and an array of objects nested in them: every nested `{`
/// opens a line of its own, as a row's does. `odd` gives row `i` a different
/// nested value when it is a multiple of 1,000.
fn nested_array(rows: usize, indent: usize, odd: bool) -> String {
    let pad = |level: usize| " ".repeat(level * indent);
    let mut s = String::from("[\n");
    for i in 0..rows {
        let c = if odd && i % 1000 == 0 { 9 } else { i % 3 };
        s.push_str(&format!(
            "{p1}{{\n{p2}\"id\": {i},\n{p2}\"meta\": {{\n{p3}\"a\": {a},\n{p3}\"b\": [\n\
             {p4}1,\n{p4}{{\n{p5}\"c\": {c}\n{p4}}}\n{p3}]\n{p2}}},\n{p2}\"v\": {v}\n{p1}}}{sep}\n",
            p1 = pad(1),
            p2 = pad(2),
            p3 = pad(3),
            p4 = pad(4),
            p5 = pad(5),
            a = i % 7,
            v = i * 3,
            sep = if i + 1 < rows { "," } else { "" },
        ));
    }
    s.push_str("]\n");
    s
}

/// A split of a pretty-printed JSON array must land on a row, not on an object
/// nested in one. Guessed at the next line opening an object, a chunk could
/// start inside a row, and the rows it then misread were dropped: 200,000 rows
/// counted as 100,718 at four threads, with nothing said. Indented, the rows'
/// own column tells them apart; with no indentation every object opens at
/// column 0, the guess is wrong, and the check behind it has to catch that.
#[test]
fn chunk_boundaries_land_between_rows_of_a_pretty_json_array() {
    for indent in [2usize, 0] {
        let rows = 120_000;
        let a = nested_array(rows, indent, false);
        let b = nested_array(rows, indent, true);
        assert!(
            a.len() > 8 << 20,
            "fixture must exceed the chunking threshold, got {} bytes",
            a.len()
        );
        let f = json_fixture(&a, &b);
        for threads in [1usize, 2, 3, 4, 8] {
            let mut opt = Options::with_key(["id"]);
            opt.threads = Some(threads);
            let r = compare(&f.a, &f.b, &mut opt).expect("compare");
            let c = &r.counts;
            assert_eq!(
                (c.a_rows, c.b_rows, c.changed, c.added, c.removed),
                (rows as i64, rows as i64, 120, 0, 0),
                "indent {indent}, threads {threads}: {}",
                serde_json::json!(r.counts)
            );
        }
    }
}

/// Rows as `jq .` writes them: one pretty-printed object after another, with
/// no array around them, the same rows as [`nested_array`].
fn nested_stream(rows: usize, indent: usize, odd: bool) -> String {
    let pad = |level: usize| " ".repeat(level * indent);
    let mut s = String::new();
    for i in 0..rows {
        let c = if odd && i % 1000 == 0 { 9 } else { i % 3 };
        s.push_str(&format!(
            "{{\n{p1}\"id\": {i},\n{p1}\"meta\": {{\n{p2}\"a\": {a},\n{p2}\"b\": [\n\
             {p3}1,\n{p3}{{\n{p4}\"c\": {c}\n{p3}}}\n{p2}]\n{p1}}},\n{p1}\"v\": {v}\n}}\n",
            p1 = pad(1),
            p2 = pad(2),
            p3 = pad(3),
            p4 = pad(4),
            a = i % 7,
            v = i * 3,
        ));
    }
    s
}

/// A stream of pretty-printed objects is its objects, not its lines. Read a
/// line at a time, a two-object file was six rows and two such files that
/// differed compared as identical, exit 0, with a column set that moved with
/// the thread count. Unindented, every nested object opens at column 0 like a
/// row, so the split guess is wrong and the check behind it has to catch it.
#[test]
fn a_stream_of_pretty_json_objects_is_read_as_its_objects() {
    for indent in [2usize, 0] {
        let rows = 120_000;
        let a = nested_stream(rows, indent, false);
        let b = nested_stream(rows, indent, true);
        assert!(
            a.len() > 8 << 20,
            "fixture must exceed the chunking threshold"
        );
        let f = json_fixture(&a, &b);
        for threads in [1usize, 2, 3, 4, 8] {
            let mut opt = Options::with_key(["id"]);
            opt.threads = Some(threads);
            let r = compare(&f.a, &f.b, &mut opt).expect("compare");
            let c = &r.counts;
            assert_eq!(
                (c.a_rows, c.b_rows, c.changed, c.added, c.removed),
                (rows as i64, rows as i64, 120, 0, 0),
                "indent {indent}, threads {threads}: {}",
                serde_json::json!(r.counts)
            );
            let names: Vec<&str> = r.columns.iter().map(|c| c.name.as_str()).collect();
            assert_eq!(names, ["meta", "v"], "indent {indent}, threads {threads}");
        }
    }
    // Several compact objects to a line are a stream too: the second and
    // third on each line were dropped. Written back to back with nothing
    // between (`}{`), the key walk stepped into the second and refused it.
    for between in [" ", ""] {
        let line = |v: u8| {
            (0..300)
                .map(|i| format!("{{\"id\":{i},\"v\":{}}}", if i == 7 { v } else { 1 }))
                .collect::<Vec<_>>()
                .chunks(3)
                .map(|c| c.join(between) + "\n")
                .collect::<String>()
        };
        let f = json_fixture(&line(1), &line(2));
        let r = compare(&f.a, &f.b, &mut Options::with_key(["id"])).expect("compare");
        assert_eq!(
            (r.counts.a_rows, r.counts.b_rows, r.counts.changed),
            (300, 300, 1),
            "objects joined by {between:?}"
        );
    }
}

/// A file read a line at a time whose lines are not one object each is
/// refused with the line and what to do, not read as some other number of
/// rows: a line that is not an object was a row with no key, and an object
/// that ran onto the next line, or a second one on its line, lost rows.
#[test]
fn json_lines_that_are_not_one_object_each_are_refused() {
    let good = "{\"id\":1,\"v\":\"x\"}\n{\"id\":2,\"v\":\"x\"}\n";
    for (bad, said) in [
        (
            "{\"id\":1,\"v\":\"x\"}\nhello\n",
            "line 2 (\"hello\") is not a JSON object",
        ),
        (
            "{\"id\":1,\"v\":\"x\"}\n[2]\n",
            "line 2 (\"[2]\") is not a JSON object",
        ),
        (
            "{\"id\":1,\"v\":\"x\"}\n{\n \"id\": 2,\n \"v\": \"y\"\n}\n",
            "line 2 (\"{\") runs onto",
        ),
        (
            "{\"id\":1,\"v\":\"x\"}\n{\"id\":2,\"v\":[\n1]}\n",
            "line 2 (\"{\\\"id\\\":2,\\\"v\\\":[\") runs onto",
        ),
        (
            "{\"id\":1,\"v\":\"x\"}\n{\"id\":2} {\"id\":3}\n",
            "follows an object on its line",
        ),
    ] {
        let f = json_fixture(bad, good);
        for threads in [1usize, 4] {
            let mut opt = Options::with_key(["id"]);
            opt.threads = Some(threads);
            let err = compare(&f.a, &f.b, &mut opt).expect_err(bad);
            let text = err.to_string();
            assert!(err.is_input(), "{bad:?}: {text}");
            assert!(text.contains(said), "{bad:?}: {text}");
            assert!(text.contains("jq -c ."), "{bad:?}: {text}");
        }
    }
}

/// The refusal `compare` gives `bad` against itself at threads 1, 2, 4 and 8,
/// which must be the same at each.
fn refused_at_every_thread_count(bad: &str) -> String {
    let f = json_fixture(bad, bad);
    let mut said: Option<String> = None;
    for threads in [1usize, 2, 4, 8] {
        let mut opt = Options::with_key(["id"]);
        opt.threads = Some(threads);
        let err = match compare(&f.a, &f.b, &mut opt) {
            Ok(r) => panic!("threads {threads}: read as {}", serde_json::json!(r.counts)),
            Err(err) => err,
        };
        assert!(err.is_input(), "threads {threads}: {err}");
        let text = err.to_string();
        match &said {
            None => said = Some(text),
            Some(first) => assert_eq!(&text, first, "threads {threads}"),
        }
    }
    said.expect("a refusal")
}

/// A framed file big enough to be split for threads, with something between
/// its rows that is not space, a comma or the next object, is refused with
/// the line, the same at every thread count. A `]` in a stream of objects
/// was the end of the rows for the piece of the file holding it, so the rows
/// read moved with the thread count (50,001 of 100,000 at one or two
/// threads, 99,998 at four and up); text after an array's closing `]`, or
/// after an object on its line, was dropped with any object in it.
#[test]
fn framed_json_with_text_between_rows_is_refused_at_every_thread_count() {
    let rows = 120_000;
    let stream = nested_stream(rows, 2, false);
    assert!(
        stream.len() > 8 << 20,
        "fixture must exceed the chunking threshold"
    );
    let middle = stream[stream.len() / 2..]
        .find("\n{\n")
        .map(|at| stream.len() / 2 + at + 1)
        .expect("a row start past the middle");
    let line = 1 + stream[..middle].matches('\n').count();
    let insert = |text: &str| format!("{}{text}{}", &stream[..middle], &stream[middle..]);
    for (bad, said) in [
        (
            insert("]\n"),
            format!("line {line} (\"]\") closes no array"),
        ),
        (
            insert(" x\n"),
            format!("line {line} (\"x\") is where the next row"),
        ),
        (
            insert("{\"id\": -1} x {\"id\": -2}\n"),
            format!("line {line} (\"x {{\\\"id\\\": -2}}\") is where the next row"),
        ),
        (
            insert(",,\n"),
            format!("line {line} (\",\") is where the next row"),
        ),
    ] {
        let got = refused_at_every_thread_count(&bad);
        assert!(got.contains(&said), "{said}: {got}");
    }
    let array = nested_array(rows, 2, false);
    for (tail, said) in [
        ("x\n", "(\"x\") follows the closing ]"),
        ("[{\"id\": -1}]\n", "follows the closing ]"),
        ("{\"id\": -1}\n", "follows the closing ]"),
    ] {
        let got = refused_at_every_thread_count(&format!("{array}{tail}"));
        assert!(got.contains(said), "{tail:?}: {got}");
        assert!(got.contains("jq -c '.[]'"), "{tail:?}: {got}");
    }
}

/// `changed` for one pair, at every thread count, so a proof that fires on one
/// chunking and not another shows up.
fn changed_json(a_body: &str, b_body: &str) -> i64 {
    let f = json_fixture(a_body, b_body);
    let mut answer = None;
    for threads in [1usize, 2, 4] {
        let mut opt = Options::with_key(["account_id", "txn_id"]);
        opt.ignore = vec!["note".to_string()];
        opt.threads = Some(threads);
        let r = compare(&f.a, &f.b, &mut opt).expect("compare");
        match answer {
            None => answer = Some(r.counts.changed),
            Some(first) => assert_eq!(
                r.counts.changed, first,
                "threads {threads} disagrees: {} against {first}",
                r.counts.changed
            ),
        }
    }
    answer.expect("a count")
}

/// The byte proof for JSON cannot take a prefix on trust the way CSV's can, and
/// these are the ways that bites. Each case is one row, so `changed` is 0 or 1.
///
/// Cross-checked against the C, C++ and Zig ports on the same fixtures: all four
/// agree on every line of this test.
#[test]
fn the_json_byte_proof_does_not_take_a_prefix_on_trust() {
    // A name repeated in one object takes its *last* value, so the mate's tail
    // can carry a value the proven prefix never saw. This is the case the whole
    // tail scan exists for, and the one a CSV-shaped proof gets wrong.
    assert_eq!(
        changed_json(
            "{\"account_id\":\"a\",\"txn_id\":\"t\",\"amount\":\"5\",\"note\":\"x\"}\n",
            "{\"account_id\":\"a\",\"txn_id\":\"t\",\"amount\":\"5\",\"note\":\"x\",\"amount\":\"9\"}\n",
        ),
        1,
        "a compared name repeated in the mate's tail was missed"
    );

    // Two objects need not list their names in the same order, so the run has to
    // reach the value that ends *last*, not the one belonging to the last column.
    assert_eq!(
        changed_json(
            "{\"account_id\":\"a\",\"txn_id\":\"t\",\"amount\":\"7\",\"note\":\"y\"}\n",
            "{\"account_id\":\"a\",\"txn_id\":\"t\",\"note\":\"y\",\"amount\":\"7\"}\n",
        ),
        0,
        "the same values in a different order were called a change"
    );

    // The run ends one byte past the last value -- its closing quote -- so a
    // value cannot stand in for a longer one that begins with it.
    assert_eq!(
        changed_json(
            "{\"account_id\":\"a\",\"txn_id\":\"t\",\"amount\":\"12\",\"note\":\"z\"}\n",
            "{\"account_id\":\"a\",\"txn_id\":\"t\",\"amount\":\"123\",\"note\":\"z\"}\n",
        ),
        1,
        "a value that is a prefix of the mate's was called equal"
    );

    // An ignored column is not tracked, so a difference there is not a change --
    // and the tail scan must not refuse the proof over it either way.
    assert_eq!(
        changed_json(
            "{\"account_id\":\"a\",\"txn_id\":\"t\",\"amount\":\"5\",\"note\":\"one\"}\n",
            "{\"account_id\":\"a\",\"txn_id\":\"t\",\"amount\":\"5\",\"note\":\"two\"}\n",
        ),
        0,
        "a difference in an ignored column was called a change"
    );

    // The keys can be written *after* the last compared value, which is why the
    // run covers every wanted field and not only the compared ones: without the
    // keys inside it, a byte-equal prefix would not prove the rows are a pair.
    assert_eq!(
        changed_json(
            "{\"amount\":\"5\",\"note\":\"x\",\"account_id\":\"a\",\"txn_id\":\"t\"}\n",
            "{\"amount\":\"6\",\"note\":\"x\",\"account_id\":\"a\",\"txn_id\":\"t\"}\n",
        ),
        1,
        "a change before the keys was missed"
    );
    assert_eq!(
        changed_json(
            "{\"amount\":\"5\",\"note\":\"x\",\"account_id\":\"a\",\"txn_id\":\"t\"}\n",
            "{\"amount\":\"5\",\"note\":\"x\",\"account_id\":\"a\",\"txn_id\":\"t\"}\n",
        ),
        0,
        "keys after the compared value broke the proof"
    );
}

/// Enough identical rows to cross `PROOF_BACKOFF` both ways, then a run of
/// changed ones, so the backoff cannot lose a difference by skipping it.
#[test]
fn the_backoff_never_skips_a_difference() {
    let mut a = String::new();
    let mut b = String::new();
    // 200 identical rows: the proof succeeds and the counter stays at zero.
    for i in 0..200 {
        let row = format!(
            "{{\"account_id\":\"a{i}\",\"txn_id\":\"t{i}\",\"amount\":\"{}\",\"note\":\"n\"}}\n",
            i % 7
        );
        a.push_str(&row);
        b.push_str(&row);
    }
    // 200 where every row differs: the proof fails often enough to back off.
    for i in 200..400 {
        a.push_str(&format!(
            "{{\"account_id\":\"a{i}\",\"txn_id\":\"t{i}\",\"amount\":\"1\",\"note\":\"n\"}}\n"
        ));
        b.push_str(&format!(
            "{{\"account_id\":\"a{i}\",\"txn_id\":\"t{i}\",\"amount\":\"2\",\"note\":\"n\"}}\n"
        ));
    }
    assert_eq!(
        changed_json(&a, &b),
        200,
        "the backoff skipped rows instead of parsing them"
    );
}

/// `agrees`, with columns ignored, and the number of rows turbo calls changed.
fn changed_ignoring(a_body: &str, b_body: &str, key: &[&str], ignore: &[&str]) -> i64 {
    let f = Fixture::new(a_body, b_body);
    let mut results = Vec::new();
    let mut changed = 0;
    for engine in [Engine::Native, Engine::Turbo] {
        let mut opt = Options::with_key(key.iter().copied());
        opt.engine = engine.label().to_string();
        opt.ignore = ignore.iter().map(|s| s.to_string()).collect();
        let r = compare(&f.a, &f.b, &mut opt).expect("compare");
        changed = r.counts.changed;
        results.push(
            serde_json::json!({
                "counts": r.counts, "columns": r.columns, "changed": r.changed,
                "added": r.added, "removed": r.removed, "dup_a": r.dup_a, "dup_b": r.dup_b,
            })
            .to_string(),
        );
    }
    assert_eq!(results[1], results[0], "turbo diverges from native");
    changed
}

/// The CSV byte proof with gaps: an ignored column among compared ones is
/// skipped by field count in each row on its own, so a value there that
/// differs -- in length, in quoting, with a delimiter inside the quotes -- does
/// not stop a row proving equal, and does not shift what the proof compares
/// after it. The last column is compared, so the proof also has to close on the
/// line ending.
#[test]
fn the_csv_byte_proof_skips_an_ignored_middle_column() {
    assert_eq!(
        changed_ignoring(
            "id,skip,a,b\nk1,S1,p,q\nk2,S1,p,q\nk3,S1,p,q\n",
            "id,skip,a,b\nk1,\"S, \"\"two\"\" and more\",p,q\nk2,,p,X\nk3,S2,p,qq\n",
            &["id"],
            &["skip"],
        ),
        2,
        "a middle ignored column that differs"
    );
    assert_eq!(
        changed_ignoring(
            "id,skip,a,b\nk1,S1,p,q\nk2,S1,p,q\n",
            "id,skip,a,b\nk1\nk2,S2\n",
            &["id"],
            &["skip"],
        ),
        2,
        "a mate that stops inside the gap"
    );
    assert_eq!(
        changed_ignoring(
            "skip,id,a,t,b\nS1,k1,p,T1,q\nS1,k2,p,T1,q\n",
            "skip,id,a,t,b\nS22,k1,p,T2,q\nS,k2,p,T222,Y\n",
            &["id"],
            &["skip", "t"],
        ),
        1,
        "a leading gap and a second one"
    );
    assert_eq!(
        changed_ignoring(
            "id,skip,a\nk1,S1,p\n",
            "id,skip,a\r\nk1,S22,p\r\n",
            &["id"],
            &["skip"],
        ),
        0,
        "a gap, and CRLF against LF"
    );
}

/// The JSON proof past an ignored value in the middle: where the rows first
/// differ inside the value of `note`, which nobody tracks, that value is skipped
/// in each row on its own and the comparison carries on -- whatever its length,
/// escapes, type or nested names. A repeat of a compared name after it still
/// wins, and a change after it is still found.
#[test]
fn the_json_byte_proof_skips_an_ignored_middle_value() {
    let row = |note: &str, amount: &str| {
        format!(
            "{{\"account_id\":\"a\",\"txn_id\":\"t\",\"note\":{note},\"amount\":\"{amount}\"}}\n"
        )
    };
    let cases: [(&str, &str, &str, &str, i64, &str); 5] = [
        (
            "\"x\"",
            "5",
            "\"x and longer\"",
            "5",
            0,
            "a middle value that differs",
        ),
        (
            "\"x\"",
            "5",
            "\"x and longer\"",
            "6",
            1,
            "and a real change after it",
        ),
        (
            "\"a\\\"b,c}\"",
            "5",
            "null",
            "5",
            0,
            "an escaped value against null",
        ),
        (
            "{\"amount\":\"Z\"}",
            "5",
            "[1,{\"amount\":\"Y\"}]",
            "5",
            0,
            "nested values, tracked names inside",
        ),
        (
            "\"x\"",
            "5",
            "\"y\"",
            "5\",\"amount\":\"LATER",
            1,
            "a repeat of the compared name after it",
        ),
    ];
    for (na, aa, nb, ab, want, what) in cases {
        assert_eq!(changed_json(&row(na, aa), &row(nb, ab)), want, "{what}");
    }
}

/// Repeated composite keys on both sides, with keys whose columns differ: the
/// duplicate sections carry each key's values as the insertion decoded them
/// when it found the repeat, and those must be the values in key order, the
/// same the reference engine reads.
#[test]
fn duplicate_sections_carry_the_whole_key_in_order() {
    let mut a = String::from("region,acct,v\n");
    let mut b = String::from("region,acct,v\n");
    for i in 0..40 {
        a.push_str(&format!("r{},a{i},x{i}\n", i % 3));
        b.push_str(&format!("r{},a{i},y{i}\n", i % 3));
    }
    for i in (0..40).step_by(4) {
        a.push_str(&format!("r{},a{i},again{i}\n", i % 3));
    }
    for i in (0..40).step_by(5) {
        b.push_str(&format!("r{},a{i},again{i}\n", i % 3));
        b.push_str(&format!("r{},a{i},thrice{i}\n", i % 3));
    }
    agrees(&a, &b, &["region", "acct"], false, false);
}

/// A file cut off mid-row (a download or a write that stopped) can end right
/// after a key's opening quote. That half-row has no key to read; reading one
/// anyway sliced backwards and panicked.
#[test]
fn an_ndjson_row_cut_off_inside_a_key_is_not_read_past() {
    for tail in ["{\"", "{\"id\":\"b\",\"", "{\"id\":\"b\",  \""] {
        let f = json_fixture(
            &format!("{{\"id\":\"a\",\"x\":\"1\"}}\n{tail}"),
            "{\"id\":\"a\",\"x\":\"1\"}\n",
        );
        let mut opt = Options::with_key(["id"]);
        opt.engine = Engine::Turbo.label().to_string();
        let r = compare(&f.a, &f.b, &mut opt).expect("compare");
        assert_eq!(r.counts.matched, 1, "tail {tail:?}");
        assert_eq!(r.counts.changed, 0, "tail {tail:?}");
    }
}
