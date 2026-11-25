//! The three input formats, and the thread count.
//!
//! Both are the same kind of risk: a change that is supposed to be invisible in
//! the answer. A file read as Parquet has to produce the counts it produces as
//! CSV, and a comparison split across four threads has to produce the counts it
//! produces on one — including *which* rows survive `--max-rows`, which is the
//! part that silently stops being true when ranges are merged out of order.

use std::fs;
use std::path::{Path, PathBuf};

use compara::contract::CompareResult;
use compara::engine::compare;
use compara::options::Options;

fn fixture(name: &str) -> PathBuf {
    Path::new("tests")
        .join("fixtures")
        .join("formats")
        .join(name)
}

fn options(key: &[&str]) -> Options {
    let mut opt = Options::with_key(key.iter().copied());
    opt.engine = "turbo".to_string();
    opt
}

fn run(a: &Path, b: &Path, opt: &mut Options) -> CompareResult {
    compare(a, b, opt)
        .unwrap_or_else(|e| panic!("comparing {} with {}: {e}", a.display(), b.display()))
}

/// Counts and per-column stats, which is what every port is held to.
fn answer(result: &CompareResult) -> String {
    let mut out = format!("{:?}", result.counts);
    for column in &result.columns {
        out.push_str(&format!(
            "\n{} {} {} {}",
            column.name, column.changed, column.blanked, column.filled
        ));
    }
    out
}

/// The same table written eight ways, compared against the same other side.
///
/// Each of these is a *writer* decision — dictionary or plain, five codecs,
/// version-2 delta pages, one row group or five — and none of them is allowed to
/// change the answer. `scripts/make_parquet_fixtures.py` writes them with
/// pyarrow, so this is a test against another implementation's output rather
/// than against our own.
#[test]
fn every_encoding_of_a_parquet_file_reads_the_same_way() {
    let b = fixture("b.csv");
    let expected = answer(&run(&fixture("a.csv"), &b, &mut options(&["id"])));

    for name in [
        "a_dict_snappy.parquet",
        "a_plain_none.parquet",
        "a_dict_gzip.parquet",
        "a_plain_zstd.parquet",
        "a_plain_lz4.parquet",
        "a_delta_v2.parquet",
        "a_dict_row_groups.parquet",
        "a.ndjson",
        "a.tsv",
    ] {
        let got = answer(&run(&fixture(name), &b, &mut options(&["id"])));
        assert_eq!(got, expected, "{name} did not read as the CSV does");
    }
}

/// Gzip, zstd and LZ4 pages are read on the columnar path, with the row
/// reader's answer, as snappy and uncompressed ones always were. Until they
/// were, any of them sent the pair row by row, and zstd is what polars writes
/// unless told otherwise. Version-2 pages still go row by row.
#[test]
fn every_codec_a_writer_picks_by_default_takes_the_columnar_path() {
    let b = fixture("b_dict_snappy.parquet");
    for (name, path) in [
        ("a_dict_snappy.parquet", "parquet"),
        ("a_plain_none.parquet", "parquet"),
        ("a_dict_gzip.parquet", "parquet"),
        ("a_plain_zstd.parquet", "parquet"),
        ("a_plain_lz4.parquet", "parquet"),
        ("a_delta_v2.parquet", "turbo"),
    ] {
        let mut columnar = options(&["id"]);
        columnar.engine = "auto".to_string();
        let got = run(&fixture(name), &b, &mut columnar);
        assert_eq!(got.meta.engine, path, "{name}");
        let want = run(&fixture(name), &b, &mut options(&["id"]));
        assert_eq!(whole(&got), whole(&want), "{name}");
    }
}

/// Either side may be in any format, and the two need not agree: a CSV export
/// compares against the Parquet a warehouse emits.
#[test]
fn the_two_sides_need_not_be_in_the_same_format() {
    let expected = answer(&run(
        &fixture("a.csv"),
        &fixture("b.csv"),
        &mut options(&["id"]),
    ));
    for (a, b) in [
        ("a.ndjson", "b.csv"),
        ("a.csv", "b.ndjson"),
        ("a.tsv", "b.tsv"),
        ("a.tsv", "b.csv"),
        ("a.csv", "b.tsv"),
        ("a.tsv", "b_dict_snappy.parquet"),
        ("a_dict_snappy.parquet", "b.csv"),
        ("a.csv", "b_dict_snappy.parquet"),
        ("a.ndjson", "b_dict_snappy.parquet"),
        ("a_dict_snappy.parquet", "b_dict_snappy.parquet"),
    ] {
        let got = answer(&run(&fixture(a), &fixture(b), &mut options(&["id"])));
        assert_eq!(got, expected, "{a} against {b}");
    }
}

/// A tab-separated file is a CSV file with another separator, and every engine
/// reads it as one: the whole answer -- counts, column statistics, every
/// changed, added, removed and duplicated row -- and the export files are the
/// CSV pair's, byte for byte.
#[test]
fn tsv_reads_as_csv_on_every_engine() {
    let dir = std::env::temp_dir().join(format!("compara-tsv-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    for engine in ["turbo", "sortmerge", "native"] {
        let read = |ext: &str| {
            let to = dir.join(format!("{engine}-{ext}"));
            let mut opt = options(&["id"]);
            opt.engine = engine.to_string();
            opt.export_dir = Some(to.to_string_lossy().into_owned());
            let got = run(
                &fixture(&format!("a.{ext}")),
                &fixture(&format!("b.{ext}")),
                &mut opt,
            );
            (whole(&got), exported(&to))
        };
        let (csv, tsv) = (read("csv"), read("tsv"));
        assert_eq!(tsv.0, csv.0, "{engine}: the answer");
        assert_eq!(tsv.1, csv.1, "{engine}: the export");
    }
    let _ = fs::remove_dir_all(&dir);
}

/// A typed Parquet column has to become the text a CSV of the same data holds,
/// and `typed.csv` is that text: written from the same values by the fixture
/// script, so this compares the reader's rendering against a stated expectation
/// rather than against another copy of itself.
#[test]
fn typed_columns_render_as_the_csv_of_the_same_data() {
    let result = run(
        &fixture("typed.parquet"),
        &fixture("typed.csv"),
        &mut options(&["id"]),
    );
    assert_eq!(
        result.counts.changed,
        0,
        "a typed column rendered differently from the CSV: {:?}",
        result
            .columns
            .iter()
            .filter(|c| c.changed > 0)
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
    );
    assert_eq!(result.counts.matched, 8);
}

/// A file with quoted newlines and doubled quotes, big enough that the engine
/// splits it into chunks.
///
/// This is where threading can actually break: a chunk boundary that lands
/// inside a quoted field would start parsing mid-row, and the only reason it
/// does not is the quote-parity count. The rows are deliberately awkward.
fn awkward_pair(dir: &Path) -> (PathBuf, PathBuf) {
    let mut a = String::from("k,v,w\n");
    let mut b = String::from("k,v,w\n");
    for i in 0..100_000 {
        let quoted = format!("\"line {i}\nsecond, with \"\"quotes\"\" in it\"");
        a.push_str(&format!("K{i},{quoted},{i}\n"));
        let value = if i % 7 == 0 { i + 1 } else { i };
        b.push_str(&format!("K{i},{quoted},{value}\n"));
    }
    let (a_path, b_path) = (dir.join("a.csv"), dir.join("b.csv"));
    fs::write(&a_path, a).expect("the A file");
    fs::write(&b_path, b).expect("the B file");
    (a_path, b_path)
}

#[test]
fn the_thread_count_does_not_change_the_answer() {
    let dir = std::env::temp_dir().join(format!("compara-threads-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    let (a, b) = awkward_pair(&dir);
    assert!(
        fs::metadata(&a).expect("the A file").len() > 4 << 20,
        "the fixture must be over the chunking threshold or this tests nothing"
    );

    let mut reference: Option<String> = None;
    for threads in [1usize, 2, 3, 4, 8, 16] {
        let mut opt = options(&["k"]);
        opt.threads = Some(threads);
        opt.max_rows = 25; // small enough that the cap decides which rows are kept
        let result = run(&a, &b, &mut opt);
        let mut got = answer(&result);
        // The kept rows, not just how many: merging ranges out of order would
        // keep a different twenty-five and leave every count intact.
        got.push_str(&format!("\n{:?}", result.changed));
        match &reference {
            None => reference = Some(got),
            Some(want) => assert_eq!(&got, want, "{threads} threads gave a different answer"),
        }
    }
    fs::remove_dir_all(&dir).ok();
}

/// The same, for a columnar file: the rows come from the Parquet reader rather
/// than from a chunked scan, and they are hashed in parallel ranges.
#[test]
fn the_thread_count_does_not_change_a_parquet_answer() {
    let mut reference: Option<String> = None;
    for threads in [1usize, 2, 4, 8] {
        let mut opt = options(&["id"]);
        opt.threads = Some(threads);
        let got = answer(&run(
            &fixture("a_dict_row_groups.parquet"),
            &fixture("b_dict_snappy.parquet"),
            &mut opt,
        ));
        match &reference {
            None => reference = Some(got),
            Some(want) => assert_eq!(&got, want, "{threads} threads gave a different answer"),
        }
    }
}

/// A file that is not what its name says is read as what it is, and a file that
/// is nothing at all is refused by name rather than misread.
#[test]
fn the_format_comes_from_the_bytes_rather_than_the_name() {
    let dir = std::env::temp_dir().join(format!("compara-sniff-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    let csv_named_parquet = dir.join("a.parquet");
    fs::copy(fixture("a.csv"), &csv_named_parquet).expect("a copy");
    let expected = answer(&run(
        &fixture("a.csv"),
        &fixture("b.csv"),
        &mut options(&["id"]),
    ));
    let got = answer(&run(
        &csv_named_parquet,
        &fixture("b.csv"),
        &mut options(&["id"]),
    ));
    assert_eq!(got, expected);

    let truncated = dir.join("truncated.parquet");
    let mut bytes = fs::read(fixture("a_dict_snappy.parquet")).expect("the fixture");
    bytes.truncate(bytes.len() / 2);
    fs::write(&truncated, bytes).expect("the truncated file");
    let error = compare(&truncated, &fixture("b.csv"), &mut options(&["id"]))
        .expect_err("half a Parquet file is not readable");
    assert!(
        format!("{error}").contains("truncated"),
        "the error should say what is wrong with the file: {error}"
    );
    fs::remove_dir_all(&dir).ok();
}

/// A typed Parquet value has one rendering, and a CSV of the same data need not
/// use it: `0.50` for the DOUBLE 0.5, `12345` for the DECIMAL 12345.00, a space
/// for the `T` of a timestamp, a zero fraction or a `Z`. Against a typed column
/// those are the same value; before, each was a changed cell, so a CSV compared
/// with its own export to Parquet came out changed on every row.
#[test]
fn a_typed_value_is_compared_by_value_rather_than_by_its_spelling() {
    let dir = std::env::temp_dir().join(format!("compara-typed-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("temp dir");
    let header = "id,count,big,ratio,flag,day,at,money\n";
    let rows = [
        "K-0000,0,0,0.0,true,1970-01-01,2024-01-01 00:00:00,0",
        "K-0001,1,1,0.50,false,2024-01-01,2024-01-01 12:34:56.000,1.0",
        "K-0002,-1,-1,-0.250,true,2024-02-29,1970-01-01T00:00:00Z,-1",
        "K-0003,42,1099511627776,1.50,true,1969-12-31,2026-09-07 22:15:00,12345",
        "K-0004,1000,-1099511627776,3.0,false,2000-03-01,1999-12-31 23:59:59+00:00,-12345.0",
        "K-0005,-1000,7,1e10,false,1999-12-31,2000-01-01 00:00:01,999999",
        "K-0006,2147483647,8,1.0e-10,true,2038-01-19,2024-06-30 06:30:30.000000,100",
        "K-0007,-2147483648,9,2.250,false,2026-09-07,2024-12-25 18:45:15,-100",
    ];
    let respelled = dir.join("respelled.csv");
    fs::write(&respelled, format!("{header}{}\n", rows.join("\n"))).expect("write");
    let parquet = fixture("typed.parquet");
    for (a, b) in [(&respelled, &parquet), (&parquet, &respelled)] {
        let result = run(a, b, &mut options(&["id"]));
        assert_eq!(
            (result.counts.matched, result.counts.changed),
            (8, 0),
            "{}",
            answer(&result)
        );
    }

    // A real change in a typed column is still one, and --tolerance still
    // applies to the numbers.
    let mut changed = rows.map(String::from);
    changed[7] = "K-0007,-2147483648,9,2.26,false,2026-09-07,2024-12-25 18:45:16,-100.01".into();
    let drifted = dir.join("drifted.csv");
    fs::write(&drifted, format!("{header}{}\n", changed.join("\n"))).expect("write");
    let result = run(&drifted, &parquet, &mut options(&["id"]));
    let per_column: Vec<_> = result
        .columns
        .iter()
        .filter(|c| c.changed > 0)
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(
        (result.counts.changed, per_column),
        (1, vec!["ratio", "at", "money"])
    );
    let mut opt = options(&["id"]);
    opt.tolerance = 0.02;
    let result = run(&drifted, &parquet, &mut opt);
    let per_column: Vec<_> = result
        .columns
        .iter()
        .filter(|c| c.changed > 0)
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!((result.counts.changed, per_column), (1, vec!["at"]));

    // Between two CSVs there is no type to go by: the text is the value.
    let result = run(&respelled, &fixture("typed.csv"), &mut options(&["id"]));
    assert_eq!(result.counts.changed, 8, "{}", answer(&result));
    let _ = fs::remove_dir_all(&dir);
}

/// Everything a comparison reports but the run's own facts (which files, how
/// long, which path), so two paths over one pair can be held to each other
/// cell for cell.
fn whole(r: &CompareResult) -> String {
    let mut v = serde_json::to_value(r).expect("the result serialises");
    for at in [Some("meta"), None] {
        let target = match at {
            Some(k) => v.get_mut(k),
            None => Some(&mut v),
        };
        if let Some(o) = target.and_then(|t| t.as_object_mut()) {
            for gone in ["a", "b", "seconds", "engine", "generated", "options"] {
                o.remove(gone);
            }
        }
    }
    serde_json::to_string(&v).unwrap()
}

/// A Parquet pair whose columns hold numbers, dates, times and booleans is
/// compared a column at a time, one word per value, instead of being turned
/// into rows of text: before, a single such column sent the whole pair to the
/// row-by-row reader. The answer has to be that reader's -- every count, every
/// changed cell as it reads, every added, removed and duplicated row -- under
/// each option that changes how a value is compared.
#[test]
fn a_parquet_pair_of_word_columns_is_compared_without_making_rows() {
    type Tweak = fn(&mut Options);
    let tweaks: [(&str, Tweak); 6] = [
        ("defaults", |_| {}),
        ("--trim", |o| o.trim = true),
        ("--ignore-case", |o| o.ignore_case = true),
        ("--empty-is-null", |o| o.empty_is_null = true),
        ("--tolerance 0.3", |o| o.tolerance = 0.3),
        ("--max-rows 3", |o| o.max_rows = 3),
    ];
    let columnar = |a: &str, b: &str, tweak: Tweak| {
        let mut opt = options(&["id"]);
        opt.engine = "auto".to_string();
        tweak(&mut opt);
        run(&fixture(a), &fixture(b), &mut opt)
    };
    for (a, b) in [
        ("words_a.parquet", "words_b.parquet"),
        ("words_a_plain.parquet", "words_b_plain.parquet"),
        ("words_a_zstd.parquet", "words_b_zstd.parquet"),
    ] {
        for (label, tweak) in tweaks {
            let got = columnar(a, b, tweak);
            assert_eq!(
                got.meta.engine, "parquet",
                "{a} {b} {label}: not the columnar path"
            );
            let mut rows = options(&["id"]);
            tweak(&mut rows);
            let want = run(&fixture(a), &fixture(b), &mut rows);
            assert_eq!(whole(&got), whole(&want), "{a} against {b}, {label}");
        }
    }

    // The fixture's drift, so the agreement above is about something: every
    // column changes somewhere, and B's -0.0 and NaN are A's 0.0 and NaN.
    let r = columnar("words_a.parquet", "words_b.parquet", |_| {});
    assert_eq!(
        (r.counts.added, r.counts.removed, r.counts.b_dup_keys),
        (1, 1, 1)
    );
    for c in &r.columns {
        assert!(c.changed > 0, "{} never changes", c.name);
    }
    let changed = |name: &str| r.columns.iter().find(|c| c.name == name).unwrap().changed;
    assert_eq!((changed("ratio"), changed("single")), (30, 7));

    // A column the two files store as different types is not compared word for
    // word: `small` is INT64 in one, `money` an INT64 decimal against a
    // fixed-length one in the other. Those pairs read row by row, and the
    // answer is the same data's.
    let want = whole(&r);
    for b in ["words_b_wider.parquet", "words_b_plain.parquet"] {
        let got = columnar("words_a.parquet", b, |_| {});
        assert_eq!(got.meta.engine, "turbo", "{b}");
        assert_eq!(whole(&got), want, "{b}");
    }
}

/// `--memory` on two Parquet files decides how many columns are read at once,
/// and a budget too small for the key columns, their index and one column of
/// each file is refused before anything is read. The columnar path used to
/// take any budget and ignore it.
#[test]
fn a_parquet_pair_keeps_to_a_stated_memory_budget() {
    let (a, b) = (fixture("words_a.parquet"), fixture("words_b.parquet"));
    let mut opt = options(&["id"]);
    opt.engine = "auto".to_string();
    let want = whole(&run(&a, &b, &mut opt.clone()));

    let mut tight = opt.clone();
    tight.memory = Some(4 << 10);
    let err = compare(&a, &b, &mut tight).expect_err("a 4 KB budget is refused");
    assert!(
        err.to_string().contains("more than the 0 MB budget"),
        "{err}"
    );

    let mut enough = opt.clone();
    enough.memory = Some(64 << 20);
    let got = run(&a, &b, &mut enough);
    assert_eq!(got.meta.engine, "parquet");
    assert_eq!(whole(&got), want);
}

/// The three files `--export-dir` writes, as they read back.
fn exported(dir: &Path) -> [String; 3] {
    ["added.csv", "removed.csv", "changed.csv"].map(|name| {
        fs::read_to_string(dir.join(name))
            .unwrap_or_else(|e| panic!("{}: {e}", dir.join(name).display()))
    })
}

/// Writes a Parquet pair from rows of text: dictionary pages, or with `plain`
/// plain ones.
fn parquet_pair(
    dir: &Path,
    stem: &str,
    names: &[&str],
    a: &[Vec<&str>],
    b: &[Vec<&str>],
    plain: bool,
) -> (PathBuf, PathBuf) {
    use compara::gendata::parquet::{Compression, Writer};
    let write = |path: &Path, rows: &[Vec<&str>]| {
        let mut w = Writer::create(path, names, Compression::None, 1 << 20).expect("a writer");
        if plain {
            w = w.with_dictionary_limit(0);
        }
        for row in rows {
            w.write_row(row.iter().copied()).expect("a row");
        }
        w.finish().expect("the footer");
    };
    let (pa, pb) = (
        dir.join(format!("{stem}_a.parquet")),
        dir.join(format!("{stem}_b.parquet")),
    );
    write(&pa, a);
    write(&pb, b);
    (pa, pb)
}

/// Values no generator writes on its own: a separator, quotes, a line break
/// and a carriage return inside a value, empty and padded values, letters in
/// both cases and outside ASCII, a key that is empty in one of its columns and
/// one empty in both, and a repeated key.
fn awkward_parquet(dir: &Path, plain: bool) -> (PathBuf, PathBuf) {
    let a: Vec<Vec<&str>> = vec![
        vec!["1", "x", "a,b", "say \"hi\""],
        vec!["2", "x", "line\nbreak", "  padded "],
        vec!["3", "", "MiXeD", "ünïcödé"],
        vec!["4", "y", "1.0", ""],
        vec!["5", "y", "gone", "r\rr"],
        vec!["2", "x", "a repeat", "is not compared"],
    ];
    let b: Vec<Vec<&str>> = vec![
        vec!["", "", "an empty key", "sorts last"],
        vec!["6", "z", "new", "comma, \"quote\""],
        vec!["4", "y", "1", ""],
        vec!["3", "", "mixed", "ünïcödé"],
        vec!["2", "x", "line\nbreak", " padded "],
        vec!["1", "x", "a;b", "say \"hi\""],
    ];
    let stem = if plain {
        "awkward_plain"
    } else {
        "awkward_dict"
    };
    parquet_pair(dir, stem, &["k1", "k2", "v", "w"], &a, &b, plain)
}

/// `--export-dir` on two Parquet files is written by the columnar path rather
/// than refused, and the files are the row reader's, byte for byte -- every
/// row, in key order, quoted the same way -- under each option that changes
/// how a value reads or compares. Asking for an export does not change the
/// report either.
#[test]
fn a_parquet_pair_exports_what_the_row_reader_exports() {
    type Tweak = fn(&mut Options);
    let tweaks: [(&str, Tweak); 6] = [
        ("defaults", |_| {}),
        ("--trim", |o| o.trim = true),
        ("--ignore-case", |o| o.ignore_case = true),
        ("--empty-is-null", |o| o.empty_is_null = true),
        ("--tolerance 0.3", |o| o.tolerance = 0.3),
        ("--max-rows 1", |o| o.max_rows = 1),
    ];
    let dir = std::env::temp_dir().join(format!("compara-pq-export-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("a scratch directory");
    let mut pairs: Vec<(PathBuf, PathBuf, &[&str])> = vec![
        (
            fixture("words_a.parquet"),
            fixture("words_b.parquet"),
            &["id"],
        ),
        (
            fixture("words_a_plain.parquet"),
            fixture("words_b_plain.parquet"),
            &["id"],
        ),
        (
            fixture("a_dict_snappy.parquet"),
            fixture("b_dict_snappy.parquet"),
            &["id"],
        ),
    ];
    for plain in [false, true] {
        let (a, b) = awkward_parquet(&dir, plain);
        pairs.push((a, b, &["k1", "k2"]));
    }
    let (columnar_dir, rows_dir) = (dir.join("columnar"), dir.join("rows"));
    for (a, b, key) in &pairs {
        for (label, tweak) in tweaks {
            let what = format!("{} against {}, {label}", a.display(), b.display());
            let mut opt = options(key);
            opt.engine = "auto".to_string();
            tweak(&mut opt);
            let report = run(a, b, &mut opt.clone());
            opt.export_dir = Some(columnar_dir.to_string_lossy().into_owned());
            let got = run(a, b, &mut opt);
            assert_eq!(got.meta.engine, "parquet", "{what}: not the columnar path");
            assert_eq!(
                whole(&got),
                whole(&report),
                "{what}: the export changed the report"
            );

            let mut rows = options(key);
            tweak(&mut rows);
            rows.export_dir = Some(rows_dir.to_string_lossy().into_owned());
            run(a, b, &mut rows);
            assert_eq!(exported(&columnar_dir), exported(&rows_dir), "{what}");
        }
    }

    // What the awkward pair's agreement is about: the values come out quoted
    // where they have to be, and the key that is empty in both columns sorts
    // after every key that is not.
    let (a, b) = awkward_parquet(&dir, true);
    let mut opt = options(&["k1", "k2"]);
    opt.engine = "auto".to_string();
    opt.export_dir = Some(columnar_dir.to_string_lossy().into_owned());
    run(&a, &b, &mut opt);
    let [added, removed, changed] = exported(&columnar_dir);
    assert_eq!(
        added,
        "k1,k2,v,w\n6,z,new,\"comma, \"\"quote\"\"\"\n,,an empty key,sorts last\n"
    );
    assert_eq!(removed, "k1,k2,v,w\n5,y,gone,\"r\rr\"\n");
    assert!(
        changed.contains("\"a,b\",a;b,\"say \"\"hi\"\"\",\"say \"\"hi\"\"\""),
        "{changed}"
    );
    assert!(
        changed.contains("\"line\nbreak\",\"line\nbreak\",  padded , padded "),
        "{changed}"
    );
    assert!(
        changed.contains("3,,MiXeD,mixed,ünïcödé,ünïcödé"),
        "{changed}"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// An export of more rows than one encoded block, on several lanes: the blocks
/// are written in order whatever order they finish in, and the rows -- stored
/// out of key order, every one of them changed -- come out sorted.
#[test]
fn a_large_parquet_export_is_written_in_key_order() {
    let dir = std::env::temp_dir().join(format!("compara-pq-export-big-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("a scratch directory");
    let n = 20_000;
    let keys: Vec<String> = (0..n).map(|i| format!("K-{:05}", (i * 7919) % n)).collect();
    let vals: Vec<(String, String)> = (0..n).map(|i| (format!("a{i}"), format!("b{i}"))).collect();
    let a: Vec<Vec<&str>> = (0..n)
        .map(|i| vec![keys[i].as_str(), vals[i].0.as_str(), "same"])
        .collect();
    let b: Vec<Vec<&str>> = (0..n)
        .rev()
        .map(|i| vec![keys[i].as_str(), vals[i].1.as_str(), "same"])
        .collect();
    let (pa, pb) = parquet_pair(&dir, "big", &["id", "v", "w"], &a, &b, false);

    let rows_dir = dir.join("rows");
    let mut rows = options(&["id"]);
    rows.export_dir = Some(rows_dir.to_string_lossy().into_owned());
    run(&pa, &pb, &mut rows);
    let want = exported(&rows_dir);
    assert_eq!(want[2].lines().count(), n + 1, "every row is changed");
    assert!(want[2].lines().nth(1).unwrap().starts_with("K-00000,"));

    for threads in [1, 2, 3] {
        let to = dir.join(format!("columnar-{threads}"));
        let mut opt = options(&["id"]);
        opt.engine = "auto".to_string();
        opt.threads = Some(threads);
        opt.export_dir = Some(to.to_string_lossy().into_owned());
        let got = run(&pa, &pb, &mut opt);
        assert_eq!(got.meta.engine, "parquet");
        assert_eq!(exported(&to), want, "{threads} threads");
    }
    let _ = fs::remove_dir_all(&dir);
}

/// A UTF-8 byte order mark (what Excel and many Windows tools write first) is
/// not part of the first column's name, in any engine that reads the file. It
/// was, in the byte-level engine: the key `id` became `\u{feff}id` and the
/// comparison failed with "key column(s) missing".
#[test]
fn a_byte_order_mark_is_not_part_of_the_first_column() {
    let dir = std::env::temp_dir().join(format!("compara-bom-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("temp dir");
    let with_bom = |name: &str| {
        let mut bytes = b"\xEF\xBB\xBF".to_vec();
        bytes.extend(fs::read(fixture(name)).expect("fixture"));
        let path = dir.join(name);
        fs::write(&path, bytes).expect("write");
        path
    };
    let (a_bom, b_bom, a_json_bom) = (with_bom("a.csv"), with_bom("b.csv"), with_bom("a.ndjson"));
    let (a, b) = (fixture("a.csv"), fixture("b.csv"));
    for engine in ["turbo", "sortmerge", "native"] {
        let mut opt = options(&["id"]);
        opt.engine = engine.to_string();
        let expected = answer(&run(&a, &b, &mut opt.clone()));
        assert_eq!(
            answer(&run(&a_bom, &b, &mut opt.clone())),
            expected,
            "{engine}: BOM on A"
        );
        assert_eq!(
            answer(&run(&a, &b_bom, &mut opt.clone())),
            expected,
            "{engine}: BOM on B"
        );
        assert_eq!(
            answer(&run(&a_bom, &b_bom, &mut opt)),
            expected,
            "{engine}: BOM on both"
        );
    }
    let expected = answer(&run(&fixture("a.ndjson"), &b, &mut options(&["id"])));
    assert_eq!(
        answer(&run(&a_json_bom, &b, &mut options(&["id"]))),
        expected,
        "BOM before JSON"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// sortmerge and native read CSV only. Named on a Parquet or JSON input they
/// are refused; before, a Parquet pair quietly took the columnar path and
/// reported it, as if the named engine had been an option it could ignore.
#[test]
fn a_csv_only_engine_named_for_parquet_or_json_is_refused() {
    let pairs = [
        ("a_plain_none.parquet", "b_dict_snappy.parquet"),
        ("a_plain_none.parquet", "b.csv"),
        ("a.ndjson", "b.csv"),
        ("a.csv", "b.ndjson"),
    ];
    for engine in ["sortmerge", "native"] {
        for (a, b) in pairs {
            let mut opt = options(&["id"]);
            opt.engine = engine.to_string();
            let err = compare(&fixture(a), &fixture(b), &mut opt).expect_err("refused");
            assert!(
                err.to_string().contains("reads CSV only"),
                "{engine} {a} {b}: {err}"
            );
        }
    }
    for engine in ["auto", "turbo"] {
        for (a, b) in pairs {
            let mut opt = options(&["id"]);
            opt.engine = engine.to_string();
            run(&fixture(a), &fixture(b), &mut opt);
        }
    }
}

/// A nested object or array is a cell of its JSON text, in newline-delimited
/// JSON and in a top-level array alike. Before, it was read as absent, so two
/// different objects compared as the same missing value and the change went
/// unreported. The same object with its keys in another order is a change,
/// as docs/engines.md says.
#[test]
fn a_nested_json_value_is_compared_as_its_text() {
    let dir = std::env::temp_dir().join(format!("compara-nested-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    let a = dir.join("a.ndjson");
    let b = dir.join("b.ndjson");
    let b_array = dir.join("b.json");
    fs::write(
        &a,
        concat!(
            "{\"id\":\"1\",\"meta\":{\"a\":1,\"b\":[1,2]},\"tags\":[],\"v\":\"x\"}\n",
            "{\"id\":\"2\",\"meta\":{\"a\":2},\"tags\":[\"p\",\"q\"],\"v\":\"y\"}\n",
            "{\"id\":\"3\",\"meta\":{\"a\":3,\"b\":\"}\"},\"v\":\"z\"}\n",
            "{\"id\":\"4\",\"meta\":{\"a\":4,\"b\":5},\"v\":\"w\"}\n",
        ),
    )
    .unwrap();
    let rows = [
        // changed: one element of a nested array
        "{\"id\":\"1\",\"meta\":{\"a\":1,\"b\":[1,3]},\"tags\":[],\"v\":\"x\"}",
        // unchanged, nested object and array alike
        "{\"id\":\"2\",\"meta\":{\"a\":2},\"tags\":[\"p\",\"q\"],\"v\":\"y\"}",
        // unchanged: a brace inside a string is text, not structure
        "{\"id\":\"3\",\"meta\":{\"a\":3,\"b\":\"}\"},\"v\":\"z\"}",
        // changed: the same object, keys in another order
        "{\"id\":\"4\",\"meta\":{\"b\":5,\"a\":4},\"v\":\"w\"}",
    ];
    fs::write(&b, rows.join("\n") + "\n").unwrap();
    fs::write(&b_array, format!("[\n  {}\n]\n", rows.join(",\n  "))).unwrap();

    for other in [&b, &b_array] {
        let result = run(&a, other, &mut options(&["id"]));
        assert_eq!(result.counts.matched, 4, "{}", answer(&result));
        assert_eq!(result.counts.changed, 2, "{}", answer(&result));
        let meta = result.columns.iter().find(|c| c.name == "meta").unwrap();
        assert_eq!(meta.changed, 2, "{}", answer(&result));
        let tags = result.columns.iter().find(|c| c.name == "tags").unwrap();
        assert_eq!(tags.changed, 0, "{}", answer(&result));
        let shown = whole(&result);
        assert!(shown.contains(r#"{\"a\":1,\"b\":[1,2]}"#), "{shown}");
        assert!(shown.contains(r#"{\"a\":1,\"b\":[1,3]}"#), "{shown}");
    }
    fs::remove_dir_all(&dir).ok();
}

/// The same records written by two writers: one compact with every non-ASCII
/// character escaped and `/` too (Python's `json.dumps` and PHP's defaults),
/// the other pretty-printed one object after another (`jq .`). A nested value
/// is the same value whatever the space between its tokens or the spelling of
/// its strings' characters; read as written, every nested cell differed. Key
/// order and a number's spelling are still a change, and the key column may
/// be a nested value itself: it hashes as it compares.
#[test]
fn the_same_records_written_pretty_and_compact_are_the_same_table() {
    let dir = std::env::temp_dir().join(format!("compara-pretty-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    let a = dir.join("a.ndjson");
    let b = dir.join("b.json");
    fs::write(
        &a,
        concat!(
            "{\"k\":{\"n\":1},\"m\":{\"s\":\"caf\\u00e9 \\/ \\u003c\",\"l\":[1,2]},\"v\":\"x\"}\n",
            "{\"k\":{\"n\":2},\"m\":[{\"t\":\"x}{\"},{\"t\":\"\\\"\"}],\"v\":\"y\"}\n",
            "{\"k\":{\"n\":3},\"m\":{\"a\":1,\"b\":2},\"v\":\"z\"}\n",
            "{\"k\":{\"n\":4},\"m\":[1.0,2],\"v\":\"w\"}\n",
            "{\"k\":{\"n\":5},\"m\":[1,2],\"v\":\"u\"}\n",
        ),
    )
    .unwrap();
    fs::write(
        &b,
        concat!(
            // unchanged: spacing, `é`, `/` and `<` as themselves
            "{\n  \"k\": {\n    \"n\": 1\n  },\n  \"m\": {\n    \"s\": \"caf\u{e9} / <\",\n",
            "    \"l\": [\n      1,\n      2\n    ]\n  },\n  \"v\": \"x\"\n}\n",
            // unchanged: objects in an array open their own lines
            "{\n  \"k\": {\"n\": 2},\n  \"m\": [\n    {\n      \"t\": \"x}{\"\n    },\n",
            "    {\n      \"t\": \"\\u0022\"\n    }\n  ],\n  \"v\": \"y\"\n}\n",
            // changed: the keys in another order
            "{\n  \"k\": {\"n\": 3},\n  \"m\": {\"b\": 2, \"a\": 1},\n  \"v\": \"z\"\n}\n",
            // changed: 1.0 written 1
            "{\n  \"k\": {\"n\": 4},\n  \"m\": [1, 2],\n  \"v\": \"w\"\n}\n",
            // changed: one element
            "{\n  \"k\": {\"n\": 5},\n  \"m\": [1, 3],\n  \"v\": \"u\"\n}\n",
        ),
    )
    .unwrap();
    for threads in [1usize, 4] {
        let mut opt = options(&["k"]);
        opt.threads = Some(threads);
        let result = run(&a, &b, &mut opt);
        assert_eq!(result.counts.matched, 5, "{}", answer(&result));
        assert_eq!(result.counts.changed, 3, "{}", answer(&result));
        let m = result.columns.iter().find(|c| c.name == "m").unwrap();
        assert_eq!(m.changed, 3, "{}", answer(&result));
        // The report shows a nested value in its one spelling.
        let shown = whole(&result);
        assert!(shown.contains(r#"{\"b\":2,\"a\":1}"#), "{shown}");
        assert!(shown.contains(r#"[1.0,2]"#), "{shown}");
    }
    fs::remove_dir_all(&dir).ok();
}

/// A nested JSON value and the same JSON in a CSV cell or a Parquet string,
/// written as `json.dumps` spaces it: the text cell is read as JSON where it
/// is one JSON object or array, and is equal to the value however either is
/// spaced or its strings escaped. Read as written, the same records in NDJSON
/// and in a CSV were changed in every nested cell; compact, they were equal,
/// so the answer turned on the writer's spacing. Text that is not one JSON
/// value stays as written, a JSON string is a string, and a key column of
/// nested values hashes as it compares.
#[test]
fn a_nested_json_value_and_the_same_json_in_a_text_cell_are_equal() {
    use compara::gendata::parquet::{Compression, Writer};
    let dir = std::env::temp_dir().join(format!("compara-json-text-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    // (id, the nested value in NDJSON, the text in the CSV cell and the
    // Parquet string, equal)
    let rows: [(&str, &str, &str, bool); 7] = [
        (
            "1",
            r#"{"a":[1,2],"s":"caf\u00e9 \/"}"#,
            r#"{"a": [1, 2], "s": "café /"}"#,
            true,
        ),
        (
            "2",
            r#"[{"t":"x}{"},{"t":"\""}]"#,
            "\n [ {\"t\": \"x}{\"}, {\"t\": \"\\u0022\"} ] ",
            true,
        ),
        ("3", r#"{"a":1,"b":2}"#, r#"{"b": 2, "a": 1}"#, false),
        ("4", r#"[1.0,2]"#, "[1, 2]", false),
        // Not JSON: as written, which the nested value's text is not.
        ("5", r#"{"a":1}"#, "{a: 1}", false),
        ("6", r#"[3,4]"#, "[3, 4] x", false),
        // Compact text is the nested value's own text, as before.
        ("7", r#"{"a":[1,2]}"#, r#"{"a":[1,2]}"#, true),
    ];
    let json = dir.join("a.ndjson");
    let csv = dir.join("b.csv");
    let parquet = dir.join("b.parquet");
    let mut nd = String::new();
    let mut text = String::from("id,k,m\n");
    let mut w = Writer::create(&parquet, &["id", "k", "m"], Compression::None, 3).unwrap();
    for (id, nested, cell, _) in rows {
        nd.push_str(&format!(
            "{{\"id\":\"{id}\",\"k\":{nested},\"m\":{nested}}}\n"
        ));
        let quoted = format!("\"{}\"", cell.replace('"', "\"\""));
        text.push_str(&format!("{id},{quoted},{quoted}\n"));
        w.write_row([id, cell, cell].into_iter()).unwrap();
    }
    w.finish().unwrap();
    fs::write(&json, nd).unwrap();
    fs::write(&csv, text).unwrap();
    let equal = rows.iter().filter(|r| r.3).count() as i64;
    for other in [&csv, &parquet] {
        for (a, b) in [(&json, other), (other, &json)] {
            for threads in [1usize, 4] {
                for trim in [false, true] {
                    let mut opt = options(&["id"]);
                    opt.threads = Some(threads);
                    opt.trim = trim;
                    let result = run(a, b, &mut opt);
                    assert_eq!(result.counts.matched, 7, "{}", answer(&result));
                    assert_eq!(result.counts.changed, 7 - equal, "{}", answer(&result));
                    // Keyed on the nested column, the equal rows match.
                    let mut opt = options(&["k"]);
                    opt.threads = Some(threads);
                    opt.trim = trim;
                    let result = run(a, b, &mut opt);
                    assert_eq!(result.counts.matched, equal, "{}", answer(&result));
                    assert_eq!(result.counts.changed, 0, "{}", answer(&result));
                }
            }
        }
    }
    // A JSON string holding the text is a string, and two CSV cells are
    // compared as written: both as before.
    let strings = dir.join("strings.ndjson");
    fs::write(&strings, "{\"id\":\"1\",\"m\":\"{\\\"a\\\": 1}\"}\n").unwrap();
    let nested = dir.join("nested.ndjson");
    fs::write(&nested, "{\"id\":\"1\",\"m\":{\"a\":1}}\n").unwrap();
    let result = run(&strings, &nested, &mut options(&["id"]));
    assert_eq!(result.counts.changed, 1, "{}", answer(&result));
    let spaced = dir.join("spaced.csv");
    fs::write(&spaced, "id,m\n1,\"{\"\"a\"\": 1}\"\n").unwrap();
    let compact = dir.join("compact.csv");
    fs::write(&compact, "id,m\n1,\"{\"\"a\"\":1}\"\n").unwrap();
    let result = run(&spaced, &compact, &mut options(&["id"]));
    assert_eq!(result.counts.changed, 1, "{}", answer(&result));
    // Against a JSON string, the CSV cell is text too.
    let result = run(&strings, &spaced, &mut options(&["id"]));
    assert_eq!(result.counts.changed, 0, "{}", answer(&result));
    fs::remove_dir_all(&dir).ok();
}

/// Two nested values that differ in a lone surrogate, which a JSON escape can
/// name and UTF-8 cannot hold, are compared different, and the report showed
/// both as U+FFFD: alike, and under `--trim`, which compares their text,
/// equal. The report shows each as its escape.
#[test]
fn a_lone_surrogate_in_a_nested_value_is_shown_as_its_escape() {
    let dir = std::env::temp_dir().join(format!("compara-surrogate-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    let a = dir.join("a.ndjson");
    let b = dir.join("b.ndjson");
    fs::write(&a, "{\"id\":1,\"v\":[\"\\ud800\",\"\\ud83d\\ude00\"]}\n").unwrap();
    fs::write(&b, "{\"id\":1,\"v\":[\"\\uD801\",\"\\ud83d\\ude00\"]}\n").unwrap();
    for trim in [false, true] {
        let mut opt = options(&["id"]);
        opt.trim = trim;
        let result = run(&a, &b, &mut opt);
        assert_eq!(result.counts.changed, 1, "{}", answer(&result));
        let shown = whole(&result);
        assert!(shown.contains(r#"[\"\\ud800\",\"😀\"]"#), "{shown}");
        assert!(shown.contains(r#"[\"\\ud801\",\"😀\"]"#), "{shown}");
    }
    fs::remove_dir_all(&dir).ok();
}

/// The same for a top-level JSON string, which has no escaping of its own to
/// tell a lone surrogate's escape from the six characters `\ud800` as text:
/// `--trim` and `--ignore-case` compared the text, where every lone surrogate
/// was U+FFFD three times, so `"\ud800"` and `"\ud801"` were equal there and
/// one key, and the report showed them alike. Compared, two values are equal
/// under every option only where their bytes allow it. Shown, a lone
/// surrogate is its escape and every other value is as written, so a lone
/// surrogate and its six characters as text look alike; the cell is still
/// listed, in the report and the export alike, as the compare decides it.
#[test]
fn a_lone_surrogate_in_a_json_string_is_compared_and_shown_as_its_escape() {
    let dir = std::env::temp_dir().join(format!("compara-surrogate-str-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    // Row by row: two lone surrogates; a plain backslash; the text `\ud800`
    // against the lone surrogate; letters around one, apart in case only; a
    // pair, which is one character; one surrogate, a space after it on one
    // side, its digits in two cases.
    let a_rows = [
        r#""\ud800""#,
        r#""a\\b""#,
        r#""\\ud800""#,
        r#""x\ud800""#,
        r#""😀""#,
        r#""\ud800 ""#,
    ];
    let b_rows = [
        r#""\ud801""#,
        r#""a\\b""#,
        r#""\ud800""#,
        r#""X\ud800""#,
        r#""😀""#,
        r#""\uD800""#,
    ];
    let write = |name: &str, rows: &[&str]| {
        let path = dir.join(name);
        let text: String = rows
            .iter()
            .enumerate()
            .map(|(i, v)| format!("{{\"id\":{i},\"v\":{v}}}\n"))
            .collect();
        fs::write(&path, text).unwrap();
        path
    };
    let (a, b) = (write("a.ndjson", &a_rows), write("b.ndjson", &b_rows));
    // Keyed on the strings, the last row left out: its two sides are one
    // key only under --trim.
    let (ka, kb) = (
        write("ka.ndjson", &a_rows[..5]),
        write("kb.ndjson", &b_rows[..5]),
    );
    // (trim, ignore case, changed rows, matched keys)
    for (trim, ignore_case, changed, matched) in [
        (false, false, 4, 3),
        (true, false, 3, 3),
        (false, true, 3, 4),
        (true, true, 2, 4),
    ] {
        for threads in [1usize, 4] {
            let what = format!("trim {trim}, ignore case {ignore_case}, threads {threads}");
            // The report's changed cells come from the rows without an
            // export, and from the export's own pass with one.
            for export in [false, true] {
                let what = format!("{what}, export {export}");
                let to = dir.join(format!("ex-{trim}-{ignore_case}-{threads}"));
                let mut opt = options(&["id"]);
                (opt.threads, opt.trim, opt.ignore_case) = (Some(threads), trim, ignore_case);
                if export {
                    opt.export_dir = Some(to.to_string_lossy().into_owned());
                }
                let result = run(&a, &b, &mut opt);
                assert_eq!(
                    result.counts.changed,
                    changed,
                    "{what}: {}",
                    answer(&result)
                );
                let shown = whole(&result);
                assert!(
                    shown.contains(r#"["0",[[0,"\\ud800","\\ud801"]]]"#),
                    "{what}: {shown}"
                );
                assert!(
                    shown.contains(r#"["2",[[0,"\\ud800","\\ud800"]]]"#),
                    "{what}: {shown}"
                );
                if export {
                    let [_, _, changed_csv] = exported(&to);
                    assert!(
                        changed_csv.contains("\n0,\\ud800,\\ud801\n")
                            && changed_csv.contains("\n2,\\ud800,\\ud800\n"),
                        "{what}: {changed_csv}"
                    );
                }
            }
            let mut opt = options(&["v"]);
            (opt.threads, opt.trim, opt.ignore_case) = (Some(threads), trim, ignore_case);
            let result = run(&ka, &kb, &mut opt);
            assert_eq!(
                result.counts.matched,
                matched,
                "{what}: {}",
                answer(&result)
            );
            assert_eq!(
                result.counts.a_dup_keys + result.counts.b_dup_keys,
                0,
                "{what}: {}",
                answer(&result)
            );
        }
    }
    // A nested value is shown as its JSON, a lone surrogate in it as its
    // escape: `["\ud800"]`, as a JSON string holding that text is shown. The
    // two differ, and the changed cell is listed though they look alike.
    let nested = write("nested.ndjson", &[r#"["\ud800"]"#, "1"]);
    let string = write("string.ndjson", &[r#""[\"\\ud800\"]""#, "1"]);
    for export in [false, true] {
        let mut opt = options(&["id"]);
        if export {
            opt.export_dir = Some(dir.join("ex-nested").to_string_lossy().into_owned());
        }
        let result = run(&nested, &string, &mut opt);
        assert_eq!(result.counts.changed, 1, "{}", answer(&result));
        let shown = whole(&result);
        assert!(
            shown.contains(r#"["0",[[0,"[\"\\ud800\"]","[\"\\ud800\"]"]]]"#),
            "export {export}: {shown}"
        );
    }
    fs::remove_dir_all(&dir).ok();
}

/// An export keeps every value exactly as it was: a CSV cell holding JSON
/// that Python's `json.dumps` wrote, an emoji spelled as the escapes of its
/// surrogate pair, and a Windows path, are written back byte for byte. Shown
/// by the reading the compare uses, which doubles a backslash before the text
/// of a surrogate, the JSON read back as other text.
#[test]
fn an_exported_cell_is_its_input_cell_byte_for_byte() {
    let dir = std::env::temp_dir().join(format!("compara-export-as-is-{}", std::process::id()));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).expect("a temp dir");
    let (a, b) = (dir.join("a.csv"), dir.join("b.csv"));
    fs::write(
        &a,
        "id,payload\n1,\"{\"\"msg\"\":\"\"hi \\ud83d\\ude00\"\"}\"\n2,C:\\temp\\x\n",
    )
    .unwrap();
    fs::write(
        &b,
        "id,payload\n1,\"{\"\"msg\"\":\"\"hi \\ud83d\\ude01\"\"}\"\n2,C:\\temp\\y\n",
    )
    .unwrap();
    let rows = |path: &Path| -> Vec<String> {
        let text = fs::read_to_string(path).unwrap();
        text.lines()
            .skip(1)
            .map(|l| l.split_once(',').unwrap().1.to_string())
            .collect()
    };
    let (cells_a, cells_b) = (rows(&a), rows(&b));
    for trim in [false, true] {
        let to = dir.join(format!("ex-{trim}"));
        let mut opt = options(&["id"]);
        opt.trim = trim;
        opt.export_dir = Some(to.to_string_lossy().into_owned());
        let result = run(&a, &b, &mut opt);
        assert_eq!(result.counts.changed, 2, "{}", answer(&result));
        let [_, _, changed] = exported(&to);
        let want: String = std::iter::once("id,payload (A),payload (B)\n".to_string())
            .chain((0..2).map(|i| format!("{},{},{}\n", i + 1, cells_a[i], cells_b[i])))
            .collect();
        assert_eq!(changed, want, "trim {trim}");
    }
    let _ = fs::remove_dir_all(&dir);
}

/// A member name is the name its escapes spell. Python's `json.dumps` writes
/// every non-ASCII name escaped by default, a character past U+FFFF as a
/// surrogate pair, and PHP escapes `/`. Such a name was looked up as written
/// and named no column: its changes went unreported with exit 0, and the same
/// name written plainly on the other side made every row changed. A key
/// column may be one, and the files are past the size a file is split at.
#[test]
fn a_json_name_written_with_escapes_is_the_name_it_spells() {
    let dir = std::env::temp_dir().join(format!("compara-escaped-names-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    let rows = 60_000;
    // `escaped` spells the names as Python and PHP do; `change` alters the
    // `café` value of every 1000th row and the emoji's of every 1500th.
    let table = |escaped: bool, change: bool| {
        let (key, cafe, slash, emoji) = if escaped {
            ("cl\\u00e9", "caf\\u00e9", "a\\/b", "\\ud83d\\ude00 x")
        } else {
            ("cl\u{e9}", "caf\u{e9}", "a/b", "\u{1f600} x")
        };
        let mut text = String::with_capacity(rows * 96);
        for i in 0..rows {
            let c = if change && i % 1000 == 0 { "z" } else { "c" };
            let e = if change && i % 1500 == 0 { "z" } else { "e" };
            text.push_str(&format!(
                "{{\"{key}\":{i},\"{cafe}\":\"{c}{i}\",\"{slash}\":\"slash {i}\",\
                 \"{emoji}\":\"{e}{i}\",\"note\":\"a row of some length\"}}\n"
            ));
        }
        let path = dir.join(format!("{escaped}-{change}.ndjson"));
        fs::write(&path, text).unwrap();
        path
    };
    let a = table(true, false);
    let a_raw = table(false, false);
    let b = table(true, true);
    let b_raw = table(false, true);
    assert!(fs::metadata(&a).unwrap().len() > 4 << 20);
    // Rows 0, 3000, 6000, ... change both columns.
    let n = rows as i64;
    let changed = n / 1000 + n / 1500 - n / 3000;
    for (x, y, want) in [
        (&a, &b, changed),
        (&a, &b_raw, changed),
        (&a_raw, &b, changed),
        (&a, &a_raw, 0),
    ] {
        for threads in [1usize, 2, 4] {
            for engine in ["auto", "turbo"] {
                let mut opt = options(&["cl\u{e9}"]);
                opt.engine = engine.to_string();
                opt.threads = Some(threads);
                let result = run(x, y, &mut opt);
                let said = format!("{} {} {engine} {threads}", x.display(), y.display());
                assert_eq!(result.counts.matched, n, "{said}: {}", answer(&result));
                assert_eq!(result.counts.changed, want, "{said}: {}", answer(&result));
                let per: Vec<(&str, i64)> = result
                    .columns
                    .iter()
                    .map(|c| (c.name.as_str(), c.changed))
                    .collect();
                let (cafe, emoji) = if want == 0 {
                    (0, 0)
                } else {
                    (n / 1000, n / 1500)
                };
                assert_eq!(
                    per,
                    [
                        ("caf\u{e9}", cafe),
                        ("a/b", 0),
                        ("\u{1f600} x", emoji),
                        ("note", 0)
                    ],
                    "{said}"
                );
            }
        }
    }
    fs::remove_dir_all(&dir).ok();
}

/// A name repeated in one JSON object takes its last value, as json.loads,
/// JavaScript and jq read it: a key column took its first, and a later `null`
/// left an earlier value standing. In A every 7th row names its key twice,
/// first wrongly, and every 5th row gives `v` a value and then `null`; B does
/// the same with `v` and another value, and every 13th row of B spells its
/// first `id` with an escape. Every 11th row of B repeats `w` with a new
/// value. So the rows all match, and only `w` changes. The files are past the
/// size a file is split at, and the run is held to it on every route that
/// reads JSON.
#[test]
fn a_json_name_repeated_in_an_object_takes_its_last_value() {
    let dir = std::env::temp_dir().join(format!("compara-repeated-names-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    let rows = 80_000usize;
    let mut a = String::with_capacity(rows * 100);
    let mut b = String::with_capacity(rows * 100);
    for i in 0..rows {
        let id = if i % 7 == 0 {
            format!("\"id\":\"t{i}\",")
        } else {
            String::new()
        };
        let v = if i % 5 == 0 {
            format!("\"v\":\"a{i}\",\"note\":\"a row of some length\",\"v\":null")
        } else {
            format!("\"v\":\"c{i}\",\"note\":\"a row of some length\"")
        };
        a.push_str(&format!("{{{id}{v},\"w\":\"x\",\"id\":{i}}}\n"));
        let id = if i % 13 == 0 {
            format!("\"\\u0069d\":\"u{i}\",")
        } else {
            String::new()
        };
        let v = if i % 5 == 0 {
            format!("\"v\":\"b{i}\",\"note\":\"a row of some length\",\"v\":null")
        } else {
            format!("\"v\":\"c{i}\",\"note\":\"a row of some length\"")
        };
        let w = if i % 11 == 0 {
            "\"w\":\"x\",\"w\":\"y\""
        } else {
            "\"w\":\"x\""
        };
        b.push_str(&format!("{{{id}{v},{w},\"id\":{i}}}\n"));
    }
    let (pa, pb) = (dir.join("a.ndjson"), dir.join("b.ndjson"));
    fs::write(&pa, a).unwrap();
    fs::write(&pb, b).unwrap();
    assert!(fs::metadata(&pa).unwrap().len() > 4 << 20);
    let n = rows as i64;
    let w = (n - 1) / 11 + 1;
    for threads in [1usize, 2, 4] {
        for engine in ["auto", "turbo"] {
            for memory in [None, Some(64u64 << 20)] {
                let mut opt = options(&["id"]);
                opt.engine = engine.to_string();
                opt.threads = Some(threads);
                opt.memory = memory;
                let result = run(&pa, &pb, &mut opt);
                let said = format!("{engine} threads {threads} memory {memory:?}");
                assert_eq!(result.counts.matched, n, "{said}: {}", answer(&result));
                assert_eq!(result.counts.added, 0, "{said}: {}", answer(&result));
                assert_eq!(result.counts.changed, w, "{said}: {}", answer(&result));
                let per: Vec<(&str, i64)> = result
                    .columns
                    .iter()
                    .map(|c| (c.name.as_str(), c.changed))
                    .collect();
                assert_eq!(per, [("v", 0), ("note", 0), ("w", w)], "{said}");
            }
        }
    }
    fs::remove_dir_all(&dir).ok();
}

/// A member name that is not UTF-8 is refused, on any thread count: the
/// header named it with a replacement character and the parser looked it up
/// as written, so the column had no value on either side and a change there
/// went unreported.
#[test]
fn a_json_name_that_is_not_utf8_is_refused() {
    let dir = std::env::temp_dir().join(format!("compara-latin1-names-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    let mut a = Vec::new();
    let mut b = Vec::new();
    for i in 0..60_000 {
        let name: &[u8] = if i == 41_000 { b"caf\xe9" } else { b"cafe" };
        for (out, v) in [(&mut a, "a"), (&mut b, "b")] {
            out.extend_from_slice(format!("{{\"id\":{i},\"").as_bytes());
            out.extend_from_slice(name);
            out.extend_from_slice(
                format!("\":\"{v}\",\"note\":\"a row of some length\"}}\n").as_bytes(),
            );
        }
    }
    let (pa, pb) = (dir.join("a.ndjson"), dir.join("b.ndjson"));
    fs::write(&pa, a).unwrap();
    fs::write(&pb, b).unwrap();
    for threads in [1usize, 2, 4] {
        let mut opt = options(&["id"]);
        opt.engine = "auto".to_string();
        opt.threads = Some(threads);
        let Err(err) = compare(&pa, &pb, &mut opt) else {
            panic!("a name that is not UTF-8 was read on {threads} threads");
        };
        let said = err.to_string();
        assert!(err.is_input(), "{said}");
        assert!(
            said.contains("line 41001: the member name \"caf\\xE9\" is not UTF-8"),
            "{said}"
        );
        assert!(said.contains("iconv"), "{said}");
    }
    fs::remove_dir_all(&dir).ok();
}

/// A JSON table's columns are the keys of all its objects, not of the first.
///
/// A writer that leaves a null field out of its object (Spark's does, and
/// Go's `omitempty`) leaves the column out of the first object whenever the
/// first row has no value there. The columns were once the first object's
/// keys alone: `v` below was then no column, the two files compared as
/// identical, and the exit code said so.
#[test]
fn a_json_column_missing_from_the_first_object_is_still_compared() {
    let dir = std::env::temp_dir().join(format!("compara-later-keys-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    let csv = |name: &str, text: &str| {
        let path = dir.join(name);
        fs::write(&path, text).unwrap();
        path
    };
    let a_rows = [
        r#"{"id":"1"}"#,
        r#"{"id":"2","v":"x","w":"p"}"#,
        r#"{"id":"3","w":"q","va":"e"}"#,
        r#"{}"#,
    ];
    let b_rows = [
        r#"{"id":"1"}"#,
        r#"{"id":"2","v":"y","w":"p"}"#,
        r#"{"w":"q","id":"3","va":"e"}"#,
    ];
    let a_csv = csv("a.csv", "id,v,w,va\n1,,,\n2,x,p,\n3,,q,e\n,,,\n");
    let b_csv = csv("b.csv", "id,v,w,va\n1,,,\n2,y,p,\n3,,q,e\n");
    let from_csv = answer(&run(&a_csv, &b_csv, &mut options(&["id"])));

    let lines = |rows: &[&str]| rows.join("\n") + "\n";
    let array = |rows: &[&str]| format!("[\n  {}\n]\n", rows.join(",\n  "));
    for (a_text, b_text) in [
        (lines(&a_rows), lines(&b_rows)),
        (array(&a_rows), array(&b_rows)),
        (lines(&a_rows), array(&b_rows)),
    ] {
        let a = csv("a.json", &a_text);
        let b = csv("b.json", &b_text);
        let result = run(&a, &b, &mut options(&["id"]));
        assert_eq!(result.counts.changed, 1, "{}", answer(&result));
        assert_eq!(answer(&result), from_csv);
        let v = result.columns.iter().find(|c| c.name == "v").unwrap();
        assert_eq!(v.changed, 1, "{}", answer(&result));
    }

    // An object with no key at all is not the end of the columns either, and
    // a file with no key anywhere is refused by what is wrong with it.
    let a = csv("a.json", "{}\n{\"id\":\"1\",\"v\":\"x\"}\n");
    let b = csv("b.json", "{}\n{\"id\":\"1\",\"v\":\"y\"}\n");
    assert_eq!(run(&a, &b, &mut options(&["id"])).counts.changed, 1);
    // A key with no colon or value after it, or one cut off by the end of
    // the file, breaks its object: the row parser read the object as far as
    // the break, so the file is refused with the line. A line break inside
    // the name runs it onto the next line.
    for (text, said) in [
        (
            "{\"id\":\"1\",\"v\":\"x\"}\n{\"id\":\"2\",\"zz\"}\n",
            "line 2 (\"}\") is not JSON",
        ),
        (
            "{\"id\":\"1\",\"v\":\"x\"}\n{\"id\":\"3\",\"q",
            "the file ends inside",
        ),
        (
            "{\"id\":\"1\",\"v\":\"x\"}\n{\"id\":\"3\",\"q\n",
            "line break",
        ),
    ] {
        let broken = csv("broken.json", text);
        let refused = compare(&broken, &broken, &mut options(&["id"])).expect_err(text);
        assert!(refused.to_string().contains(said), "{text:?}: {refused}");
    }
    let empty = csv("empty.json", "{}\n{}\n");
    let refused = compare(&empty, &empty, &mut options(&["id"])).expect_err("no columns");
    assert!(
        refused
            .to_string()
            .contains("no JSON object in the file has a key"),
        "{refused}"
    );
    fs::remove_dir_all(&dir).ok();
}

/// The columns found past the first object come out in the order the file
/// holds them, on any number of threads: the file is walked in pieces, and a
/// key first met in the last piece must not move ahead of one met in the
/// first.
#[test]
fn json_columns_found_later_keep_the_file_s_order_on_every_thread_count() {
    let dir = std::env::temp_dir().join(format!("compara-later-order-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("a temp dir");
    // Past the size a file is split at, with one new key early and one late.
    let rows = 120_000;
    let table = |changed: &str| {
        let mut text = String::with_capacity(rows * 64);
        for i in 0..rows {
            let extra = match i {
                5 => ",\"early\":\"e\"".to_string(),
                n if n == rows - 5 => format!(",\"late\":\"{changed}\""),
                _ => String::new(),
            };
            text.push_str(&format!(
                "{{\"id\":\"{i}\",\"note\":\"a row of some length to fill the file\"{extra}}}\n"
            ));
        }
        text
    };
    let a = dir.join("a.ndjson");
    let b = dir.join("b.ndjson");
    fs::write(&a, table("x")).unwrap();
    fs::write(&b, table("y")).unwrap();
    assert!(fs::metadata(&a).unwrap().len() > 4 << 20);

    let mut first = None;
    for threads in [1, 2, 3, 8] {
        let mut opt = options(&["id"]);
        opt.threads = Some(threads);
        let result = run(&a, &b, &mut opt);
        let names: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["note", "early", "late"], "threads {threads}");
        assert_eq!(result.counts.changed, 1, "threads {threads}");
        let said = answer(&result);
        assert_eq!(
            first.get_or_insert_with(|| said.clone()),
            &said,
            "threads {threads}"
        );
    }
    fs::remove_dir_all(&dir).ok();
}
