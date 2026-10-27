//! `compara columns` and `compara head`: looking at one file rather than two.
//!
//! The gate that matters is cross-format agreement. The same rows written as
//! CSV, as ndjson and as Parquet must preview identically, because the preview
//! goes through the engine's own parsers -- a quoted CSV field is unquoted here
//! exactly as the comparison would unquote it. A preview that disagreed with
//! the comparison would be worse than none.

use std::path::{Path, PathBuf};
use std::process::Command;

use compara::gendata::{Compression, Format, generate_as};

/// A number no other fixture in this process has. The tests run as parallel
/// threads of one process, and the clock this used to take cannot tell them
/// apart: macOS reports microseconds, and two fixtures made in the same one
/// shared a directory, which the first to finish deleted under the other.
fn fixture_id() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT: AtomicU32 = AtomicU32::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

const BIN: &str = env!("CARGO_BIN_EXE_compara");

struct Fixture(PathBuf);

impl Fixture {
    /// One pair in every format, from this crate's own generator.
    fn new(rows: i64) -> Fixture {
        let dir = std::env::temp_dir().join(format!(
            "compara-inspect-{}-{}",
            std::process::id(),
            fixture_id()
        ));
        std::fs::create_dir_all(&dir).expect("a temp directory");
        for (format, ext) in [
            (Format::Csv, "csv"),
            (Format::Tsv, "tsv"),
            (Format::Ndjson, "ndjson"),
            (Format::Parquet, "parquet"),
        ] {
            generate_as(
                rows,
                &dir.join(format!("a.{ext}")),
                &dir.join(format!("b.{ext}")),
                7,
                format,
                Compression::None,
            )
            .expect("the generator");
        }
        Fixture(dir)
    }

    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn run(args: &[&str], file: &Path) -> (i32, String, String) {
    let out = Command::new(BIN)
        .args(args)
        .arg(file)
        .output()
        .expect("compara runs");
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn columns_agree_across_the_formats() {
    let f = Fixture::new(2_000);
    let mut seen: Vec<(String, Vec<String>)> = Vec::new();
    for ext in ["csv", "tsv", "ndjson", "parquet"] {
        let (code, stdout, stderr) = run(&["columns"], &f.path(&format!("a.{ext}")));
        assert_eq!(code, 0, "{ext}: {stderr}");
        let names: Vec<String> = stdout.lines().map(|l| l.to_string()).collect();
        assert_eq!(names.len(), 20, "{ext} should list 20 columns: {stdout}");
        assert_eq!(names[0], "account_id", "{ext}");
        assert_eq!(names[1], "txn_id", "{ext}");
        seen.push((ext.to_string(), names));
    }
    let (_, first) = &seen[0];
    for (ext, names) in &seen[1..] {
        assert_eq!(names, first, "{ext} lists different columns than csv");
    }
}

/// The names go to stdout on their own so `columns f | paste -sd,` builds a
/// `--key`; the summary goes to stderr.
#[test]
fn columns_keeps_the_summary_off_stdout() {
    let f = Fixture::new(2_000);
    let (code, stdout, stderr) = run(&["columns"], &f.path("a.parquet"));
    assert_eq!(code, 0);
    assert!(
        !stdout.contains("columns,"),
        "stdout is names only: {stdout}"
    );
    assert!(
        stderr.contains("parquet"),
        "the summary names the format: {stderr}"
    );
    assert!(
        stderr.contains("20 columns"),
        "and the column count: {stderr}"
    );
    // Parquet states its rows in the footer, so no read is needed for them.
    // 2,000 rows plus the one duplicate the recipe adds at this size.
    assert!(stderr.contains("2,001 rows"), "{stderr}");
}

/// A text format cannot state a row count without being read end to end, and
/// `columns` is not the command that should pay for that.
#[test]
fn columns_does_not_claim_a_row_count_it_would_have_to_read_for() {
    let f = Fixture::new(2_000);
    let (_, _, stderr) = run(&["columns"], &f.path("a.csv"));
    assert!(stderr.contains("row count needs a full read"), "{stderr}");
}

#[test]
fn head_previews_the_same_rows_from_every_format() {
    let f = Fixture::new(2_000);
    let csv = run(&["head", "-n", "5", "--csv"], &f.path("a.csv"));
    assert_eq!(csv.0, 0, "{}", csv.2);
    for ext in ["tsv", "ndjson", "parquet"] {
        let other = run(&["head", "-n", "5", "--csv"], &f.path(&format!("a.{ext}")));
        assert_eq!(other.0, 0, "{ext}: {}", other.2);
        assert_eq!(
            other.1, csv.1,
            "{ext} previews different rows than csv does"
        );
    }
    // Header plus five rows.
    assert_eq!(csv.1.lines().count(), 6, "{}", csv.1);
}

#[test]
fn head_defaults_to_ten_rows() {
    let f = Fixture::new(2_000);
    let (code, stdout, stderr) = run(&["head", "--csv"], &f.path("a.csv"));
    assert_eq!(code, 0, "{stderr}");
    assert_eq!(stdout.lines().count(), 11, "header plus ten: {stdout}");
}

/// Asking for more rows than the file holds is not an error; it is the file.
#[test]
fn head_stops_at_the_end_of_a_short_file() {
    let f = Fixture::new(2_000);
    let (code, stdout, _) = run(&["head", "-n", "100000", "--csv"], &f.path("a.parquet"));
    assert_eq!(code, 0);
    // 2,000 rows plus the one duplicate the recipe adds at this size, and a header.
    assert_eq!(
        stdout.lines().count(),
        2_001 + 1,
        "{}",
        stdout.lines().count()
    );
}

#[test]
fn the_aligned_table_is_the_default_and_carries_a_rule() {
    let f = Fixture::new(2_000);
    let (code, stdout, _) = run(&["head", "-n", "2"], &f.path("a.csv"));
    assert_eq!(code, 0);
    let lines: Vec<&str> = stdout.lines().collect();
    assert_eq!(lines.len(), 4, "header, rule, two rows: {stdout}");
    assert!(lines[0].starts_with("account_id"), "{stdout}");
    assert!(lines[1].starts_with("---"), "{stdout}");
}

#[test]
fn a_bad_row_count_is_an_error() {
    let f = Fixture::new(2_000);
    let (code, _, stderr) = run(&["head", "-n", "many"], &f.path("a.csv"));
    assert_eq!(code, 2);
    assert!(stderr.contains("--rows must be a whole number"), "{stderr}");
}

/// A typo in an option used to take the file after it as its value and fail
/// with "needs a value", or quietly do nothing.
#[test]
fn an_option_compara_does_not_have_is_named() {
    let f = Fixture::new(10);
    for (args, said) in [
        (vec!["head", "--bogus"], "unknown option: --bogus"),
        (
            vec!["columns", "--delimeter", ";"],
            "unknown option: --delimeter",
        ),
        (vec!["compare", "-x"], "unknown option: -x"),
        (vec!["head", "--rows-max=3"], "unknown option: --rows-max"),
    ] {
        let (code, _, stderr) = run(&args, &f.path("a.csv"));
        assert_eq!(code, 2, "{args:?}: {stderr}");
        assert!(stderr.contains(said), "{args:?}: {stderr}");
    }
    let (code, _, stderr) = run(&["head", "-n", "3", "--delimiter=,"], &f.path("a.csv"));
    assert_eq!(code, 0, "{stderr}");
}

/// A command compara doesn't have is named in one line with the ones it
/// does, not followed by sixty lines of usage; a file given where the command
/// goes gets the line that compares it.
#[test]
fn an_unknown_command_lists_the_commands() {
    let out = Command::new(BIN).arg("diff").output().expect("runs");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("unknown command \"diff\""), "{stderr}");
    assert!(
        stderr.contains("compare, columns, head, env and version"),
        "{stderr}"
    );
    assert_eq!(stderr.lines().count(), 1, "{stderr}");
    let out = Command::new(BIN)
        .args(["a.csv", "b.csv"])
        .output()
        .expect("runs");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stderr.contains("compara compare a.csv B -k KEY"),
        "{stderr}"
    );
}

/// Standard input and a compressed file are named as the user gave them,
/// not as the temporary copy compara read; a forced delimiter that isn't in
/// the header says so.
#[test]
fn inputs_are_named_as_given() {
    use std::io::Write;
    let f = Fixture::new(10);
    let mut child = Command::new(BIN)
        .args([
            "compare",
            "-",
            f.path("a.csv").to_str().unwrap(),
            "-k",
            "id",
        ])
        .stdin(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("runs");
    drop(child.stdin.take());
    let out = child.wait_with_output().expect("runs");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(stderr.contains("standard input is empty"), "{stderr}");
    assert!(!stderr.contains("compara-"), "a temporary path: {stderr}");

    let gz = f.path("empty.csv.gz");
    let mut enc = flate2::write::GzEncoder::new(
        std::fs::File::create(&gz).unwrap(),
        flate2::Compression::default(),
    );
    enc.write_all(b"").unwrap();
    enc.finish().unwrap();
    let (code, _, stderr) = run(&["columns"], &gz);
    assert_eq!(code, 2, "{stderr}");
    assert!(stderr.contains("empty.csv.gz is empty"), "{stderr}");

    let (code, _, stderr) = run(&["head", "--delimiter", ";"], &f.path("a.csv"));
    assert_eq!(code, 0, "{stderr}");
    assert!(stderr.contains("--delimiter ';' is not in"), "{stderr}");
}

/// `--fail-on-dups` names itself when it sets the exit code.
#[test]
fn exit_3_says_it_came_from_fail_on_dups() {
    let f = Fixture::new(10);
    let one = f.path("one.csv");
    std::fs::write(&one, "id,name\n1,x\n").unwrap();
    let dups = f.path("dups.csv");
    std::fs::write(&dups, "id,name\n1,x\n1,y\n").unwrap();
    let out = Command::new(BIN)
        .args([
            "compare",
            one.to_str().unwrap(),
            dups.to_str().unwrap(),
            "-k",
            "id",
            "--fail-on-dups",
        ])
        .output()
        .expect("runs");
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(3), "{stderr}");
    assert!(
        stderr.contains("exit 3 (--fail-on-dups): duplicate keys, A 0, B 1"),
        "{stderr}"
    );
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("duplicate keys A 0, B 1"), "{stdout}");
}

/// An empty file and a binary one are said as what they are, by every
/// command and engine. A binary file used to be read as a CSV with one
/// strange column, and the error blamed the key.
#[test]
fn an_empty_or_binary_file_is_named_as_such() {
    let f = Fixture::new(10);
    let empty = f.path("empty.csv");
    std::fs::write(&empty, b"").unwrap();
    let binary = f.path("bad.bin");
    // Bytes like a compressed or executable file's: control bytes throughout.
    let bytes: Vec<u8> = (0..4000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    std::fs::write(&binary, bytes).unwrap();
    let a = f.path("a.csv");
    for (file, said) in [(&empty, "is empty"), (&binary, "binary data")] {
        for engine in ["turbo", "native", "sortmerge"] {
            let (code, _, stderr) = run(
                &[
                    "compare",
                    a.to_str().unwrap(),
                    "-k",
                    "id",
                    "--engine",
                    engine,
                ],
                file,
            );
            assert_eq!(code, 2, "{engine}: {stderr}");
            assert!(stderr.contains(said), "{engine}: {stderr}");
            assert!(!stderr.contains("key column"), "{engine}: {stderr}");
        }
        for cmd in ["columns", "head"] {
            let (code, stdout, stderr) = run(&[cmd], file);
            assert_eq!(code, 2, "{cmd}: {stderr}");
            assert!(stderr.contains(said), "{cmd}: {stderr}");
            assert!(stdout.is_empty(), "{cmd} printed {stdout:?}");
        }
    }
}

#[test]
fn both_commands_need_a_file() {
    let out = Command::new(BIN).arg("columns").output().expect("runs");
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("columns needs a file"));
    let out = Command::new(BIN).arg("head").output().expect("runs");
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("head needs a file"));
}
