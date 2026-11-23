//! The generated files are shared with the Python, TypeScript, Java and Go
//! generators byte for byte, so a benchmark number from any of them is directly
//! comparable.

use std::collections::HashSet;
use std::fs;
use std::process::Command;

use compara::gendata::{
    COLUMNS, Compression, Format, Keys, ParquetLayout, generate, generate_keys, parse_rows,
};

#[test]
fn the_shape_and_drift_are_what_the_recipe_says() {
    let dir = std::env::temp_dir().join(format!("compara-gen-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("temp dir");
    let a = dir.join("a.csv");
    let b = dir.join("b.csv");
    generate(10_000, &a, &b, 7).expect("generate");

    let a_text = fs::read_to_string(&a).expect("read a");
    let b_text = fs::read_to_string(&b).expect("read b");
    let a_lines: Vec<&str> = a_text.trim_end().lines().collect();
    let b_lines: Vec<&str> = b_text.trim_end().lines().collect();

    assert_eq!(a_lines[0], COLUMNS.join(","));
    assert_eq!(a_lines[1].split(',').count(), 20);
    // 10k rows, 1 in 1000 removed, 1 duplicate row added to A, 10 added to B, 1 duplicated in B.
    assert_eq!(a_lines.len(), 10_002);
    assert_eq!(b_lines.len(), 10_002);

    // Money never passes through a float, so a two-decimal amount is the same
    // digits in every implementation regardless of its rounding rule.
    let index = |name: &str| COLUMNS.iter().position(|c| *c == name).expect("column");
    for line in a_lines.iter().chain(&b_lines).skip(1).take(500) {
        let fields: Vec<&str> = line.split(',').collect();
        for column in ["amount", "fee", "balance"] {
            let value = fields[index(column)];
            let (_, decimals) = value
                .split_once('.')
                .unwrap_or_else(|| panic!("{column}={value}"));
            assert_eq!(decimals.len(), 2, "{column}={value} is not 2dp");
        }
        let rate = fields[index("rate")];
        assert_eq!(
            rate.split_once('.').expect("rate").1.len(),
            4,
            "rate={rate} is not 4dp"
        );
    }

    let _ = fs::remove_dir_all(dir);
}

#[test]
fn parse_rows_accepts_the_usual_shorthands() {
    for (input, want) in [
        ("10000", 10_000i64),
        ("10k", 10_000),
        ("1m", 1_000_000),
        ("2.5M", 2_500_000),
        ("1g", 1_000_000_000),
        ("1_000", 1_000),
    ] {
        assert_eq!(parse_rows(input).expect(input), want, "parse_rows({input})");
    }
    assert!(parse_rows("many").is_err());
}

/// The key of a CSV line: its first two fields.
fn key(line: &str) -> &str {
    let second = line.find(',').expect("a field") + 1;
    let end = second + line[second..].find(',').expect("a second field");
    &line[..end]
}

#[test]
fn unique_keys_is_the_default_pair_without_its_repeated_rows() {
    let dir = std::env::temp_dir().join(format!("compara-gen-unique-{}", std::process::id()));
    fs::create_dir_all(&dir).expect("temp dir");
    let read = |name: &str| fs::read_to_string(dir.join(name)).expect("read");
    let layout = ParquetLayout {
        compression: Compression::None,
        ..ParquetLayout::default()
    };
    for (keys, a, b) in [
        (Keys::Repeated, "r_a.csv", "r_b.csv"),
        (Keys::Unique, "u_a.csv", "u_b.csv"),
    ] {
        generate_keys(
            20_000,
            &dir.join(a),
            &dir.join(b),
            7,
            Format::Csv,
            layout,
            keys,
        )
        .expect("generate");
    }
    let (ra, rb, ua, ub) = (
        read("r_a.csv"),
        read("r_b.csv"),
        read("u_a.csv"),
        read("u_b.csv"),
    );

    // The default pair repeats keys; the unique one never does, in either file.
    for (text, repeats) in [(&ra, true), (&rb, true), (&ua, false), (&ub, false)] {
        let mut seen = HashSet::new();
        let twice = text
            .lines()
            .skip(1)
            .filter(|l| !seen.insert(key(l)))
            .count();
        assert_eq!(twice > 0, repeats, "repeated keys: {twice}");
    }
    // 20k rows a side; 20 removed from B and 20 added to it.
    assert_eq!(ua.lines().count(), 20_001);
    assert_eq!(ub.lines().count(), 20_001);

    // Every other row is the default pair's, in its order: the repeated keys
    // are the only difference, so a run on either pair measures the same data.
    let first_only = |text: &str| -> Vec<String> {
        let mut seen = HashSet::new();
        text.lines()
            .filter(|l| seen.insert(key(l).to_string()))
            .map(str::to_string)
            .collect()
    };
    assert_eq!(
        first_only(&ra),
        ua.lines().map(str::to_string).collect::<Vec<_>>()
    );
    assert_eq!(
        first_only(&rb),
        ub.lines().map(str::to_string).collect::<Vec<_>>()
    );

    // The command line writes the same bytes with --unique-keys, and the
    // default pair without it.
    let tool = env!("CARGO_BIN_EXE_compara-gen-data");
    for (flag, prefix, want_a) in [(Some("--unique-keys"), "cu", &ua), (None, "cr", &ra)] {
        let mut cmd = Command::new(tool);
        cmd.args(["--rows", "20k", "--prefix", prefix, "--out-dir"])
            .arg(&dir);
        cmd.args(flag);
        let out = cmd.output().expect("run compara-gen-data");
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(&read(&format!("{prefix}_a.csv")), want_a, "{flag:?}");
    }
    assert_eq!(read("cu_b.csv"), ub);
    let bad = Command::new(tool)
        .args(["--unique-keys=yes"])
        .output()
        .expect("run");
    assert!(!bad.status.success());

    let _ = fs::remove_dir_all(dir);
}
