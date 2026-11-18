//! Files built to break a reader, not to be compared.
//!
//! compara decodes Parquet itself, so every length, count and offset in a file
//! is the file's word until checked. Each case here is one way that word was
//! trusted -- found by mutating the fixtures in `formats/` and `fold/`, or
//! written by hand from the format -- and each must end the run with exit 2
//! and a message, quickly: not a panic (101), not an abort, not a hang, and not
//! gigabytes of memory on the way to a refusal.

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_compara");

fn fixture(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures")
        .join(name)
}

/// A file of this test's own, removed when it goes out of scope.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str, bytes: &[u8]) -> Scratch {
        let path =
            std::env::temp_dir().join(format!("compara-hostile-{}-{name}", std::process::id()));
        std::fs::write(&path, bytes).expect("write a scratch file");
        Scratch(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// Runs compara and insists on a refusal within `limit`.
fn refused(args: &[&Path], extra: &[&str], limit: Duration) -> String {
    let mut child = Command::new(BIN)
        .args(args)
        .args(extra)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("run compara");
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("wait for compara") {
            break status;
        }
        if started.elapsed() > limit {
            let _ = child.kill();
            panic!("compara {args:?} {extra:?} was still running after {limit:?}");
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    let mut stderr = String::new();
    std::io::Read::read_to_string(&mut child.stderr.take().expect("stderr"), &mut stderr)
        .expect("read stderr");
    assert_eq!(
        status.code(),
        Some(2),
        "compara {args:?} {extra:?}: {status}, stderr: {stderr}"
    );
    assert!(stderr.starts_with("error: "), "no message: {stderr}");
    assert!(
        !stderr.contains("internal:"),
        "a panic, reported as an error: {stderr}"
    );
    stderr
}

/// Every way in that reads rows: the first rows, and both compare paths with
/// the file on either side.
fn refused_reading(file: &Path) {
    let mate = fixture("formats/b_dict_snappy.parquet");
    let limit = Duration::from_secs(20);
    refused(&[Path::new("head"), file], &[], limit);
    for engine in ["auto", "turbo"] {
        refused(
            &[Path::new("compare"), file, &mate],
            &["-k", "id", "--summary", "--engine", engine],
            limit,
        );
        refused(
            &[Path::new("compare"), &mate, file],
            &["-k", "id", "--summary", "--engine", engine],
            limit,
        );
    }
}

/// `refused_reading`, and `columns` too: for a file whose footer is the problem.
fn refused_everywhere(file: &Path) {
    refused(&[Path::new("columns"), file], &[], Duration::from_secs(20));
    refused_reading(file);
}

fn varint(mut n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    loop {
        let b = (n & 0x7f) as u8;
        n >>= 7;
        if n == 0 {
            out.push(b);
            return out;
        }
        out.push(b | 0x80);
    }
}

/// A Parquet file of nothing but this footer.
fn with_footer(meta: &[u8]) -> Vec<u8> {
    let mut out = b"PAR1".to_vec();
    out.extend_from_slice(meta);
    out.extend_from_slice(&(meta.len() as u32).to_le_bytes());
    out.extend_from_slice(b"PAR1");
    out
}

#[test]
fn a_footer_nested_past_any_writer_does_not_overflow_the_stack() {
    // Field 10 of the file metadata, unknown to the reader, holding a struct
    // in a struct, 200,000 deep. Skipping it recursed once per level.
    let depth = 200_000;
    let mut meta = vec![0xAC];
    meta.extend(std::iter::repeat_n(0x1C, depth));
    meta.extend(std::iter::repeat_n(0x00, depth + 1));
    let file = Scratch::new("deep.parquet", &with_footer(&meta));
    refused_everywhere(&file.0);
}

#[test]
fn a_list_of_bools_cannot_spin_without_reading() {
    // A thousand lists of two billion bools each. A bool in a list takes a
    // byte, but the skip took none, so this looped for hours on a few bytes.
    let mut meta = vec![0xA9, 0xF9];
    meta.extend(varint(1000));
    for _ in 0..1000 {
        meta.push(0xF1);
        meta.extend(varint(1 << 31));
    }
    meta.push(0x00);
    let file = Scratch::new("bools.parquet", &with_footer(&meta));
    refused_everywhere(&file.0);
}

#[test]
fn a_row_group_count_the_footer_cannot_hold_sizes_nothing() {
    // version 1, then field 4 (row groups): a list of 2^32 - 1 structs, in a
    // 14-byte footer. The columnar reader allocated 100 GB for it and aborted.
    let mut meta = vec![0x15, 0x02, 0x39, 0xFC];
    meta.extend(varint(0xFFFF_FFFF));
    meta.push(0x00);
    let file = Scratch::new("groups.parquet", &with_footer(&meta));
    refused_everywhere(&file.0);
}

#[test]
fn a_string_length_that_wraps_the_cursor_is_refused() {
    let mut meta = vec![0xA8];
    meta.extend(varint(u64::MAX - 1));
    meta.push(0x00);
    let file = Scratch::new("wrap.parquet", &with_footer(&meta));
    refused_everywhere(&file.0);
}

#[test]
fn a_negative_page_size_does_not_wrap_the_bounds_check() {
    // A mutant of b_dict_snappy.parquet: a page header whose compressed size
    // is negative, which cast to usize wrapped past the check that the page
    // fits in the file.
    let mate = fixture("formats/a_dict_snappy.parquet");
    let file = fixture("hostile/negative_page_size.parquet");
    refused(
        &[Path::new("compare"), &file, &mate],
        &["-k", "id", "--summary"],
        Duration::from_secs(20),
    );
}

#[test]
fn a_delta_header_claiming_billions_of_values_sizes_nothing() {
    // A mutant of a_delta_v2.parquet whose DELTA_BINARY_PACKED header asks
    // for a miniblock of 10^16 values: the reader reserved it and aborted.
    refused_reading(&fixture("hostile/delta_header.parquet"));
}

#[test]
fn a_zstd_page_cannot_expand_past_its_header() {
    // One page of 36 KB that its header says decompresses to 36 KB, and whose
    // zstd frame decompresses to 256 MB of zeros. The reader decompressed all
    // of it before comparing the length with the header's.
    let stderr = refused(
        &[Path::new("head"), &fixture("hostile/zstd_bomb.parquet")],
        &[],
        Duration::from_secs(20),
    );
    assert!(
        !stderr.contains("268435456"),
        "the whole bomb was decompressed: {stderr}"
    );
}

#[test]
fn a_gzip_bomb_is_refused_before_it_fills_the_disk() {
    // 18 gzip members of 64 MiB of zeros each: 1.1 MiB that expands to 1.1 GiB,
    // a thousand times and more. It was expanded whole before reading.
    use std::io::Write;
    let mut member = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    let zeros = vec![0u8; 1 << 20];
    for _ in 0..64 {
        member.write_all(&zeros).unwrap();
    }
    let member = member.finish().unwrap();
    let mut bomb = Vec::new();
    for _ in 0..18 {
        bomb.extend_from_slice(&member);
    }
    let file = Scratch::new("bomb.csv.gz", &bomb);
    let stderr = refused(&[Path::new("head"), &file.0], &[], Duration::from_secs(60));
    assert!(stderr.contains("decompression bomb"), "{stderr}");
}

#[test]
fn a_page_that_claims_more_values_than_its_bytes_hold_is_refused() {
    // Found by fuzzing: 9.7 KB whose data page claims 1.47 billion values in
    // 1.5 KB. Both engines sized their per-value buffers from that count, and
    // 2.8 GB was reserved before the page turned out to be short. Refused
    // now as the decompression bomb it is, before anything is allocated.
    let file = fixture("hostile/page_values_bomb.parquet");
    refused_reading(&file);
    let stderr = refused(
        &[Path::new("compare"), &file, &file],
        &["-k", "id", "--summary"],
        Duration::from_secs(20),
    );
    assert!(stderr.contains("decompression bomb"), "{stderr}");
}

#[test]
fn a_snappy_stream_is_held_to_the_size_its_page_header_gives() {
    // Found by fuzzing: 2.8 KB with a snappy page of 11 bytes, by its header,
    // whose stream opens with "11.5 GB follow". The columnar reader took the
    // stream's word and asked for the room: an abort under a memory limit,
    // "out of memory" where the machine refused, and where it did not, 11 GB
    // of address space on the way to "will not decompress".
    // Compared with itself, so that every column is read: the page is not in
    // the key column, and a mate without that column never opens it.
    let file = fixture("hostile/snappy_length.parquet");
    let limit = Duration::from_secs(20);
    refused(&[Path::new("head"), &file], &[], limit);
    refused(
        &[Path::new("compare"), &file, &file],
        &["-k", "id", "--summary", "--engine", "turbo"],
        limit,
    );
    let stderr = refused(
        &[Path::new("compare"), &file, &file],
        &["-k", "id", "--summary"],
        limit,
    );
    assert!(
        stderr.contains("declares 11542724607 bytes where its header says 11"),
        "{stderr}"
    );
}

#[test]
fn a_bit_width_wider_than_its_values_is_refused() {
    // Found by fuzzing: a page whose packed values claim 110 bits each. The
    // shift past 64 panicked in a debug build and, in a release one, read
    // garbage; a delta page's widths past 32 also lost their high bits.
    let file = fixture("hostile/bit_width_overflow.parquet");
    refused_reading(&file);
    let stderr = refused(
        &[Path::new("compare"), &file, &file],
        &["-k", "id", "--summary", "--engine", "turbo"],
        Duration::from_secs(20),
    );
    assert!(stderr.contains("bits wide"), "{stderr}");
}

/// Keys built to share one hash under the FNV the engines used before
/// `keyhash`: a second word that cancels the first (`w2 = f(w1) ^ C`). Every
/// row landed in one probe chain, and 40,000 rows took 40 s.
fn colliding_keys(n: usize) -> Vec<[u8; 16]> {
    const PRIME: u64 = 0x100_0000_01b3;
    const BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    let f = |h: u64, w: u64| {
        let h = (h ^ w).wrapping_mul(PRIME);
        h ^ (h >> 29)
    };
    let c = f(BASIS, u64::from_le_bytes(*b"AAAAAAAA")) ^ u64::from_le_bytes(*b"BBBBBBBB");
    let fits = |b: &[u8; 8]| {
        b.iter()
            .all(|&x| (0x21..=0xfe).contains(&x) && x != b',' && x != b'"')
    };
    let mut out = Vec::with_capacity(n);
    for i in 0u64.. {
        if out.len() == n {
            break;
        }
        let w1: [u8; 8] = std::array::from_fn(|k| b'A' + ((i >> (4 * k)) & 15) as u8);
        let w2 = (f(BASIS, u64::from_le_bytes(w1)) ^ c).to_le_bytes();
        if fits(&w2) {
            let mut key = [0u8; 16];
            key[..8].copy_from_slice(&w1);
            key[8..].copy_from_slice(&w2);
            out.push(key);
        }
    }
    out
}

#[test]
fn keys_built_to_collide_do_not_make_a_compare_quadratic() {
    let keys = colliding_keys(20_000);
    let file = |changed: bool| {
        let mut b = b"id,v\n".to_vec();
        for (i, k) in keys.iter().enumerate() {
            b.extend_from_slice(k);
            b.extend_from_slice(
                format!(",{}\n", i + usize::from(changed && i % 100 == 0)).as_bytes(),
            );
        }
        b
    };
    let (a, b) = (
        Scratch::new("flood_a.csv", &file(false)),
        Scratch::new("flood_b.csv", &file(true)),
    );
    // turbo, the default: these keys aren't UTF-8, which the native engine
    // refuses before hashing; it hashes by the same `keyhash`.
    let started = Instant::now();
    let out = Command::new(BIN)
        .args(["compare", "-k", "id", "--engine", "turbo"])
        .arg(&a.0)
        .arg(&b.0)
        .output()
        .expect("run compara");
    let took = started.elapsed();
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("matched 20,000 (changed 200)"), "{stdout}");
    // Ordinary keys: well under a second, debug build included. Hashed the
    // old way these rows took about 10 s in a release build.
    assert!(took < Duration::from_secs(5), "took {took:?}");
}

/// A JSON file whose every object names a column of its own is refused for
/// what it is, and quickly. Its columns are all its objects' keys, so
/// 60,000 such objects are 60,001 columns and 3.6 billion cells in two
/// megabytes of file: the compare took seven seconds, and two minutes at
/// 400,000. Naming the columns to compare is the way through, and is said.
#[test]
fn a_json_file_with_a_key_of_its_own_in_every_object_is_refused() {
    let table = |value: &str| {
        let mut text = String::new();
        for i in 0..60_000 {
            text.push_str(&format!("{{\"id\":\"{i}\",\"k{i}\":\"{value}\"}}\n"));
        }
        text
    };
    let a = Scratch::new("scattered-a.ndjson", table("x").as_bytes());
    let b = Scratch::new("scattered-b.ndjson", table("y").as_bytes());
    let said = refused(
        &[Path::new("compare"), &a.0, &b.0],
        &["-k", "id"],
        Duration::from_secs(20),
    );
    assert!(said.contains("is not one table"), "{said}");
    assert!(said.contains("--compare"), "{said}");

    // With the columns named there is a table to compare, and it is.
    let out = Command::new(BIN)
        .args(["compare", "-k", "id", "-c", "k5,k7"])
        .args([&a.0, &b.0])
        .output()
        .expect("run compara");
    assert_eq!(out.status.code(), Some(1));
    let counts = String::from_utf8_lossy(&out.stdout);
    assert!(counts.contains("(changed 2)"), "{counts}");
}

/// The names a JSON file's later objects bring are held to one budget for
/// the file, whatever the thread count, and to a quarter of `--memory` when
/// that is less: each thread had a 64 MB ceiling of its own, so a file's
/// names could take 64 MB a core, and `--memory` was not asked. Eight names
/// of a megabyte each: four pieces of two fit under any one thread's share of
/// a 4 MB budget, and only the file's total says it does not.
#[test]
fn a_json_file_s_key_names_have_one_budget_across_threads() {
    let pad = "Q".repeat(1 << 20);
    let mut text = String::new();
    for i in 0..8 {
        text.push_str(&format!("{{\"id\":{i},\"{pad}{i}\":1}}\n"));
    }
    let file = Scratch::new("big-names.ndjson", text.as_bytes());
    for threads in ["1", "4"] {
        let said = refused(
            &[Path::new("compare"), &file.0, &file.0],
            &["-k", "id", "--threads", threads, "--memory", "16M"],
            Duration::from_secs(20),
        );
        assert!(said.contains("4 MB of different keys"), "{said}");
        assert!(said.contains("--memory"), "{said}");
    }
    // Under the ceiling a file's own budget allows, the same file compares.
    let out = Command::new(BIN)
        .args(["compare", "-k", "id", "--threads", "4"])
        .args([&file.0, &file.0])
        .output()
        .expect("run compara");
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// A table of twenty thousand columns is compared in the time its bytes
/// take: each column's name used to be searched for in the list of all of
/// them, at five places, so the time grew with the square of the width (17
/// seconds at sixty thousand columns and one row).
#[test]
fn a_very_wide_table_is_not_quadratic_in_its_columns() {
    let width = 20_000;
    let header: Vec<String> = (0..width).map(|i| format!("c{i}")).collect();
    let row = |v: &str| vec![v; width].join(",");
    let a = Scratch::new(
        "wide-a.csv",
        format!("id,{}\n1,{}\n", header.join(","), row("x")).as_bytes(),
    );
    let b = Scratch::new(
        "wide-b.csv",
        format!("id,{}\n1,{}\n", header.join(","), row("y")).as_bytes(),
    );
    let started = Instant::now();
    let out = Command::new(BIN)
        .args(["compare", "-k", "id"])
        .args([&a.0, &b.0])
        .output()
        .expect("run compara");
    assert_eq!(out.status.code(), Some(1));
    assert!(String::from_utf8_lossy(&out.stdout).contains("(changed 1)"));
    // Two seconds at the old cost on a fast machine; a tenth of one now.
    assert!(
        started.elapsed() < Duration::from_secs(20),
        "{:?}",
        started.elapsed()
    );
}
