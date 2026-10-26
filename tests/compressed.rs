//! A gzip or zstd side, expanded in memory where it fits and to a temporary
//! file where it does not: the same answer either way.

use std::path::PathBuf;
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_compara");

struct Dir(PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn table(rows: usize, changed_every: usize) -> String {
    let mut s = String::from("id,name,qty\n");
    for i in 0..rows {
        let qty = if changed_every > 0 && i % changed_every == 0 {
            i + 1
        } else {
            i
        };
        s.push_str(&format!("{i},name-{i},{qty}\n"));
    }
    s
}

/// (counts, stderr) for comparing the pair with `memory` as COMPARA_MEMORY.
fn compare(dir: &Dir, memory: Option<&str>) -> (serde_json::Value, String) {
    let json = dir
        .0
        .join(format!("out-{}.json", memory.unwrap_or("probed")));
    let mut cmd = Command::new(BIN);
    cmd.args(["compare"])
        .arg(dir.0.join("a.csv.gz"))
        .arg(dir.0.join("b.csv.zst"))
        .args(["-k", "id", "--json"])
        .arg(&json);
    match memory {
        Some(m) => cmd.env("COMPARA_MEMORY", m),
        None => cmd.env_remove("COMPARA_MEMORY"),
    };
    let out = cmd.output().expect("run compara");
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    assert!(matches!(out.status.code(), Some(0 | 1)), "{stderr}");
    let report: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&json).unwrap()).unwrap();
    (report["counts"].clone(), stderr)
}

#[test]
fn an_expanded_side_gives_the_same_answer_in_memory_and_on_disk() {
    use std::io::Write;
    let dir = Dir(std::env::temp_dir().join(format!("compara-compressed-{}", std::process::id())));
    std::fs::create_dir_all(&dir.0).unwrap();
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gz.write_all(table(50_000, 0).as_bytes()).unwrap();
    std::fs::write(dir.0.join("a.csv.gz"), gz.finish().unwrap()).unwrap();
    let b = zstd::encode_all(table(50_001, 7).as_bytes(), 3).unwrap();
    std::fs::write(dir.0.join("b.csv.zst"), b).unwrap();

    let (in_memory, stderr) = compare(&dir, None);
    if cfg!(target_os = "linux") {
        assert!(stderr.contains("in memory"), "{stderr}");
    }
    // A budget of 4 KB: a quarter of it is far less than either side, so both
    // start in memory and move to a temporary file partway.
    let (on_disk, stderr) = compare(&dir, Some("4K"));
    assert!(!stderr.contains("in memory"), "{stderr}");
    assert_eq!(in_memory, on_disk);
    assert_eq!(in_memory["changed"], 7143);
    assert_eq!(in_memory["added"], 1);
}
