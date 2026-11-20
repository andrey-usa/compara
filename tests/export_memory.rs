//! `--export-dir` on a pair where every row changed holds a block of rows, not
//! all of them.
//!
//! The export used to decode both sides of every changed row into strings and
//! then copy them again to write the file: about 5.8 KB a row at twenty
//! columns, so a 3M-row pair wanted over 17 GB and the process died on an
//! allocation it could not make. This runs the same shape small, under a data
//! limit the old code could not stay inside, and checks the file as well.
//!
//! Linux only: `RLIMIT_DATA` bounds private anonymous memory and leaves the
//! mapped input alone, which is what makes it a cap on what grows with rows.

#![cfg(target_os = "linux")]

use std::fs;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};

/// Prime, so that `i * 7919 % ROWS` visits every key once, out of order.
const ROWS: usize = 100_003;
const COLUMNS: usize = 12;
/// The cap in MiB: above what the block export needs, below what holding the
/// rows needed (the old code needed over 400 on this fixture).
const CAP_MB: usize = 200;

struct Fixture {
    dir: PathBuf,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!(
            "compara-export-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&dir).expect("temp dir");
        let mut header = String::from("account_id,txn_id");
        for c in 0..COLUMNS {
            header.push_str(&format!(",c{c}"));
        }
        header.push('\n');
        let (mut a, mut b) = (header.clone(), header);
        for i in 0..ROWS {
            let k = i * 7919 % ROWS;
            let mut row = format!("acc{},t{k:07}", k % 97);
            for c in 0..COLUMNS {
                row.push_str(&format!(",value {c} of row {k} padded out"));
            }
            a.push_str(&row);
            a.push('\n');
            // Every compared column differs, so every row is changed.
            b.push_str(&row.replace("padded out", "changed"));
            b.push('\n');
        }
        fs::write(dir.join("a.csv"), a).expect("write a");
        fs::write(dir.join("b.csv"), b).expect("write b");
        Fixture { dir }
    }

    /// Runs one export under the cap; returns the exit code and stderr.
    fn export(&self, engine: &str, threads: &str, cap_mb: usize) -> (Option<i32>, String) {
        let out = self.dir.join(format!("out-{engine}-{threads}"));
        let script = format!(
            "ulimit -d {}; exec {} compare {} {} -k account_id,txn_id --engine {engine} --threads {threads} --export-dir {}",
            cap_mb * 1024,
            env!("CARGO_BIN_EXE_compara"),
            self.dir.join("a.csv").display(),
            self.dir.join("b.csv").display(),
            out.display(),
        );
        let run = Command::new("bash")
            .arg("-c")
            .arg(script)
            .env("RUST_BACKTRACE", "0")
            .stdout(Stdio::null())
            .output()
            .expect("bash");
        (
            run.status.code(),
            String::from_utf8_lossy(&run.stderr).into_owned(),
        )
    }

    fn changed(&self, engine: &str, threads: &str) -> String {
        fs::read_to_string(self.dir.join(format!("out-{engine}-{threads}/changed.csv")))
            .expect("changed.csv")
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// The file the export must write: every row, in key order, with the two sides
/// of each compared column side by side.
fn expected() -> String {
    let mut header = String::from("account_id,txn_id");
    for c in 0..COLUMNS {
        header.push_str(&format!(",c{c} (A),c{c} (B)"));
    }
    let mut keys: Vec<usize> = (0..ROWS).collect();
    // Sorted as text on (account_id, txn_id), as the export is.
    keys.sort_by_key(|k| (format!("acc{}", k % 97), format!("t{k:07}")));
    let mut out = header + "\n";
    for k in keys {
        out.push_str(&format!("acc{},t{k:07}", k % 97));
        for c in 0..COLUMNS {
            out.push_str(&format!(
                ",value {c} of row {k} padded out,value {c} of row {k} changed"
            ));
        }
        out.push('\n');
    }
    out
}

#[test]
fn an_all_changed_export_stays_inside_a_memory_cap_and_is_complete() {
    let fx = Fixture::new();
    let want = expected();
    for engine in ["turbo", "sortmerge"] {
        for threads in ["1", "4"] {
            let (code, stderr) = fx.export(engine, threads, CAP_MB);
            // Exit 1 is "the files differ"; a signal is no code at all.
            assert_eq!(
                code,
                Some(1),
                "{engine} at {threads} threads under {CAP_MB} MiB: {stderr}"
            );
            assert!(
                fx.changed(engine, threads) == want,
                "{engine} at {threads} threads: changed.csv is not every row in key order"
            );
        }
    }
}
