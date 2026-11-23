//! Remote inputs: the same pair read from a local stand-in for object storage
//! (`tests/object_server.py`) must give the answer the local files give --
//! counts, column statistics and every exported row -- over S3 and plain HTTP,
//! gzip and zstd, Parquet fetched a column at a time, and a server that does
//! not do ranges. Skipped where there is no Python to run the server.
#![cfg(feature = "cloud")]

use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

fn id() -> u32 {
    use std::sync::atomic::{AtomicU32, Ordering};
    static NEXT: AtomicU32 = AtomicU32::new(0);
    NEXT.fetch_add(1, Ordering::Relaxed)
}

fn python() -> Option<&'static str> {
    ["python3", "python"].into_iter().find(|p| {
        Command::new(p)
            .arg("--version")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

struct Server {
    child: Child,
    url: String,
    log: PathBuf,
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl Server {
    fn start(root: &Path, extra: &[&str]) -> Option<Server> {
        let py = python()?;
        let log = root.join(format!("requests-{}.jsonl", id()));
        let mut child = Command::new(py)
            .arg(concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/tests/object_server.py"
            ))
            .arg("--root")
            .arg(root)
            .arg("--log")
            .arg(&log)
            .args(extra)
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .ok()?;
        let mut line = String::new();
        BufReader::new(child.stdout.take()?)
            .read_line(&mut line)
            .ok()?;
        let url = line.trim().strip_prefix("LISTENING ")?.to_string();
        Some(Server { child, url, log })
    }

    /// Waits until every request the server has taken is in its log. It
    /// writes a request's line after the answer, so the compara that asked
    /// can have it all and exit first: on a macOS runner the last parts of a
    /// compare were counted against the `columns` run that followed it.
    fn settle(&self) {
        let host = self.url.strip_prefix("http://").unwrap();
        let mut s = std::net::TcpStream::connect(host).unwrap();
        write!(
            s,
            "GET /__settled HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n"
        )
        .unwrap();
        let mut answer = String::new();
        s.read_to_string(&mut answer).unwrap();
        assert!(
            answer.starts_with("HTTP/1.1 200") || answer.starts_with("HTTP/1.0 200"),
            "the object server still had a request unlogged after 10 s: {answer}"
        );
    }

    /// Each request's Range header (or what the log says in its place) and
    /// the bytes sent for it, from the server's log.
    fn requests(&self) -> Vec<(String, u64)> {
        self.settle();
        std::fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .map(|v| {
                (
                    v["range"].as_str().unwrap_or("").to_string(),
                    v["sent"].as_u64().unwrap_or(0),
                )
            })
            .collect()
    }

    /// The bytes the server has sent, from its request log.
    fn sent(&self) -> u64 {
        self.settle();
        std::fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
            .map(|v| v["sent"].as_u64().unwrap_or(0))
            .sum()
    }
}

struct Dir(PathBuf);

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn fixture(rows: &str, flags: &[&str]) -> Dir {
    let dir = std::env::temp_dir().join(format!("compara-remote-{}-{}", std::process::id(), id()));
    let bucket = dir.join("bkt");
    std::fs::create_dir_all(&bucket).unwrap();
    for f in [&[][..], flags] {
        let out = Command::new(env!("CARGO_BIN_EXE_compara-gen-data"))
            .args(["--rows", rows, "--out-dir"])
            .arg(&bucket)
            .args(["--prefix", "t"])
            .args(f)
            .output()
            .unwrap();
        assert!(out.status.success(), "{out:?}");
    }
    Dir(dir)
}

/// Runs a comparison and returns (exit code, stderr, the summary without its
/// run facts, the exported rows).
fn compare(
    a: &str,
    b: &str,
    args: &[&str],
    env: &[(&str, &str)],
    stdin: Option<&[u8]>,
) -> (i32, String, String, String) {
    compare_by(
        Command::new(env!("CARGO_BIN_EXE_compara")),
        a,
        b,
        args,
        env,
        stdin,
    )
}

/// The program run where the system refuses every thread it asks for: each
/// asks for a stack (`RUST_MIN_STACK`) larger than the data limit.
fn without_threads() -> Command {
    with_stacks_of(8u64 << 30)
}

/// The program run where the system gives it about `n` threads and refuses
/// more: each asks for a stack of a little under 1/n of the data limit, which
/// leaves the rest of the process room for its heap.
fn with_threads(n: u64) -> Command {
    with_stacks_of((4u64 << 30) * 10 / (n * 10 + 6))
}

/// The program under a 4 GiB data limit, which thread stacks count against,
/// each thread asking for a stack of `bytes`.
fn with_stacks_of(bytes: u64) -> Command {
    let mut cmd = Command::new("bash");
    cmd.args(["-c", "ulimit -d 4194304; exec \"$0\" \"$@\""])
        .arg(env!("CARGO_BIN_EXE_compara"))
        .env("RUST_MIN_STACK", bytes.to_string());
    cmd
}

/// [`compare`], run as `cmd`.
fn compare_by(
    cmd: Command,
    a: &str,
    b: &str,
    args: &[&str],
    env: &[(&str, &str)],
    stdin: Option<&[u8]>,
) -> (i32, String, String, String) {
    compare_within(cmd, a, b, args, env, stdin, None)
}

/// [`compare_by`], killed if it has not exited within `limit`: exit -2, and
/// stderr says so.
fn compare_within(
    mut cmd: Command,
    a: &str,
    b: &str,
    args: &[&str],
    env: &[(&str, &str)],
    stdin: Option<&[u8]>,
    limit: Option<std::time::Duration>,
) -> (i32, String, String, String) {
    let out_dir = std::env::temp_dir().join(format!(
        "compara-remote-out-{}-{}",
        std::process::id(),
        id()
    ));
    let json = out_dir.join("summary.json");
    let export = out_dir.join("export");
    std::fs::create_dir_all(&out_dir).unwrap();
    cmd.args(["compare", a, b])
        .args(args)
        .arg("--json")
        .arg(&json)
        .arg("--export-dir")
        .arg(&export)
        .envs(env.iter().copied())
        .stdin(if stdin.is_some() {
            Stdio::piped()
        } else {
            Stdio::null()
        })
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = cmd.spawn().unwrap();
    if let Some(bytes) = stdin {
        child.stdin.take().unwrap().write_all(bytes).unwrap();
    }
    let mut killed = None;
    if let Some(limit) = limit {
        let started = std::time::Instant::now();
        // The pipes are drained by wait_with_output after; a comparison's
        // stdout and stderr are a few lines, well inside a pipe's buffer.
        while child.try_wait().unwrap().is_none() {
            if started.elapsed() > limit {
                let _ = child.kill();
                killed = Some(limit);
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
    let mut out = child.wait_with_output().unwrap();
    let code = match killed {
        Some(limit) => {
            out.stderr
                .extend(format!("\n(killed: no exit within {limit:?})").bytes());
            -2
        }
        None => out.status.code().unwrap_or(-1),
    };
    let mut summary = String::new();
    let mut rows = String::new();
    if let Ok(text) = std::fs::read_to_string(&json) {
        let mut v: serde_json::Value = serde_json::from_str(&text).unwrap();
        v.as_object_mut().unwrap().remove("meta");
        summary = v.to_string();
        let mut names: Vec<_> = std::fs::read_dir(&export)
            .map(|d| d.filter_map(|e| e.ok().map(|e| e.path())).collect())
            .unwrap_or_default();
        names.sort();
        for n in names {
            rows.push_str(&std::fs::read_to_string(&n).unwrap());
        }
    }
    let _ = std::fs::remove_dir_all(&out_dir);
    (
        code,
        String::from_utf8_lossy(&out.stderr).into_owned(),
        summary,
        rows,
    )
}

fn s3_env(url: &str) -> Vec<(&'static str, String)> {
    vec![
        ("AWS_ENDPOINT_URL", url.to_string()),
        ("AWS_ACCESS_KEY_ID", "test".into()),
        ("AWS_SECRET_ACCESS_KEY", "test".into()),
        ("AWS_REGION", "us-east-1".into()),
    ]
}

fn borrowed<'a>(env: &'a [(&'static str, String)]) -> Vec<(&'static str, &'a str)> {
    env.iter().map(|(k, v)| (*k, v.as_str())).collect()
}

/// A case: A, B, the environment, and the local answer it must give.
type Case<'a> = (
    String,
    String,
    Vec<(&'a str, &'a str)>,
    &'a (i32, String, String, String),
);

const KEY: [&str; 4] = ["-k", "account_id,txn_id", "-i", "updated_at"];

/// Parquet in every layout the generator writes, over S3, a column at a time:
/// the answer is the local one, with each reader, and with only some columns.
#[test]
fn parquet_over_s3_is_the_local_answer() {
    for flags in [
        &["--format", "parquet", "--compression", "none"][..],
        &[
            "--format",
            "parquet",
            "--compression",
            "zstd",
            "--row-group-size",
            "512",
        ][..],
        &[
            "--format",
            "parquet",
            "--dict-limit",
            "175",
            "--row-group-size",
            "300",
        ][..],
        &["--format", "parquet", "--dict-limit", "1"][..],
    ] {
        let f = fixture("20k", flags);
        let Some(server) = Server::start(&f.0, &[]) else {
            eprintln!("skipping: no python to run the object server");
            return;
        };
        let env = s3_env(&server.url);
        let local = |n: &str| f.0.join("bkt").join(n).to_string_lossy().into_owned();
        for extra in [
            &[][..],
            &["-c", "amount,note"][..],
            &["--engine", "turbo"][..],
        ] {
            let args: Vec<&str> = KEY.iter().copied().chain(extra.iter().copied()).collect();
            let want = compare(
                &local("t_a.parquet"),
                &local("t_b.parquet"),
                &args,
                &[],
                None,
            );
            let got = compare(
                "s3://bkt/t_a.parquet",
                "s3://bkt/t_b.parquet",
                &args,
                &borrowed(&env),
                None,
            );
            assert_eq!(
                want.0, got.0,
                "{flags:?} {extra:?}: exit code; stderr {}",
                got.1
            );
            assert_eq!(want.2, got.2, "{flags:?} {extra:?}: summary");
            assert_eq!(want.3, got.3, "{flags:?} {extra:?}: exported rows");
            assert!(got.1.contains("downloaded"), "{}", got.1);
        }
        // Two compared columns of twenty: most of the file stays where it is.
        // Small row groups put the wanted chunks closer together than it pays
        // to skip, and are fetched nearly whole by design.
        if flags.contains(&"--row-group-size") {
            continue;
        }
        let before = server.sent();
        let args: Vec<&str> = ["-k", "account_id,txn_id", "-c", "amount"].to_vec();
        compare(
            "s3://bkt/t_a.parquet",
            "s3://bkt/t_b.parquet",
            &args,
            &borrowed(&env),
            None,
        );
        let size = std::fs::metadata(f.0.join("bkt/t_a.parquet"))
            .unwrap()
            .len()
            + std::fs::metadata(f.0.join("bkt/t_b.parquet"))
                .unwrap()
                .len();
        let sent = server.sent() - before;
        assert!(
            sent < size * 3 / 4,
            "{flags:?}: fetched {sent} of {size} bytes for three columns"
        );
    }
}

/// CSV and NDJSON over plain HTTP and S3, compressed and not, from a server
/// that does ranges and one that does not, and from standard input.
#[test]
fn text_over_http_and_s3_is_the_local_answer() {
    let f = fixture("20k", &["--format", "ndjson"]);
    let bucket = f.0.join("bkt");
    let csv_a = std::fs::read(bucket.join("t_a.csv")).unwrap();
    {
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        gz.write_all(&csv_a).unwrap();
        std::fs::write(bucket.join("t_a.csv.gz"), gz.finish().unwrap()).unwrap();
        let nd = std::fs::read(bucket.join("t_b.ndjson")).unwrap();
        std::fs::write(
            bucket.join("t_b.ndjson.zst"),
            zstd::stream::encode_all(&nd[..], 3).unwrap(),
        )
        .unwrap();
    }
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    let want_nd = compare(&local("t_a.ndjson"), &local("t_b.ndjson"), &KEY, &[], None);
    assert!(!want.2.is_empty());
    for extra in [&[][..], &["--no-ranges"][..]] {
        let Some(server) = Server::start(&f.0, extra) else {
            eprintln!("skipping: no python to run the object server");
            return;
        };
        let u = &server.url;
        let env = s3_env(u);
        let mut cases: Vec<Case> = vec![
            (
                format!("{u}/bkt/t_a.csv.gz"),
                format!("{u}/bkt/t_b.csv"),
                vec![],
                &want,
            ),
            (
                format!("{u}/bkt/t_a.ndjson"),
                format!("{u}/bkt/t_b.ndjson.zst"),
                vec![],
                &want_nd,
            ),
        ];
        // S3 always serves ranges; a server that does not is a plain HTTP one.
        if extra.is_empty() {
            cases.push((
                "s3://bkt/t_a.csv".into(),
                "s3://bkt/t_b.csv".into(),
                borrowed(&env),
                &want,
            ));
        }
        for (a, b, env, want) in cases {
            let got = compare(&a, &b, &KEY, &env, None);
            assert_eq!(want.0, got.0, "{extra:?} {a} {b}: exit; stderr {}", got.1);
            assert_eq!(want.2, got.2, "{extra:?} {a} {b}: summary");
            assert_eq!(want.3, got.3, "{extra:?} {a} {b}: rows");
            // A server without ranges sends the object whole: it goes to disk
            // as it comes, not into memory.
            if !extra.is_empty() && b.ends_with("t_b.csv") {
                assert!(
                    got.1.contains("whole (the server sends no ranges)"),
                    "{}",
                    got.1
                );
            }
        }
    }
    // Standard input, gzipped, against a local file.
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    gz.write_all(&csv_a).unwrap();
    let got = compare(
        "-",
        &local("t_b.csv"),
        &KEY,
        &[],
        Some(&gz.finish().unwrap()),
    );
    assert_eq!(
        (want.0, &want.2, &want.3),
        (got.0, &got.2, &got.3),
        "stdin: {}",
        got.1
    );
}

/// Where the system refuses every thread, a URL is read on the calling thread
/// alone, to the local answer: over HTTP by a name, looked up on that thread
/// too, and over S3 by one. With threads, the runtime and the name lookup
/// would each have asked tokio for one, and tokio panics when refused.
#[test]
fn urls_are_read_on_one_thread_when_no_other_can_be_had() {
    if !cfg!(target_os = "linux") {
        eprintln!("skipping: thread stacks count against `ulimit -d` on Linux");
        return;
    }
    let f = fixture("20k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    assert!(!want.2.is_empty());
    let Some(server) = Server::start(&f.0, &[]) else {
        eprintln!("skipping: no python to run the object server");
        return;
    };
    let named = server.url.replace("127.0.0.1", "localhost");
    let env = s3_env(&named);
    let s3 = borrowed(&env);
    for (a, b, env) in [
        (format!("{named}/bkt/t_a.csv"), local("t_b.csv"), &[][..]),
        (
            "s3://bkt/t_a.csv".into(),
            "s3://bkt/t_b.csv".into(),
            &s3[..],
        ),
    ] {
        let got = compare_by(without_threads(), &a, &b, &KEY, env, None);
        assert_eq!(want.0, got.0, "{a} {b}: exit; stderr {}", got.1);
        assert_eq!(want.2, got.2, "{a} {b}: summary");
        assert_eq!(want.3, got.3, "{a} {b}: rows");
    }
}

/// Where the system gives some threads but not as many as the reads could
/// use, two URLs still read to the local answer, the transfers spread over
/// the threads there are: never a panic (exit 101, or 2 for B) or a hang. A
/// check that a runtime's threads could be had, then a multi-thread runtime
/// starting them, did all three: the check held none, so A's and B's both
/// passed and their runtimes then took more than was left; tokio panics when
/// refused a thread, and when refused one while the runtime is being built,
/// waits on unwinding for the workers it did start.
#[test]
fn urls_read_to_the_local_answer_under_a_partial_thread_budget() {
    if !cfg!(target_os = "linux") {
        eprintln!("skipping: thread stacks count against `ulimit -d` on Linux");
        return;
    }
    let f = fixture("20k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    assert!(!want.2.is_empty());
    let Some(server) = Server::start(&f.0, &[]) else {
        eprintln!("skipping: no python to run the object server");
        return;
    };
    // By name, so each connection's lookup wants a thread too, over plain
    // HTTP and S3; in parts small enough that each read is spread over
    // several threads.
    let named = server.url.replace("127.0.0.1", "localhost");
    let part = ("COMPARA_FETCH_PART", "262144".to_string());
    let http = vec![part.clone()];
    let mut s3 = s3_env(&named);
    s3.push(part);
    let cases = [
        (
            format!("{named}/bkt/t_a.csv"),
            format!("{named}/bkt/t_b.csv"),
            borrowed(&http),
        ),
        (
            "s3://bkt/t_a.csv".to_string(),
            "s3://bkt/t_b.csv".to_string(),
            borrowed(&s3),
        ),
    ];
    for n in 6..=11 {
        for i in 0..3 {
            let (a, b, env) = &cases[i % 2];
            let got = compare_within(
                with_threads(n),
                a,
                b,
                &KEY,
                env,
                None,
                Some(std::time::Duration::from_secs(60)),
            );
            assert_eq!(
                want.0, got.0,
                "{a}, about {n} threads: exit; stderr {}",
                got.1
            );
            assert_eq!(want.2, got.2, "{a}, about {n} threads: summary");
            assert_eq!(want.3, got.3, "{a}, about {n} threads: rows");
        }
    }
}

/// Two URLs read a few megabytes above the heap they are started with
/// (`ulimit -d`, or a container's) answer as the local files do, or refuse
/// with what ran out: never signal 6. hyper grows each connection's read
/// buffer with the allocator's abort on failure, and with sixteen requests in
/// flight on each side every limit from 9 to 24 MB ended on "memory
/// allocation of 131072 bytes failed" or the like. One file is both sides, so
/// that the comparison after the reads has no rows to report: what is tested
/// is the reads.
#[test]
fn urls_near_the_heap_limit_answer_or_refuse_rather_than_aborting() {
    if !cfg!(target_os = "linux") {
        eprintln!("skipping: `ulimit -d` bounds the heap on Linux");
        return;
    }
    let f = fixture("20k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_a.csv"), &KEY, &[], None);
    assert_eq!(want.0, 0, "{}", want.1);
    let Some(server) = Server::start(&f.0, &[]) else {
        eprintln!("skipping: no python to run the object server");
        return;
    };
    let a = format!("{}/bkt/t_a.csv", server.url);
    // Parts of 64 KB: many requests in flight, each growing its buffer.
    let env = [("COMPARA_FETCH_PART", "65536")];
    let (mut failed, mut answered) = (Vec::new(), 0);
    for mb in 9..=24 {
        let kb = mb * 1024 + 512;
        let mut cmd = Command::new("bash");
        cmd.args(["-c", &format!("ulimit -d {kb}; exec \"$0\" \"$@\"")])
            .arg(env!("CARGO_BIN_EXE_compara"))
            .env_remove("COMPARA_MEMORY");
        let got = compare_within(
            cmd,
            &a,
            &a,
            &KEY,
            &env,
            None,
            Some(std::time::Duration::from_secs(60)),
        );
        match got.0 {
            c if c == want.0 && got.2 == want.2 && got.3 == want.3 => answered += 1,
            2 if got.1.contains("out of memory") => {}
            c => failed.push(format!(
                "{kb} KB: exit {c}: {}",
                got.1
                    .lines()
                    .find(|l| !l.contains(" downloaded "))
                    .unwrap_or("")
            )),
        }
    }
    assert!(
        failed.is_empty(),
        "{} of 16 runs neither answered as the local files do nor refused:\n{}",
        failed.len(),
        failed.join("\n")
    );
    assert!(answered > 0, "no limit in the band was enough to answer");
}

/// What goes wrong says what it is: a missing object, a refused one, a
/// compression this does not read.
#[test]
fn remote_failures_name_the_problem() {
    let f = fixture("1k", &[]);
    let Some(server) = Server::start(&f.0, &[]) else {
        return;
    };
    let u = server.url.clone();
    let (code, err, ..) = compare(
        &format!("{u}/bkt/missing.csv"),
        &format!("{u}/bkt/t_b.csv"),
        &KEY,
        &[],
        None,
    );
    assert_eq!(code, 2, "{err}");
    assert!(
        err.contains("not found") && err.contains("missing.csv"),
        "{err}"
    );
    let env = s3_env(&u);
    let (code, err, ..) = compare(
        "s3://bkt/missing.parquet",
        &format!("{u}/bkt/t_b.csv"),
        &KEY,
        &borrowed(&env),
        None,
    );
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("not found"), "{err}");
    drop(server);
    let Some(private) = Server::start(&f.0, &["--private"]) else {
        return;
    };
    let (code, err, ..) = compare(
        &format!("{}/bkt/t_a.csv", private.url),
        &format!("{}/bkt/t_b.csv", private.url),
        &KEY,
        &[],
        None,
    );
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("403"), "{err}");
}

/// `columns` on a remote Parquet object reads its footer and nothing else.
#[test]
fn columns_of_a_remote_parquet_file_read_only_the_footer() {
    let f = fixture("20k", &["--format", "parquet"]);
    let Some(server) = Server::start(&f.0, &[]) else {
        return;
    };
    let out = Command::new(env!("CARGO_BIN_EXE_compara"))
        .args(["columns", &format!("{}/bkt/t_a.parquet", server.url)])
        .output()
        .unwrap();
    assert!(out.status.success(), "{out:?}");
    let names = String::from_utf8_lossy(&out.stdout);
    assert!(names.lines().any(|l| l == "txn_id"), "{names}");
    assert!(
        server.sent() < 70_000,
        "columns fetched {} bytes",
        server.sent()
    );
}

/// A server that gives no end of an object by `bytes=-n` is read from its
/// start, then in ranges that name their first byte, to the local answer:
/// one that answers 501 (Fastly, in front of GitHub's release assets) or 416
/// with no size. They had been an error (501, after three more tries) and an
/// empty file (416). Parquet still fetches its footer and the columns asked
/// for, not the file. One that sends the whole object is a download, as
/// before. A server that refuses every range says so, and what to do; one
/// that claims to send more than was asked is not read on.
#[test]
fn a_server_that_takes_no_suffix_range_is_read_in_ranges() {
    let f = fixture("20k", &["--format", "parquet"]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    assert!(std::fs::metadata(bucket.join("t_a.csv")).unwrap().len() > 4 << 16);
    // Parts of 64 KB: a whole answer is then larger than a part.
    let env = [("COMPARA_FETCH_PART", "65536")];
    for mode in ["suffix-501", "suffix-416", "suffix-200"] {
        let Some(server) = Server::start(&f.0, &["--fault", mode]) else {
            eprintln!("skipping: no python to run the object server");
            return;
        };
        let u = &server.url;
        let got = compare_urls(
            &format!("{u}/bkt/t_a.csv"),
            &format!("{u}/bkt/t_b.csv"),
            &env,
        );
        assert_eq!(
            (want.0, &want.2, &want.3),
            (got.0, &got.2, &got.3),
            "{mode}: {}",
            got.1
        );
        let ranged = server
            .requests()
            .iter()
            .filter(|(range, sent)| {
                range.starts_with("bytes=") && !range.starts_with("bytes=-") && *sent > 0
            })
            .count();
        if mode != "suffix-200" {
            assert!(ranged >= 8, "{mode}: {ranged} ranges by their first byte");
        }
        let start = server.requests().len();
        let out = Command::new(env!("CARGO_BIN_EXE_compara"))
            .args(["columns", &format!("{u}/bkt/t_a.parquet")])
            .output()
            .unwrap();
        assert!(out.status.success(), "{mode}: {out:?}");
        let names = String::from_utf8_lossy(&out.stdout);
        assert!(names.lines().any(|l| l == "txn_id"), "{mode}: {names}");
        let sent: u64 = server.requests()[start..].iter().map(|r| r.1).sum();
        let size = std::fs::metadata(bucket.join("t_a.parquet")).unwrap().len();
        // The start and the end, 64 KB each; a whole answer is the file.
        if mode != "suffix-200" {
            assert!(
                sent < 140_000,
                "{mode}: columns fetched {sent} of {size} bytes"
            );
        }
    }
    let Some(server) = Server::start(&f.0, &["--fault", "ranges-501"]) else {
        return;
    };
    let u = &server.url;
    let (code, err, ..) = compare_urls(
        &format!("{u}/bkt/t_a.csv"),
        &format!("{u}/bkt/t_b.csv"),
        &[],
    );
    assert_eq!(code, 2, "{err}");
    assert!(
        err.contains("/bkt/t_")
            && err.contains("bytes=-65536")
            && err.contains("HTTP 501")
            && err.contains("download it first"),
        "{err}"
    );
    // Not asked again: 501 says the server does not do it.
    assert!(server.requests().len() <= 4, "{:?}", server.requests());
    drop(server);
    let Some(server) = Server::start(&f.0, &["--fault", "wide"]) else {
        return;
    };
    let u = &server.url;
    let (code, err, ..) = compare_urls(
        &format!("{u}/bkt/t_a.csv"),
        &format!("{u}/bkt/t_b.csv"),
        &[],
    );
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("more than the 65536 bytes asked for"), "{err}");
}

/// An object store that gives no end of an object by `bytes=-n` (a 501, as
/// Fastly answers it: an S3-compatible endpoint behind it) is read as a
/// plain HTTP server is, from its start and then in ranges by their first
/// byte, to the local answer, CSV and Parquet, one object or the parts under
/// a prefix. object_store had asked again after the 501 ten times, for
/// seconds to minutes by its random backoff, and then the read was an error.
/// Once refused, a store is not asked for another object's end that way.
/// A store that refuses every range says so at once, and what to do.
#[test]
fn a_store_that_takes_no_suffix_range_is_read_in_ranges() {
    let f = fixture("20k", &["--format", "parquet"]);
    let bucket = f.0.join("bkt");
    for side in ["a", "b"] {
        let csv = std::fs::read_to_string(bucket.join(format!("t_{side}.csv"))).unwrap();
        let (header, rows) = csv.split_once('\n').unwrap();
        let rows: Vec<&str> = rows.lines().collect();
        let dir = bucket.join(format!("parts-{side}"));
        std::fs::create_dir_all(&dir).unwrap();
        for (i, chunk) in rows.chunks(rows.len().div_ceil(4)).enumerate() {
            let body = format!("{header}\n{}\n", chunk.join("\n"));
            std::fs::write(dir.join(format!("p{i}.csv")), body).unwrap();
        }
    }
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let Some(server) = Server::start(&f.0, &["--fault", "suffix-501"]) else {
        eprintln!("skipping: no python to run the object server");
        return;
    };
    let env = s3_env(&server.url);
    let env = borrowed(&env);
    // One object at a time, so that each is asked after the one before.
    let one_by_one = [&env[..], &[("COMPARA_FETCH_CONCURRENCY", "1")]].concat();
    let limit = Some(std::time::Duration::from_secs(30));
    for (a, b, extra) in [
        ("t_a.csv", "t_b.csv", &[][..]),
        ("t_a.parquet", "t_b.parquet", &["-c", "amount"][..]),
        ("parts-a", "parts-b", &[][..]),
    ] {
        let args = [&KEY[..], extra].concat();
        let want = compare(&local(a), &local(b), &args, &[], None);
        let start = server.requests().len();
        let started = std::time::Instant::now();
        let prefix = a.starts_with("parts");
        let slash = if prefix { "/" } else { "" };
        let got = compare_within(
            Command::new(env!("CARGO_BIN_EXE_compara")),
            &format!("s3://bkt/{a}{slash}"),
            &format!("s3://bkt/{b}{slash}"),
            &args,
            if prefix { &one_by_one } else { &env },
            None,
            limit,
        );
        assert_eq!(
            (want.0, &want.2, &want.3),
            (got.0, &got.2, &got.3),
            "{a}: {}",
            got.1
        );
        // A suffix refused is not asked again, nor asked of the next part:
        // A's first and B's.
        let refused = server.requests()[start..]
            .iter()
            .filter(|(range, _)| range.starts_with("bytes=-"))
            .count();
        assert!(refused <= 2, "{a}: {refused} suffix ranges asked");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(20),
            "{a}: {:?}",
            started.elapsed()
        );
    }
    drop(server);
    let Some(server) = Server::start(&f.0, &["--fault", "ranges-501"]) else {
        return;
    };
    let env = s3_env(&server.url);
    let (code, err, ..) = compare_within(
        Command::new(env!("CARGO_BIN_EXE_compara")),
        "s3://bkt/t_a.csv",
        "s3://bkt/t_b.csv",
        &KEY,
        &borrowed(&env),
        None,
        limit,
    );
    assert_eq!(code, 2, "{err}");
    assert!(
        err.contains("s3://bkt/t_")
            && err.contains("bytes=-65536")
            && err.contains("bytes=0-65535")
            && err.contains("HTTP 501")
            && err.contains("download it first"),
        "{err}"
    );
    // The end and the start of each file, each asked once.
    assert!(server.requests().len() <= 4, "{:?}", server.requests());
    // Under a prefix the listing gave the size: the end by its first byte.
    let (code, err, ..) = compare_within(
        Command::new(env!("CARGO_BIN_EXE_compara")),
        "s3://bkt/parts-a/",
        "s3://bkt/parts-b/",
        &KEY,
        &borrowed(&env),
        None,
        limit,
    );
    assert_eq!(code, 2, "{err}");
    assert!(
        err.contains("end by its first byte") && err.contains("download it first"),
        "{err}"
    );
}

/// A URL whose first request is redirected (GitHub sends a release asset to
/// a signed URL on its CDN) has its parts asked where it was sent, not each
/// redirected again, to the local answer; once that refuses them, as a
/// signed URL does when it expires, the URL itself is asked again.
#[test]
fn a_redirected_url_is_read_where_it_was_sent() {
    let f = fixture("20k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    // Parts of 64 KB: about sixty a file.
    let env = [("COMPARA_FETCH_PART", "65536")];
    for extra in [&[][..], &["--expire-after", "20"][..]] {
        let args = [&["--redirect"][..], extra].concat();
        let Some(server) = Server::start(&f.0, &args) else {
            eprintln!("skipping: no python to run the object server");
            return;
        };
        let u = &server.url;
        let got = compare_urls(
            &format!("{u}/go/bkt/t_a.csv"),
            &format!("{u}/go/bkt/t_b.csv"),
            &env,
        );
        assert_eq!(
            (want.0, &want.2, &want.3),
            (got.0, &got.2, &got.3),
            "{extra:?}: {}",
            got.1
        );
        let log = std::fs::read_to_string(&server.log).unwrap();
        let redirected = log.lines().filter(|l| l.contains("\"/go/")).count();
        if extra.is_empty() {
            // The first request of each file.
            assert_eq!(redirected, 2, "{log}");
        } else {
            // Refused where they were sent, then sent there again.
            let refused = log
                .lines()
                .filter(|l| l.contains("signed=") && l.contains("\"sent\": 0"))
                .count();
            assert!(refused > 0 && redirected > 2, "{log}");
        }
    }
}

/// A made-up credential, in the URLs [`no_credential_in_a_url_is_shown`]
/// reads; its `)` is one a query string can hold, where a message that
/// cut a URL at the first `)` showed the rest.
const SECRET: &str = "x)SECRETnot-a-real-key";

/// `compara compare a b` with the report and the summary written, and
/// everything it said: exit code, then stdout, stderr, the summary and the
/// report, one after the other.
fn said_all(a: &str, b: &str, env: &[(&str, &str)]) -> (i32, String) {
    let dir = std::env::temp_dir().join(format!("compara-said-{}-{}", std::process::id(), id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_compara"))
        .args(["compare", a, b])
        .args(KEY)
        .arg("--json")
        .arg(dir.join("summary.json"))
        .arg("-o")
        .arg(dir.join("report.html"))
        .arg("--no-compress")
        .envs(env.iter().copied())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let started = std::time::Instant::now();
    while child.try_wait().unwrap().is_none() && started.elapsed().as_secs() < 60 {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let _ = child.kill();
    let out = child.wait_with_output().unwrap();
    let mut all = String::from_utf8_lossy(&out.stdout).into_owned();
    all.push_str(&String::from_utf8_lossy(&out.stderr));
    for name in ["summary.json", "report.html"] {
        all.push_str(&std::fs::read_to_string(dir.join(name)).unwrap_or_default());
    }
    let _ = std::fs::remove_dir_all(&dir);
    (out.status.code().unwrap_or(-1), all)
}

/// A credential in a URL is never shown: not in an error, a step line, the
/// summary or the report. A redirect to a signed URL (`?sig=…`) whose parts
/// then fail -- a reset connection, a body cut short, a stall, a 403, a 500
/// -- names the URL given; so does a URL given with a token in its query or
/// a password before its host, read or not found, a proxy's with one, and
/// an output URL that refuses the upload. The reset had shown the
/// signature's end (`error sending request<rest>): …`), and every URL given
/// was shown whole.
#[test]
fn no_credential_in_a_url_is_shown() {
    let f = fixture("20k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let quick = [
        ("COMPARA_FETCH_PART", "65536"),
        ("COMPARA_READ_TIMEOUT", "1"),
    ];
    let secret = SECRET.trim_start_matches("x)");
    // Every case is run before any is failed, so a failure lists them all.
    let mut shown: Vec<(String, String)> = Vec::new();
    let mut check = |case: &str, all: &str| {
        if all.contains(secret) {
            shown.push((case.to_string(), all.to_string()));
        }
    };
    for fault in ["reset", "drop", "stall", "http-403", "http-500"] {
        let Some(server) = Server::start(
            &f.0,
            &[
                "--redirect",
                "--secret",
                SECRET,
                "--fault",
                fault,
                "--fault-at",
                "1",
                "--fault-every",
                "1",
            ],
        ) else {
            eprintln!("skipping: no python to run the object server");
            return;
        };
        let given = format!("{}/go/bkt/t_a.csv", server.url);
        // A short read timeout only for the stall: under it the resets
        // too end as a stall, which names no URL.
        let env = if fault == "stall" {
            &quick[..]
        } else {
            &quick[..1]
        };
        let (code, all) = said_all(&given, &local("t_b.csv"), env);
        assert_eq!(code, 2, "{fault}: {all}");
        assert!(all.contains(&given), "{fault}: {all}");
        check(fault, &all);
    }
    let Some(server) = Server::start(&f.0, &["--redirect", "--secret", SECRET]) else {
        return;
    };
    let u = &server.url;
    let host = u.strip_prefix("http://").unwrap();
    let (code, all) = said_all(&format!("{u}/go/bkt/t_a.csv"), &local("t_b.csv"), &quick);
    assert_eq!(code, 1, "{all}");
    check("redirected, read", &all);
    for (case, a, code) in [
        ("query, read", format!("{u}/bkt/t_a.csv?token={SECRET}"), 1),
        (
            "password, read",
            format!("http://u:{secret}@{host}/bkt/t_a.csv"),
            1,
        ),
        (
            "query, not found",
            format!("{u}/bkt/nope.csv?token={SECRET}"),
            2,
        ),
        (
            "password, not found",
            format!("http://u:{secret}@{host}/bkt/nope.csv"),
            2,
        ),
    ] {
        let (got, all) = said_all(&a, &local("t_b.csv"), &quick);
        assert_eq!(got, code, "{a}: {all}");
        assert!(all.contains(&format!("{host}/bkt/")), "{a}: {all}");
        check(case, &all);
    }
    let proxy = format!("http://u:{secret}@127.0.0.1:9");
    let (code, all) = said_all(
        &format!("{u}/bkt/t_a.csv"),
        &local("t_b.csv"),
        &[
            ("NO_PROXY", ""),
            ("no_proxy", ""),
            ("HTTP_PROXY", &proxy),
            ("HTTPS_PROXY", &proxy),
        ],
    );
    assert_eq!(code, 2, "{all}");
    assert!(all.contains("a proxy is set: http://"), "{all}");
    check("proxy", &all);
    drop(server);
    let Some(server) = Server::start(
        &f.0,
        &["--fault", "http-403", "--fault-every", "1", "--fault-puts"],
    ) else {
        return;
    };
    let out = Command::new(env!("CARGO_BIN_EXE_compara"))
        .args(["compare", &local("t_a.csv"), &local("t_b.csv")])
        .args(KEY)
        .args([
            "--json",
            &format!("{}/out/summary.json?sig={SECRET}", server.url),
        ])
        .output()
        .unwrap();
    let all = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(out.status.code(), Some(2), "{all}");
    assert!(all.contains("403"), "{all}");
    check("output URL", &all);
    let cases: Vec<&str> = shown.iter().map(|(case, _)| case.as_str()).collect();
    let all: Vec<String> = shown.iter().map(|(c, all)| format!("{c}: {all}")).collect();
    assert!(
        shown.is_empty(),
        "shown in {} cases: {}\n{}",
        cases.len(),
        cases.join("; "),
        all.join("\n---\n")
    );
}

/// Outputs to object storage: the report, the summary and the export land in
/// the bucket as they would on disk, in one request or as a multipart upload;
/// `--json -` is the summary on standard output.
#[test]
fn outputs_reach_object_storage_and_stdout() {
    let f = fixture("20k", &[]);
    let Some(server) = Server::start(&f.0, &[]) else {
        return;
    };
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    for (one_shot, part) in [("", ""), ("1000", "200000")] {
        let mut env = s3_env(&server.url);
        if !one_shot.is_empty() {
            env.push(("COMPARA_UPLOAD_ONE_SHOT", one_shot.into()));
            env.push(("COMPARA_UPLOAD_PART", part.into()));
        }
        let prefix = format!("out{}", id());
        let out = Command::new(env!("CARGO_BIN_EXE_compara"))
            .args(["compare", &local("t_a.csv"), &local("t_b.csv")])
            .args(KEY)
            .args(["-o", &format!("s3://bkt/{prefix}/report.html")])
            .args(["--json", &format!("s3://bkt/{prefix}/summary.json")])
            .args(["--export-dir", &format!("s3://bkt/{prefix}/export/")])
            .envs(borrowed(&env))
            .output()
            .unwrap();
        assert_eq!(out.status.code(), Some(want.0), "{out:?}");
        let up = bucket.join(&prefix);
        assert!(std::fs::metadata(up.join("report.html")).unwrap().len() > 1000);
        let mut v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(up.join("summary.json")).unwrap())
                .unwrap();
        v.as_object_mut().unwrap().remove("meta");
        assert_eq!(v.to_string(), want.2, "{one_shot}: summary");
        let mut rows = String::new();
        for n in ["added.csv", "changed.csv", "removed.csv"] {
            rows.push_str(&std::fs::read_to_string(up.join("export").join(n)).unwrap());
        }
        assert_eq!(rows, want.3, "{one_shot}: exported rows");
    }
    // The summary alone on stdout; the counts line on stderr.
    let out = Command::new(env!("CARGO_BIN_EXE_compara"))
        .args(["compare", &local("t_a.csv"), &local("t_b.csv")])
        .args(KEY)
        .args(["--json", "-"])
        .output()
        .unwrap();
    let mut v: serde_json::Value =
        serde_json::from_slice(&out.stdout).expect("stdout is the summary");
    v.as_object_mut().unwrap().remove("meta");
    assert_eq!(v.to_string(), want.2);
    assert!(String::from_utf8_lossy(&out.stderr).contains("matched"));
}

/// A multipart upload where the system gives some threads but not as many
/// as the upload could send its parts from lands whole, its parts sent from
/// the threads there are or from the calling thread: never a panic or a
/// hang.
#[test]
fn a_multipart_upload_lands_whole_under_a_partial_thread_budget() {
    if !cfg!(target_os = "linux") {
        eprintln!("skipping: thread stacks count against `ulimit -d` on Linux");
        return;
    }
    let f = fixture("20k", &[]);
    let Some(server) = Server::start(&f.0, &[]) else {
        return;
    };
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    // By name, so the connections' lookups want threads too; parts small
    // enough that each export is a dozen or more.
    let mut env = s3_env(&server.url.replace("127.0.0.1", "localhost"));
    env.push(("COMPARA_UPLOAD_ONE_SHOT", "1".into()));
    env.push(("COMPARA_UPLOAD_PART", "4096".into()));
    for n in [2, 4, 6, 8, 10] {
        let prefix = format!("out{}", id());
        let mut cmd = with_threads(n);
        cmd.args(["compare", &local("t_a.csv"), &local("t_b.csv")])
            .args(KEY)
            .args(["--export-dir", &format!("s3://bkt/{prefix}/")])
            .envs(borrowed(&env))
            .stdout(Stdio::null())
            .stderr(Stdio::piped());
        let mut child = cmd.spawn().unwrap();
        let started = std::time::Instant::now();
        while child.try_wait().unwrap().is_none() {
            if started.elapsed() > std::time::Duration::from_secs(60) {
                let _ = child.kill();
                panic!("about {n} threads: no exit within 60 s");
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        let out = child.wait_with_output().unwrap();
        let err = String::from_utf8_lossy(&out.stderr);
        assert_eq!(out.status.code(), Some(want.0), "about {n} threads: {err}");
        let mut rows = String::new();
        for name in ["added.csv", "changed.csv", "removed.csv"] {
            rows.push_str(&std::fs::read_to_string(bucket.join(&prefix).join(name)).unwrap());
        }
        assert_eq!(rows, want.3, "about {n} threads: exported rows");
    }
}

/// A server that drops some ranged requests is retried through; an object
/// overwritten part-way through the download is an error, not a file stitched
/// together from two versions.
#[test]
fn transient_failures_are_retried_and_an_overwrite_is_caught() {
    let f = fixture("200k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    let Some(flaky) = Server::start(&f.0, &["--flaky", "3"]) else {
        return;
    };
    let u = &flaky.url;
    // Small parts, so there are enough requests for some to fail.
    let env = [("COMPARA_FETCH_CONCURRENCY", "2")];
    let got = compare(
        &format!("{u}/bkt/t_a.csv"),
        &format!("{u}/bkt/t_b.csv"),
        &KEY,
        &env,
        None,
    );
    assert_eq!(
        (want.0, &want.2, &want.3),
        (got.0, &got.2, &got.3),
        "flaky: {}",
        got.1
    );
    drop(flaky);
    let Some(changing) = Server::start(&f.0, &["--overwrite-after", "2"]) else {
        return;
    };
    let u = &changing.url;
    let (code, err, ..) = compare(
        &format!("{u}/bkt/t_a.csv"),
        &local("t_b.csv"),
        &KEY,
        &[],
        None,
    );
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("changed while it was being read"), "{err}");
}

/// `compare_within` a minute, of the program as it is, over `a` and `b`.
fn compare_urls(a: &str, b: &str, env: &[(&str, &str)]) -> (i32, String, String, String) {
    compare_within(
        Command::new(env!("CARGO_BIN_EXE_compara")),
        a,
        b,
        &KEY,
        env,
        None,
        Some(std::time::Duration::from_secs(60)),
    )
}

/// A server that stops answering is given up on after the read timeout (1 s
/// here) and asked again: a part whose answer never comes, or stops after
/// its headers, still reads to the local answer, over HTTP and S3, where it
/// had hung for good, and so does a transfer whose every request in flight
/// stalls once. One that stalls every time is an error once it has sent
/// nothing for twice the timeout, however many requests and tries that is
/// (it had been each request's four tries), that names the URL, says it
/// stalled and what to do. One that keeps sending, if only 1 KB a second, is
/// waited for: the timeout is the wait for each piece, not for the whole.
#[test]
fn a_stalled_server_is_asked_again_and_a_slow_one_waited_for() {
    let f = fixture("20k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    let quick = [
        ("COMPARA_READ_TIMEOUT", "1"),
        ("COMPARA_FETCH_PART", "65536"),
    ];
    for (mode, s3) in [
        ("stall", false),
        ("silent", false),
        ("stall", true),
        ("silent", true),
    ] {
        let Some(server) = Server::start(&f.0, &["--fault", mode, "--fault-at", "10"]) else {
            eprintln!("skipping: no python to run the object server");
            return;
        };
        let u = &server.url;
        let mut env = if s3 { s3_env(u) } else { Vec::new() };
        env.extend(quick.iter().map(|(k, v)| (*k, v.to_string())));
        let (a, b) = if s3 {
            (
                "s3://bkt/t_a.csv".to_string(),
                "s3://bkt/t_b.csv".to_string(),
            )
        } else {
            (format!("{u}/bkt/t_a.csv"), format!("{u}/bkt/t_b.csv"))
        };
        let got = compare_urls(&a, &b, &borrowed(&env));
        assert_eq!(
            (want.0, &want.2, &want.3),
            (got.0, &got.2, &got.3),
            "{mode}, s3 {s3}: {}",
            got.1
        );
    }

    // A over the URL, B local: after A's end, its first sixteen parts, all
    // in flight together, each stalled once. Silent for the timeout and the
    // pause before the next try, inside the budget (3 s here, for room).
    for s3 in [false, true] {
        let Some(server) = Server::start(
            &f.0,
            &[
                "--fault",
                "stall",
                "--fault-at",
                "1",
                "--fault-every",
                "1",
                "--fault-max",
                "16",
            ],
        ) else {
            return;
        };
        let u = &server.url;
        let mut env = if s3 { s3_env(u) } else { Vec::new() };
        env.push(("COMPARA_READ_TIMEOUT", "3".into()));
        env.push(("COMPARA_FETCH_PART", "65536".into()));
        let a = if s3 {
            "s3://bkt/t_a.csv".to_string()
        } else {
            format!("{u}/bkt/t_a.csv")
        };
        let got = compare_urls(&a, &local("t_b.csv"), &borrowed(&env));
        assert_eq!(
            (want.0, &want.2, &want.3),
            (got.0, &got.2, &got.3),
            "every request stalled once, s3 {s3}: {}",
            got.1
        );
    }

    for (at, s3) in [("0", false), ("10", false), ("0", true), ("10", true)] {
        let Some(server) = Server::start(
            &f.0,
            &["--fault", "stall", "--fault-at", at, "--fault-every", "1"],
        ) else {
            return;
        };
        let u = &server.url;
        let mut env = if s3 { s3_env(u) } else { Vec::new() };
        env.extend(quick.iter().map(|(k, v)| (*k, v.to_string())));
        let (a, b) = if s3 {
            (
                "s3://bkt/t_a.csv".to_string(),
                "s3://bkt/t_b.csv".to_string(),
            )
        } else {
            (format!("{u}/bkt/t_a.csv"), format!("{u}/bkt/t_b.csv"))
        };
        let started = std::time::Instant::now();
        let (code, err, ..) = compare_urls(&a, &b, &borrowed(&env));
        assert_eq!(code, 2, "from {at}, s3 {s3}: {err}");
        assert!(
            err.contains("t_") && err.contains("sent nothing for 2 s on any request"),
            "from {at}, s3 {s3}: {err}"
        );
        assert!(err.contains("COMPARA_READ_TIMEOUT"), "{err}");
        // Twice the timeout, and the time to see it; it had been four tries
        // (HTTP), or the store's own retries for three minutes (S3).
        assert!(
            started.elapsed().as_secs() < 10,
            "from {at}, s3 {s3}: {:?}",
            started.elapsed()
        );
    }

    let small = fixture("30", &[]);
    let bucket = small.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    for s3 in [false, true] {
        // About 6 KB a side: 6 s at 1 KB a second, twice the timeout.
        let Some(server) = Server::start(&small.0, &["--fault", "dribble", "--fault-every", "1"])
        else {
            return;
        };
        let u = &server.url;
        let mut env = if s3 { s3_env(u) } else { Vec::new() };
        env.push(("COMPARA_READ_TIMEOUT", "3".into()));
        let (a, b) = if s3 {
            (
                "s3://bkt/t_a.csv".to_string(),
                "s3://bkt/t_b.csv".to_string(),
            )
        } else {
            (format!("{u}/bkt/t_a.csv"), format!("{u}/bkt/t_b.csv"))
        };
        let got = compare_urls(&a, &b, &borrowed(&env));
        assert_eq!(
            (want.0, &want.2, &want.3),
            (got.0, &got.2, &got.3),
            "dribble, s3 {s3}: {}",
            got.1
        );
    }
}

/// Files for the PUT tests, in a directory the object server serves: `a.csv`
/// with no rows, so that `compare a.csv <b> -k id --export-dir URL` sends
/// B's rows first, as `added.csv`; `small.csv` (6.6 KB of rows),
/// `medium.csv` (205 KB) and `big.csv` (8.4 MB).
fn put_fixture() -> Dir {
    let dir =
        Dir(std::env::temp_dir().join(format!("compara-put-{}-{}", std::process::id(), id())));
    std::fs::create_dir_all(dir.0.join("bkt")).unwrap();
    std::fs::write(dir.0.join("a.csv"), "id,v\n").unwrap();
    let rows = |n: usize| {
        let mut text = String::from("id,v\n");
        for i in 0..n {
            text.push_str(&format!("{i:08},{:032}\n", i * 7));
        }
        text
    };
    std::fs::write(dir.0.join("small.csv"), rows(160)).unwrap();
    std::fs::write(dir.0.join("medium.csv"), rows(5_000)).unwrap();
    std::fs::write(dir.0.join("big.csv"), rows(200_000)).unwrap();
    dir
}

/// `compare a.csv <b> -k id --export-dir URL`, the object server failing
/// every PUT with `mode` (and `extra` arguments), within a minute: the exit,
/// the error, the file the server wrote, how long it took, and the URL.
fn put_export(
    root: &Dir,
    b: &str,
    mode: &str,
    timeout: &str,
    extra: &[&str],
) -> (
    std::process::ExitStatus,
    String,
    Option<Vec<u8>>,
    std::time::Duration,
    String,
) {
    let server = Server::start(
        &root.0,
        &[
            &["--fault", mode, "--fault-puts", "--fault-every", "1"],
            extra,
        ]
        .concat(),
    )
    .unwrap();
    let name = format!("{mode}{}", id());
    let dest = format!("{}/bkt/{name}/", server.url);
    let started = std::time::Instant::now();
    let mut child = Command::new(env!("CARGO_BIN_EXE_compara"))
        .arg("compare")
        .arg(root.0.join("a.csv"))
        .arg(root.0.join(b))
        .args(["-k", "id", "--export-dir", &dest])
        .env("COMPARA_READ_TIMEOUT", timeout)
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    while child.try_wait().unwrap().is_none() && started.elapsed().as_secs() < 60 {
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    let _ = child.kill();
    let out = child.wait_with_output().unwrap();
    let written = std::fs::read(root.0.join("bkt").join(&name).join("added.csv")).ok();
    let err = String::from_utf8_lossy(&out.stderr).into_owned();
    (out.status, err, written, started.elapsed(), dest)
}

/// A plain PUT whose connection the server resets, or closes half-way, under
/// a body of megabytes is an error that names the URL and says so: the
/// process had been killed by SIGPIPE, exit 141 and no word.
#[test]
fn a_put_whose_connection_the_server_closes_is_an_error() {
    if python().is_none() {
        eprintln!("skipping: no python to run the object server");
        return;
    }
    let root = put_fixture();
    // The write that meets the reset fails with ECONNRESET or, after it,
    // with EPIPE, which raised the SIGPIPE: most runs, not all, so four.
    for mode in ["reset", "reset", "reset", "reset", "drop"] {
        let (status, err, _, _, dest) = put_export(&root, "big.csv", mode, "1", &[]);
        // Not a signal.
        assert_eq!(status.code(), Some(2), "{mode}: {status:?} {err}");
        assert!(
            err.contains(&format!("cannot write {dest}added.csv")),
            "{mode}: {err}"
        );
    }
}

/// A plain PUT to a server that stops -- never reading it, or reading it all
/// and never answering -- ends in an error that names the URL, says what
/// happened, the pace it gives up below and what to do, within the read
/// timeout and the time the body would take at 16 KB/s, where it had waited
/// for good. One reading 32 KB a second is waited for, and has the whole
/// file; one reading 1 KB a second is given up on, and told why.
#[test]
fn a_put_to_a_stopped_server_ends_and_a_slow_one_is_waited_for() {
    if python().is_none() {
        eprintln!("skipping: no python to run the object server");
        return;
    }
    let root = put_fixture();
    let local = root.0.join("local");
    let status = Command::new(env!("CARGO_BIN_EXE_compara"))
        .arg("compare")
        .arg(root.0.join("a.csv"))
        .arg(root.0.join("medium.csv"))
        .args(["-k", "id", "--export-dir"])
        .arg(&local)
        .output()
        .unwrap()
        .status;
    assert_eq!(status.code(), Some(1));
    let want = std::fs::read(local.join("added.csv")).unwrap();
    assert!(want.len() > 200_000, "{}", want.len());
    let floor = "compara gives up on a server taking less than 16 KB/s";

    for mode in ["silent", "stall"] {
        let (status, err, _, took, dest) = put_export(&root, "small.csv", mode, "1", &[]);
        assert_eq!(status.code(), Some(2), "{mode}: {status:?} {err}");
        assert!(
            err.contains(&format!("cannot write {dest}added.csv")),
            "{mode}: {err}"
        );
        assert!(
            err.contains("no answer")
                && err.contains(floor)
                && err.contains("COMPARA_READ_TIMEOUT"),
            "{mode}: {err}"
        );
        // 1 s, and 6.6 KB at 16 KB/s.
        assert!(took.as_secs() < 20, "{mode}: {took:?}");
    }
    // 205 KB at 32 KB a second: about 7 s, seven times the timeout.
    let (status, err, written, took, _) =
        put_export(&root, "medium.csv", "dribble", "1", &["--rate", "32768"]);
    assert_eq!(status.code(), Some(1), "32 KB/s: {status:?} {err}");
    assert_eq!(written.as_deref(), Some(&want[..]), "32 KB/s: {err}");
    assert!(took.as_secs() >= 5, "{took:?}");
    // 6.6 KB at 1 KB a second, under the floor.
    let (status, err, _, took, _) = put_export(&root, "small.csv", "dribble", "1", &[]);
    assert_eq!(status.code(), Some(2), "1 KB/s: {status:?} {err}");
    assert!(err.contains(floor), "1 KB/s: {err}");
    assert!(took.as_secs() < 6, "1 KB/s: {took:?}");
}

/// The first request, for the end of the object, is asked again when its
/// connection drops part-way through the answer or closes before it, as any
/// later range is: one such drop had failed the compare (exit 2, "error
/// decoding response body", or "connection closed before message
/// completed").
#[test]
fn the_end_of_an_object_is_asked_again_when_its_connection_drops() {
    let f = fixture("20k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    for mode in ["drop", "reset"] {
        // The first ranged request: A's end or B's.
        let Some(server) = Server::start(&f.0, &["--fault", mode]) else {
            eprintln!("skipping: no python to run the object server");
            return;
        };
        let u = &server.url;
        let got = compare_urls(
            &format!("{u}/bkt/t_a.csv"),
            &format!("{u}/bkt/t_b.csv"),
            &[],
        );
        assert_eq!(
            (want.0, &want.2, &want.3),
            (got.0, &got.2, &got.3),
            "{mode}: {}",
            got.1
        );
    }
}

/// An object overwritten part-way through the read, on a server that does
/// not check If-Match, is an error over HTTP and S3, as on one that does:
/// each part's ETag, and the object's size its range came with, are the
/// end's or the part is of another version. It had been read as one file
/// stitched from two (exit 1, thousands of keys changed, added and
/// duplicated). With no ETag at all, a size that changed is caught the same
/// way; the same size under no validator cannot be told apart.
#[test]
fn an_overwrite_is_caught_where_the_server_does_not_check_if_match() {
    let f = fixture("20k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    for (extra, s3) in [
        (&[][..], false),
        (&[][..], true),
        (&["--no-etag", "--grow"][..], false),
    ] {
        let mut args = vec!["--overwrite-after", "10", "--ignore-if-match"];
        args.extend(extra);
        let Some(server) = Server::start(&f.0, &args) else {
            eprintln!("skipping: no python to run the object server");
            return;
        };
        let u = &server.url;
        let mut env = if s3 { s3_env(u) } else { Vec::new() };
        env.push(("COMPARA_FETCH_PART", "65536".into()));
        let a = if s3 {
            "s3://bkt/t_a.csv".to_string()
        } else {
            format!("{u}/bkt/t_a.csv")
        };
        let (code, err, ..) = compare_urls(&a, &local("t_b.csv"), &borrowed(&env));
        assert_eq!(code, 2, "{extra:?}, s3 {s3}: {err}");
        assert!(
            err.contains("t_a.csv: the object changed while it was being read"),
            "{extra:?}, s3 {s3}: {err}"
        );
    }
}

/// An answer that runs 1,000 bytes past the range asked is asked for again,
/// and nothing of it written: not over the next part, which had kept the
/// extra bytes when it was in first (a wrong answer, at 8 of 15 requests
/// tried), and not for the end of the object, whose bytes are placed by
/// their count, so all of them landed 1,000 bytes early (A 199,955 rows of
/// 200,000). Over HTTP and S3, for the end (request 0) and for parts.
#[test]
fn an_answer_past_its_range_is_asked_for_again_and_never_written() {
    let f = fixture("20k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    for (at, s3) in [
        ("0", false),
        ("5", false),
        ("20", false),
        ("40", false),
        ("0", true),
        ("20", true),
    ] {
        let Some(server) = Server::start(&f.0, &["--fault", "overlong", "--fault-at", at]) else {
            eprintln!("skipping: no python to run the object server");
            return;
        };
        let u = &server.url;
        let mut env = if s3 { s3_env(u) } else { Vec::new() };
        env.push(("COMPARA_FETCH_PART", "65536".into()));
        let (a, b) = if s3 {
            (
                "s3://bkt/t_a.csv".to_string(),
                "s3://bkt/t_b.csv".to_string(),
            )
        } else {
            (format!("{u}/bkt/t_a.csv"), format!("{u}/bkt/t_b.csv"))
        };
        let got = compare_urls(&a, &b, &borrowed(&env));
        assert_eq!(
            (want.0, &want.2, &want.3),
            (got.0, &got.2, &got.3),
            "request {at}, s3 {s3}: {}",
            got.1
        );
    }
}

/// A prefix, a glob, or a name that is a prefix rather than an object reads
/// every part under it as one table, the local directory's answer; Parquet
/// parts only as far as the comparison reads them, gzip parts expanded.
#[test]
fn parts_under_a_prefix_are_one_table() {
    let dir = std::env::temp_dir().join(format!(
        "compara-remote-many-{}-{}",
        std::process::id(),
        id()
    ));
    let d = Dir(dir.clone());
    let gen_dir = d.0.join("gen");
    for (p, rows, seed) in [("p", "30k", "1"), ("q", "10k", "2")] {
        for extra in [
            &[][..],
            // Uncompressed, so a column's chunk is worth not fetching.
            &["--format", "parquet", "--compression", "none"][..],
        ] {
            let out = Command::new(env!("CARGO_BIN_EXE_compara-gen-data"))
                .args(["--rows", rows, "--seed", seed, "--prefix", p, "--out-dir"])
                .arg(&gen_dir)
                .args(extra)
                .output()
                .unwrap();
            assert!(out.status.success(), "{out:?}");
        }
    }
    let bucket = d.0.join("bkt");
    for side in ["a", "b"] {
        for fmt in ["csv", "parquet"] {
            for (n, p) in [(1, "p"), (2, "q")] {
                let to = bucket.join(format!("{fmt}-{side}/date=2026-10-0{n}/part-0.{fmt}"));
                std::fs::create_dir_all(to.parent().unwrap()).unwrap();
                std::fs::copy(gen_dir.join(format!("{p}_{side}.{fmt}")), &to).unwrap();
            }
            std::fs::write(bucket.join(format!("{fmt}-{side}/_SUCCESS")), "").unwrap();
        }
        // The CSV parts again, gzipped, in the same partitions.
        for (n, p) in [(1, "p"), (2, "q")] {
            let to = bucket.join(format!("gz-{side}/date=2026-10-0{n}/part-0.csv.gz"));
            std::fs::create_dir_all(to.parent().unwrap()).unwrap();
            let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            gz.write_all(&std::fs::read(gen_dir.join(format!("{p}_{side}.csv"))).unwrap())
                .unwrap();
            std::fs::write(&to, gz.finish().unwrap()).unwrap();
        }
    }
    let local = |p: &str| bucket.join(p).to_string_lossy().into_owned();
    let want = compare(&local("csv-a"), &local("csv-b"), &KEY, &[], None);
    assert_eq!(want.0, 1, "{}", want.1);
    let Some(server) = Server::start(&d.0, &[]) else {
        return;
    };
    let env = s3_env(&server.url);
    let env = borrowed(&env);
    for (a, b) in [
        ("s3://bkt/csv-a/", "s3://bkt/csv-b/"),
        ("s3://bkt/parquet-a", "s3://bkt/parquet-b"),
        (
            "s3://bkt/parquet-a/date=*/*.parquet",
            "s3://bkt/csv-b/**/part-?.csv",
        ),
        ("s3://bkt/gz-a/", "s3://bkt/gz-b/**/*.csv.gz"),
        // Partitioned text, read whole and given its column on the way.
        ("s3://bkt/csv-a/", "s3://bkt/gz-b/"),
    ] {
        let got = compare(a, b, &KEY, &env, None);
        assert_eq!(
            (got.0, &got.2, &got.3),
            (want.0, &want.2, &want.3),
            "{a} {b}: {}",
            got.1
        );
    }
    // One compared column, far from the key, of the Parquet parts: their footers and three
    // columns' chunks, not the parts.
    let total: u64 = ["a", "b"]
        .iter()
        .flat_map(|s| (1..=2).map(move |n| format!("parquet-{s}/date=2026-10-0{n}/part-0.parquet")))
        .map(|p| std::fs::metadata(bucket.join(p)).unwrap().len())
        .sum();
    let before = server.sent();
    let narrow = [&KEY[..], &["-c", "risk_flag"]].concat();
    let got = compare(
        "s3://bkt/parquet-a/",
        "s3://bkt/parquet-b/",
        &narrow,
        &env,
        None,
    );
    let local_narrow = compare(&local("parquet-a"), &local("parquet-b"), &narrow, &[], None);
    assert_eq!(
        (got.0, &got.2),
        (local_narrow.0, &local_narrow.2),
        "{}",
        got.1
    );
    let sent = server.sent() - before;
    assert!(sent < total / 2, "sent {sent} of {total}: {}", got.1);
    for (spec, says) in [
        ("s3://bkt/nothing/", "no objects under"),
        ("s3://bkt/csv-a/*.parquet", "no objects match"),
        ("s3://bkt/nothing", "not found"),
    ] {
        let got = compare(spec, &local("csv-b"), &KEY, &env, None);
        assert_eq!(got.0, 2, "{spec}: {}", got.1);
        assert!(got.1.contains(says), "{spec}: {}", got.1);
    }
}

/// Runs `compara compare` with the environment cleared of every credential
/// the test process may carry, and `env` set; exit code and stderr.
fn compare_bare(a: &str, b: &str, env: &[(&str, &str)]) -> (i32, String) {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_compara"));
    cmd.args(["compare", a, b]).args(KEY);
    for (k, _) in std::env::vars() {
        if k.starts_with("AWS_") || k.starts_with("AZURE_") || k.starts_with("GOOGLE_") {
            cmd.env_remove(k);
        }
    }
    let out = cmd
        .env("HOME", std::env::temp_dir().join("compara-no-home"))
        .env("AWS_EC2_METADATA_DISABLED", "true")
        .envs(env.iter().copied())
        .output()
        .unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

/// With no credentials anywhere, a public bucket is read unsigned and says
/// so; a private one still fails on the credentials. A profile whose keys come
/// from a `credential_process` -- what SSO and assumed roles reduce to -- is
/// run for them.
#[test]
fn public_buckets_need_no_credentials_and_profiles_run_their_process() {
    let f = fixture("20k", &[]);
    let bucket = f.0.join("bkt");
    let local = |n: &str| bucket.join(n).to_string_lossy().into_owned();
    let want = compare(&local("t_a.csv"), &local("t_b.csv"), &KEY, &[], None);
    let Some(public) = Server::start(&f.0, &[]) else {
        return;
    };
    let endpoint = [
        ("AWS_ENDPOINT_URL", public.url.as_str()),
        ("AWS_REGION", "us-east-1"),
    ];
    let (code, err) = compare_bare("s3://bkt/t_a.csv", "s3://bkt/t_b.csv", &endpoint);
    assert_eq!(code, want.0, "{err}");
    assert!(err.contains("unsigned"), "{err}");
    drop(public);

    let Some(private) = Server::start(&f.0, &["--private"]) else {
        return;
    };
    let endpoint = [
        ("AWS_ENDPOINT_URL", private.url.as_str()),
        ("AWS_REGION", "us-east-1"),
    ];
    let (code, err) = compare_bare("s3://bkt/t_a.csv", "s3://bkt/t_b.csv", &endpoint);
    assert_eq!(code, 2, "{err}");
    assert!(err.contains("no usable credentials"), "{err}");

    let home = f.0.join("home");
    std::fs::create_dir_all(home.join(".aws")).unwrap();
    std::fs::write(
        home.join(".aws/config"),
        "[profile ci]\nregion = us-east-1\ncredential_process = echo '{\"Version\": 1, \
         \"AccessKeyId\": \"AKIDEXAMPLE\", \"SecretAccessKey\": \"secret\"}'\n",
    )
    .unwrap();
    let home = home.to_string_lossy().into_owned();
    let env = [
        ("AWS_ENDPOINT_URL", private.url.as_str()),
        ("AWS_PROFILE", "ci"),
        ("HOME", home.as_str()),
    ];
    if cfg!(unix) {
        let (code, err) = compare_bare("s3://bkt/t_a.csv", "s3://bkt/t_b.csv", &env);
        assert_eq!(code, want.0, "{err}");
        assert!(!err.contains("unsigned"), "{err}");
    }
}

/// A table at a cloud metadata address (where a machine's credentials come
/// from) is refused: given directly, or reached by a redirect.
#[test]
fn metadata_addresses_are_not_read() {
    let run = |url: &str| {
        let out = Command::new(env!("CARGO_BIN_EXE_compara"))
            .args(["head", url])
            .env_remove("COMPARA_ALLOW_LINK_LOCAL")
            .output()
            .expect("run compara");
        (
            out.status.code(),
            String::from_utf8_lossy(&out.stderr).to_string(),
        )
    };
    let (code, err) = run("http://169.254.169.254/latest/meta-data/x.csv");
    assert_eq!(code, Some(2), "{err}");
    assert!(err.contains("link-local"), "{err}");
    // A server of our own that sends every request there.
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::thread::spawn(move || {
        for mut stream in listener.incoming().flatten() {
            let mut buf = [0u8; 4096];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            let _ = stream.write_all(
                b"HTTP/1.1 302 Found\r\nLocation: http://169.254.169.254/latest/x.csv\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
            );
        }
    });
    let (code, err) = run(&format!("http://127.0.0.1:{port}/redir.csv"));
    assert_eq!(code, Some(2), "{err}");
    assert!(err.contains("link-local"), "{err}");
    // Through a proxy, which would look the name up itself and answer from
    // the metadata service: refused before the proxy hears of it.
    let proxy = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let proxy_url = format!("http://127.0.0.1:{}", proxy.local_addr().unwrap().port());
    let asked = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = asked.clone();
    std::thread::spawn(move || {
        for mut stream in proxy.incoming().flatten() {
            counted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let mut buf = [0u8; 4096];
            let _ = std::io::Read::read(&mut stream, &mut buf);
            let _ = stream.write_all(
                b"HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nkey\nsec\n",
            );
        }
    });
    let out = Command::new(env!("CARGO_BIN_EXE_compara"))
        .args([
            "head",
            "http://metadata.google.internal/computeMetadata/v1/x.csv",
        ])
        .env_remove("COMPARA_ALLOW_LINK_LOCAL")
        .env_remove("NO_PROXY")
        .env_remove("no_proxy")
        .env("HTTP_PROXY", &proxy_url)
        .env("HTTPS_PROXY", &proxy_url)
        .output()
        .expect("run compara");
    let err = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{err}");
    assert!(err.contains("link-local"), "{err}");
    assert_eq!(
        asked.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "the proxy was asked"
    );
}
