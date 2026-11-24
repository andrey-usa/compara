//! Where a side's bytes come from.
//!
//! The engines read a plain local file: they map it, and everything they do
//! after that is pointer arithmetic over the mapping. So every other kind of
//! input is turned into one first, and the engines never know:
//!
//! - a local file is read where it is;
//! - `-` is standard input, spooled to a temporary file;
//! - a gzip or zstd file (`.csv.gz`, `.ndjson.zst`, whatever its name) is
//!   decompressed, by its first bytes rather than its name: on Linux into an
//!   in-memory file while it fits a quarter of the memory free, to a
//!   temporary file past that and elsewhere;
//! - with the `cloud` feature, an `s3://`, `gs://`, `az://` or `https://` URL
//!   is downloaded in parallel ranges, and a Parquet object only as far as the
//!   comparison reads it: its footer and the column chunks of the columns it
//!   compares (see [`remote`]).
//!
//! Temporary files live in the system's temporary directory (`TMPDIR`) and go
//! when the [`Source`] does; a temporary file or directory that a killed run
//! left there goes with the next run that makes one. In a container that is the writable scratch
//! space -- an `emptyDir` under a read-only root -- and a page cache the
//! kernel can reclaim, where holding the bytes in memory would count against
//! the container's limit until the end. A decompressed input is the exception
//! while it is small next to what is free (within that limit): writing it
//! out made the disk, not the decoder, the slowest step of a compressed
//! comparison -- 8 to 30 s a side for a 1.7 GB table on a VM whose disk
//! takes 150 MB/s, against 3.5 s, the decoder's own pace, into memory.

use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use crate::error::{Error, Result};
use crate::options::Options;

mod delta;
mod many;
#[cfg(feature = "cloud")]
mod remote;
#[cfg(unix)]
mod sweep;

/// One side of a comparison, as a local file the engines can map.
pub struct Source {
    spec: String,
    path: PathBuf,
    /// The bytes the input holds where it is stored: the object's size, the
    /// compressed file's, what came in on stdin.
    stored: u64,
    temps: Vec<Temp>,
    steps: Vec<String>,
    /// The in-memory file `path` names, when the input was expanded into one;
    /// it goes when the [`Source`] does.
    held: Option<File>,
}

impl Drop for Source {
    fn drop(&mut self) {
        self.temps.clear();
        if let Ok(mut given) = GIVEN.lock() {
            given.retain(|(path, _)| *path != self.path);
        }
    }
}

/// The local files that stand for something else, with what the user gave:
/// standard input, a compressed file, a URL. An engine is handed the local
/// file, and a note it prints should name the input.
static GIVEN: std::sync::Mutex<Vec<(PathBuf, String)>> = std::sync::Mutex::new(Vec::new());

/// What the user called the input `path` holds: `standard input`, the file
/// or URL given, or the path itself for a file read where it is.
pub fn given_name(path: &Path) -> String {
    GIVEN
        .lock()
        .ok()
        .and_then(|given| {
            given
                .iter()
                .find(|(local, _)| local == path)
                .map(|(_, name)| name.clone())
        })
        .unwrap_or_else(|| path.display().to_string())
}

/// Which columns a comparison reads, so a remote Parquet object is fetched
/// only as far as that.
#[derive(Clone, Debug, Default)]
pub struct Want {
    key: Vec<String>,
    compare: Option<Vec<String>>,
    ignore: Vec<String>,
    /// Only the footer: `columns` asks for nothing else.
    footer_only: bool,
}

impl Want {
    /// Every column.
    pub fn all() -> Self {
        Want::default()
    }

    /// The schema and nothing else.
    pub fn footer_only() -> Self {
        Want {
            footer_only: true,
            ..Want::default()
        }
    }

    /// What a comparison with these options reads: the key, and the compared
    /// columns -- all but the ignored ones, or those `--compare` names. A
    /// superset of what the engines resolve, never less: a column present on
    /// one side only is fetched and then not compared.
    pub fn for_options(opt: &Options) -> Self {
        Want {
            key: opt.key.clone(),
            compare: opt.compare.clone(),
            ignore: opt.ignore.clone(),
            footer_only: false,
        }
    }

    /// Whether the column called `name` is read.
    pub fn needs(&self, name: &str) -> bool {
        if self.footer_only {
            return false;
        }
        if self.key.iter().any(|k| k == name) {
            return true;
        }
        if self.ignore.iter().any(|i| i == name) {
            return false;
        }
        match &self.compare {
            Some(list) => list.iter().any(|c| c == name),
            None => true,
        }
    }
}

/// Whether `spec` names something to download rather than a local path.
pub fn is_remote(spec: &str) -> bool {
    let Some((scheme, rest)) = spec.split_once("://") else {
        return false;
    };
    !rest.is_empty()
        && matches!(
            scheme.to_ascii_lowercase().as_str(),
            "s3" | "s3a" | "gs" | "az" | "adl" | "azure" | "abfs" | "abfss" | "http" | "https"
        )
}

/// Whether the local input `spec` (a path, a directory or a glob) is, or would
/// read, `file`: the files a run must not write over.
pub fn reads_file(spec: &str, file: &Path) -> bool {
    !is_remote(spec) && spec != "-" && many::would_read(spec, file)
}

impl Source {
    /// Opens `spec` -- a path, `-` or a URL -- as a local file.
    pub fn open(spec: &str, want: &Want) -> Result<Source> {
        let source = if spec == "-" {
            Source::stdin()?
        } else if is_remote(spec) {
            Source::remote(spec, want)?
        } else if let Some((files, note)) = many::local_files(spec)? {
            Source::local_many(spec, files, &note)?
        } else {
            let path = PathBuf::from(spec);
            let meta = std::fs::metadata(&path)
                .map_err(|_| Error::new(format!("file not found: {spec}")))?;
            if !meta.is_file() {
                // A pipe can be read once and not seeked, and compara reads
                // its inputs more than once: say how it takes one instead.
                return Err(Error::input(format!(
                    "{spec} is not a regular file (a pipe, a device or a process substitution); \
                     compara reads a pipe as standard input: `… | compara compare - B`, or save it \
                     to a file first"
                )));
            }
            Source {
                spec: spec.to_string(),
                path,
                stored: meta.len(),
                temps: Vec::new(),
                steps: Vec::new(),
                held: None,
            }
        };
        source.ready()
    }

    /// What every input goes through once it is a local file: named as
    /// messages name it, expanded when compressed, and its local file tied to
    /// what the user gave.
    fn ready(mut self) -> Result<Source> {
        // Named from here on as messages name it: a URL without its
        // credentials.
        if let std::borrow::Cow::Owned(shown) = crate::redact::urls(&self.spec) {
            self.spec = shown;
        }
        #[cfg(all(test, unix))]
        tests::written(&self.path);
        self.decompress()?;
        #[cfg(all(test, unix))]
        tests::written(&self.path);
        if self.path != Path::new(&self.spec)
            && let Ok(mut given) = GIVEN.lock()
        {
            let name = if self.spec == "-" {
                "standard input".to_string()
            } else {
                self.spec.clone()
            };
            given.push((self.path.clone(), name));
        }
        Ok(self)
    }

    /// What the caller named: the path, `-`, or the URL with no credential
    /// in it ([`crate::redact::urls`]).
    pub fn spec(&self) -> &str {
        &self.spec
    }

    /// The local file to read.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The input's size where it is stored, before decompression.
    pub fn stored_size(&self) -> u64 {
        self.stored
    }

    /// What was done to get the local file, one line a step (`downloaded
    /// 3.4 GB in 2.1s`), for a note on stderr; empty for a local file read
    /// where it is.
    pub fn steps(&self) -> &[String] {
        &self.steps
    }

    /// The last segment of the spec, as a report names a file.
    pub fn name(&self) -> String {
        if self.spec == "-" {
            return "stdin".to_string();
        }
        let trimmed = self.spec.split(['?', '#']).next().unwrap_or(&self.spec);
        trimmed
            .rsplit(['/', '\\'])
            .find(|s| !s.is_empty())
            .unwrap_or(trimmed)
            .to_string()
    }

    /// The files of a local directory or glob, in path order, as one.
    fn local_many(spec: &str, files: Vec<(String, PathBuf)>, note: &str) -> Result<Source> {
        let started = Instant::now();
        let (mut combined, temp) = many::Combined::new()?;
        let relative: Vec<String> = files.iter().map(|f| f.0.clone()).collect();
        let partitions = many::table_partitions(&relative);
        if let Some((names, _)) = &partitions {
            combined.set_partitions(names.clone());
        }
        let mut source = Source {
            spec: spec.to_string(),
            path: combined.path.clone(),
            stored: 0,
            temps: vec![temp],
            steps: Vec::new(),
            held: None,
        };
        let total: u64 = files
            .iter()
            .map(|f| std::fs::metadata(&f.1).map(|m| m.len()).unwrap_or(0))
            .sum();
        room_for(total, spec)?;
        for (i, (_, file)) in files.iter().enumerate() {
            source.stored += std::fs::metadata(file).map(|m| m.len()).unwrap_or(0);
            let values = partitions.as_ref().map_or(&[][..], |p| &p.1[i][..]);
            combined.append_file(&file.display().to_string(), file, values)?;
        }
        combined.finish()?;
        source.steps.push(format!(
            "{}{note} in {:.1}s",
            combined.note(),
            started.elapsed().as_secs_f64()
        ));
        Ok(source)
    }

    fn stdin() -> Result<Source> {
        Source::spool(&mut std::io::stdin().lock())
    }

    /// Standard input, or what stands for it in a test, spooled to a
    /// temporary file.
    fn spool<R: Read>(input: &mut R) -> Result<Source> {
        let started = Instant::now();
        let (temp, file) = temp_file("stdin")?;
        let mut source = Source {
            spec: "-".to_string(),
            path: temp.path().to_path_buf(),
            stored: 0,
            temps: vec![temp],
            steps: Vec::new(),
            held: None,
        };
        let mut out = BufWriter::with_capacity(1 << 20, crate::memory::WriteBehind::new(file));
        let n = std::io::copy(input, &mut out)
            .and_then(|n| out.flush().map(|_| n))
            .map_err(|e| Error::new(format!("cannot read standard input: {e}")))?;
        source.stored = n;
        source.steps.push(format!(
            "read {} from standard input in {:.1}s",
            human(n),
            started.elapsed().as_secs_f64()
        ));
        Ok(source)
    }

    #[cfg(feature = "cloud")]
    fn remote(spec: &str, want: &Want) -> Result<Source> {
        remote::fetch(spec, want)
    }

    #[cfg(not(feature = "cloud"))]
    fn remote(spec: &str, _want: &Want) -> Result<Source> {
        Err(Error::new(format!(
            "{spec}: this build reads local files only (it was built without the `cloud` \
             feature); download the file first, or use a release build"
        )))
    }

    /// Decompresses a gzip or zstd file to a temporary one, and refuses the
    /// compressed formats it cannot read with what they are, rather than letting
    /// a CSV reader report the binary as a missing key column.
    fn decompress(&mut self) -> Result<()> {
        let mut head = [0u8; 6];
        let n = {
            let mut f = open_input(&self.path)
                .map_err(|e| Error::new(format!("cannot read {}: {e}", self.spec)))?;
            read_up_to(&mut f, &mut head)?
        };
        let head = &head[..n];
        let kind = if head.starts_with(&[0x1f, 0x8b]) {
            "gzip"
        } else if head.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
            "zstd"
        } else {
            let other = if head.starts_with(b"BZh") {
                Some("bzip2")
            } else if head.starts_with(&[0xfd, b'7', b'z', b'X', b'Z', 0]) {
                Some("xz")
            } else if head.starts_with(b"PK\x03\x04") {
                Some("zip")
            } else {
                None
            };
            if let Some(other) = other {
                return Err(Error::new(format!(
                    "{} is {other}-compressed, which is not read here; gzip and zstd are \
                     (or decompress it first)",
                    self.spec
                )));
            }
            return Ok(());
        };
        let started = Instant::now();
        let input = BufReader::with_capacity(
            1 << 20,
            open_input(&self.path)
                .map_err(|e| Error::new(format!("cannot read {}: {e}", self.spec)))?,
        );
        let budget = crate::memory::in_memory_budget();
        #[cfg(all(test, unix))]
        let memory = (!tests::ON_DISK.get()).then(memfd::create).flatten();
        #[cfg(not(all(test, unix)))]
        let memory = memfd::create();
        let mut out = match memory {
            Some(file) => Sink::Memory(file),
            None => Sink::disk(&mut self.temps)?,
        };
        let read = std::rc::Rc::new(std::cell::Cell::new(0u64));
        let input = Counted(input, read.clone());
        let bad = |e: std::io::Error| {
            Error::new(format!("{} does not {kind}-decompress: {e}", self.spec))
        };
        let mut decoder: Box<dyn Read> = match kind {
            "gzip" => Box::new(flate2::read::MultiGzDecoder::new(input)),
            _ => Box::new(zstd::stream::read::Decoder::new(input).map_err(bad)?),
        };
        let ratio = max_expansion();
        let mut buf = vec![0u8; 1 << 20];
        let mut written = 0u64;
        loop {
            let k = match decoder.read(&mut buf) {
                Ok(0) => break,
                Ok(k) => k,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(bad(e)),
            };
            if let Sink::Memory(file) = &mut out
                && written + k as u64 > budget
            {
                out = Sink::spill(file, &mut self.temps)?;
            }
            out.write_all(&buf[..k]).map_err(bad)?;
            written += k as u64;
            if ratio > 0 && written > EXPANSION_FLOOR && written / read.get().max(1) > ratio {
                return Err(Error::new(format!(
                    "{} expands to more than {ratio} times its size ({} from {} so far): a \
                     decompression bomb? COMPARA_MAX_EXPANSION=<times> (0: no limit) lets it",
                    self.spec,
                    human(written),
                    human(read.get())
                )));
            }
        }
        out.flush().map_err(bad)?;
        // A downloaded or spooled compressed file is not needed once it is
        // expanded; a local one is the caller's and stays.
        if let Some(i) = self.temps.iter().position(|t| t.path() == self.path) {
            drop(self.temps.remove(i));
        }
        let place = match out {
            Sink::Memory(file) => {
                crate::memory::hold(written);
                self.path = memfd::path(&file);
                self.held = Some(file);
                " in memory"
            }
            Sink::Disk(_, path) => {
                self.path = path;
                ""
            }
        };
        self.steps.push(format!(
            "{kind}-decompressed {} to {}{place} in {:.1}s",
            human(self.stored),
            human(written),
            started.elapsed().as_secs_f64()
        ));
        Ok(())
    }
}

/// Where a decompressed input goes.
enum Sink {
    Memory(File),
    Disk(BufWriter<crate::memory::WriteBehind>, PathBuf),
}

impl Sink {
    fn disk(temps: &mut Vec<Temp>) -> Result<Sink> {
        let (temp, file) = temp_file("plain")?;
        let path = temp.path().to_path_buf();
        temps.push(temp);
        let out = BufWriter::with_capacity(1 << 20, crate::memory::WriteBehind::new(file));
        Ok(Sink::Disk(out, path))
    }

    /// A temporary file holding what the in-memory one does, which it
    /// replaces.
    fn spill(memory: &mut File, temps: &mut Vec<Temp>) -> Result<Sink> {
        use std::io::{Seek, SeekFrom};
        let mut disk = Sink::disk(temps)?;
        memory.seek(SeekFrom::Start(0))?;
        std::io::copy(memory, &mut disk)?;
        Ok(disk)
    }
}

impl Write for Sink {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Sink::Memory(file) => file.write(buf),
            Sink::Disk(out, _) => out.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Sink::Memory(file) => file.flush(),
            Sink::Disk(out, _) => out.flush(),
        }
    }
}

/// Files in memory, which the engines map as they would any other file
/// (through `/proc/self/fd`). Linux only; elsewhere [`create`] says no.
mod memfd {
    use std::fs::File;
    use std::path::PathBuf;

    #[cfg(target_os = "linux")]
    pub fn create() -> Option<File> {
        use std::os::fd::FromRawFd;
        unsafe extern "C" {
            fn memfd_create(
                name: *const std::ffi::c_char,
                flags: std::ffi::c_uint,
            ) -> std::ffi::c_int;
        }
        const CLOEXEC: std::ffi::c_uint = 1;
        if !std::path::Path::new("/proc/self/fd").is_dir() {
            return None;
        }
        // SAFETY: a NUL-terminated name; the descriptor returned is new and
        // owned by the File from here on.
        let fd = unsafe { memfd_create(c"compara-plain".as_ptr(), CLOEXEC) };
        (fd >= 0).then(|| unsafe { File::from_raw_fd(fd) })
    }

    #[cfg(not(target_os = "linux"))]
    pub fn create() -> Option<File> {
        None
    }

    #[cfg(target_os = "linux")]
    pub fn path(file: &File) -> PathBuf {
        use std::os::fd::AsRawFd;
        PathBuf::from(format!("/proc/self/fd/{}", file.as_raw_fd()))
    }

    #[cfg(not(target_os = "linux"))]
    pub fn path(_: &File) -> PathBuf {
        unreachable!("no in-memory files here")
    }
}

/// Tables expand 3 to 30 times their gzip or zstd size; a decompression
/// bomb thousands of times. Past [`EXPANSION_FLOOR`] of output, more than
/// this many times the input read so far is refused (`COMPARA_MAX_EXPANSION`,
/// 0 for no limit), so a small file of one repeated value still opens.
const MAX_EXPANSION: u64 = 1000;
const EXPANSION_FLOOR: u64 = 1 << 30;

fn max_expansion() -> u64 {
    std::env::var("COMPARA_MAX_EXPANSION")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(MAX_EXPANSION)
}

/// The refusal for a Parquet page that claims more than its bytes can hold
/// by the same rule: past [`EXPANSION_FLOOR`], more than [`MAX_EXPANSION`]
/// times what is stored. A page's value count and uncompressed size each size
/// an allocation before the page is read, so a 9.7 KB file that claimed 1.47
/// billion values had 2.8 GB reserved for them (found by fuzzing). Real
/// writers put thousands to a few million values in a page.
pub(crate) fn page_bomb(values: u64, uncompressed: u64, stored: u64) -> Option<String> {
    let ratio = max_expansion();
    let past = |n: u64| ratio > 0 && n > EXPANSION_FLOOR && n / stored.max(1) > ratio;
    let what = if past(values) {
        format!("{values} values")
    } else if past(uncompressed) {
        human(uncompressed)
    } else {
        return None;
    };
    Some(format!(
        "a Parquet page claims {what} in {} stored: a decompression bomb? \
         COMPARA_MAX_EXPANSION=<times> (0: no limit) lets it",
        human(stored)
    ))
}

/// A reader that counts what it has read into the cell.
struct Counted<R>(R, std::rc::Rc<std::cell::Cell<u64>>);

impl<R: Read> Read for Counted<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.0.read(buf)?;
        self.1.set(self.1.get() + n as u64);
        Ok(n)
    }
}

fn read_up_to(f: &mut File, buf: &mut [u8]) -> Result<usize> {
    let mut n = 0;
    while n < buf.len() {
        match f.read(&mut buf[n..]) {
            Ok(0) => break,
            Ok(k) => n += k,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(n)
}

/// A name no file in the temporary directory has yet, for this process: its
/// id, so a crash leaves something recognisable behind, and a number that
/// cannot be guessed from outside it, so another user of a shared temporary
/// directory cannot take the names first and stop the run.
fn temp_path(tag: &str) -> PathBuf {
    use std::hash::{BuildHasher, Hasher};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    static SALT: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
    // The standard library seeds each `RandomState` from the system's
    // randomness.
    let salt = SALT.get_or_init(|| {
        std::collections::hash_map::RandomState::new()
            .build_hasher()
            .finish()
    });
    let n = NEXT.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "compara-{}-{:08x}-{n}-{tag}",
        std::process::id(),
        salt & 0xffff_ffff
    ))
}

fn no_temp(what: &str, e: &std::io::Error) -> Error {
    Error::new(format!(
        "cannot create a temporary {what} in {} ({e}); set TMPDIR to a writable directory",
        std::env::temp_dir().display()
    ))
}

/// A temporary file of this run's, removed when this goes.
///
/// Until then it is locked, which is what tells a later run, in any pid
/// namespace, that this one is not over; its name ends `-locked` to say so
/// (see [`sweep`]). On Unix it is also held open from its making, on a
/// descriptor of its own (the one locked), whatever becomes of the one it
/// was made with: most are closed once the file is written, and what reads
/// it next -- the input's expansion, the engines -- opens it again by its
/// name, through [`open_input`], which reads it only while the name is still
/// this file's.
pub(crate) struct Temp {
    path: PathBuf,
    #[cfg(unix)]
    _held: File,
}

impl Temp {
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// The file `file` made at `path`, held and known by its device and
    /// inode from now on: neither is another file's while it is held open.
    #[cfg(unix)]
    fn held(path: PathBuf, file: File) -> Result<Temp> {
        use std::os::unix::fs::MetadataExt;
        let made = file.metadata().map_err(|e| no_temp("file", &e))?;
        if let Ok(mut written) = WRITTEN.lock() {
            written.push((path.clone(), (made.dev(), made.ino())));
        }
        Ok(Temp { path, _held: file })
    }
}

impl Drop for Temp {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Ok(mut written) = WRITTEN.lock() {
            written.retain(|(path, _)| *path != self.path);
        }
        // Before the lock goes with the fields: the file is not unlocked
        // while it is there.
        let _ = std::fs::remove_file(&self.path);
    }
}

/// This process's temporary files, by name, with the device and inode of
/// the file each was made as ([`Temp::held`]).
#[cfg(unix)]
static WRITTEN: std::sync::Mutex<Vec<(PathBuf, (u64, u64))>> = std::sync::Mutex::new(Vec::new());

/// Opens `path`, an input to compare, to read: what the engines read.
///
/// A temporary file of this run's -- standard input spooled, a download, a
/// decompressed or combined input -- is opened as the sweep opens a name
/// ([`sweep::reopen`]: not through a link, not waiting on a pipe), and only
/// if what it opens is the file this run made there. Where anyone may rename
/// in the temporary directory (no sticky bit), the name may by now be
/// someone else's file, or a link to one, and reading it would compare their
/// data in place of the input. The check is on the descriptor that is then
/// read or mapped, so nothing can come between them.
pub fn open_input(path: &Path) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        let made = WRITTEN.lock().ok().and_then(|written| {
            written
                .iter()
                .find(|(at, _)| at == path)
                .map(|&(_, made)| made)
        });
        if let Some(made) = made {
            use std::os::unix::fs::MetadataExt;
            let replaced = |e: std::io::Error| {
                std::io::Error::other(format!(
                    "{e}; it is no longer the temporary file this run wrote there (another user \
                     can rename files in {} where it has no sticky bit); set TMPDIR to a \
                     directory of your own",
                    std::env::temp_dir().display()
                ))
            };
            let file = sweep::reopen(path, false).map_err(replaced)?;
            let opened = file.metadata().map_err(replaced)?;
            if (opened.dev(), opened.ino()) != made {
                return Err(replaced(std::io::Error::other(
                    "another file is at its name",
                )));
            }
            return Ok(file);
        }
    }
    File::open(path)
}

/// The metadata of `path`, an input to compare: of the file
/// [`open_input`] would read.
pub fn input_metadata(path: &Path) -> std::io::Result<std::fs::Metadata> {
    #[cfg(unix)]
    if WRITTEN
        .lock()
        .is_ok_and(|written| written.iter().any(|(at, _)| at == path))
    {
        return open_input(path)?.metadata();
    }
    std::fs::metadata(path)
}

/// A new file in the temporary directory, for this user alone (see
/// [`new_temp_file`]), locked while the [`Temp`] lives. The first a process
/// makes removes, first, the files that killed runs left there.
pub(crate) fn temp_file(tag: &str) -> Result<(Temp, File)> {
    sweep_once();
    #[cfg(unix)]
    {
        let (path, file) = new_temp_file(&format!("{tag}-{}", sweep::LOCKED))?;
        if let Some(lock) = file.try_clone().ok().filter(|l| l.try_lock().is_ok()) {
            return Ok((Temp::held(path, lock)?, file));
        }
        // As for a directory (`TempDir::new`): where the lock cannot be
        // taken, the file is one of an unlocked run's, which a later run
        // leaves for a day.
        drop(file);
        let _ = std::fs::remove_file(&path);
        let (path, file) = new_temp_file(tag)?;
        let held = file.try_clone().map_err(|e| no_temp("file", &e))?;
        Ok((Temp::held(path, held)?, file))
    }
    #[cfg(not(unix))]
    {
        let (path, file) = new_temp_file(tag)?;
        Ok((Temp { path }, file))
    }
}

/// A new file in the temporary directory, for this user alone.
///
/// What goes in one is the data being compared: standard input, a download
/// made with the caller's credentials, a decompressed copy. The temporary
/// directory is shared on most machines, so the file is made readable and
/// writable by its owner only (it was made with the default mode, which
/// leaves it readable by every user), and only where nothing is: a file or a
/// link already at the name is not opened.
fn new_temp_file(tag: &str) -> Result<(PathBuf, File)> {
    for _ in 0..16 {
        let path = temp_path(tag);
        let mut options = OpenOptions::new();
        options.read(true).write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        match options.open(&path) {
            Ok(file) => return Ok((path, file)),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(no_temp("file", &e)),
        }
    }
    Err(Error::new("cannot find a free temporary file name"))
}

/// A new directory in the temporary directory, for this user alone: made
/// in one step, with no moment at which another user could put something at
/// its name, and closed to everyone else, so what is written inside it is
/// too.
fn temp_dir(tag: &str) -> Result<PathBuf> {
    for _ in 0..16 {
        let path = temp_path(tag);
        let mut builder = std::fs::DirBuilder::new();
        // This one directory, and only where nothing is.
        builder.recursive(false);
        #[cfg(unix)]
        std::os::unix::fs::DirBuilderExt::mode(&mut builder, 0o700);
        match builder.create(&path) {
            Ok(()) => return Ok(path),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(no_temp("directory", &e)),
        }
    }
    Err(Error::new("cannot find a free temporary directory name"))
}

/// Where each cloud's credentials would come from here (scheme, source), by
/// what is set and on disk; empty without the `cloud` feature.
pub fn credential_sources() -> Vec<(&'static str, String)> {
    #[cfg(feature = "cloud")]
    {
        remote::credential_sources()
    }
    #[cfg(not(feature = "cloud"))]
    {
        Vec::new()
    }
}

/// Writes the file `from`, read from its start, to `dest`, an object
/// storage or HTTP URL; the bytes written.
#[cfg(feature = "cloud")]
pub fn upload(from: File, dest: &str) -> Result<u64> {
    remote::upload(from, dest)
}

/// Writes the file `from` to `dest`, an object storage or HTTP URL.
#[cfg(not(feature = "cloud"))]
pub fn upload(_from: File, dest: &str) -> Result<u64> {
    Err(Error::new(format!(
        "{dest}: this build writes local files only (it was built without the `cloud` feature)"
    )))
}

/// A directory of this user's alone for what is written on the way to
/// somewhere else -- an export bound for object storage, the runs an
/// external sort spills -- removed with what is in it when dropped.
///
/// A run killed before then leaves it behind; the first directory a later
/// run makes removes those first. The directory is locked for as long as it
/// is used, which is what tells that later run, in any pid namespace, that
/// this one is not over, and its name ends `-locked` to say it is.
///
/// It is held open from its making, and what goes in it is made, read and
/// removed through it ([`WorkDir`]): where anyone may rename in the
/// temporary directory (no sticky bit), its name may be swapped at any time
/// for a link or for someone else's directory, and a write by the name
/// would land there.
pub struct TempDir {
    dir: Arc<WorkDir>,
}

impl TempDir {
    pub fn new(tag: &str) -> Result<TempDir> {
        sweep_once();
        #[cfg(unix)]
        {
            let path = temp_dir(&format!("{tag}-{}", sweep::LOCKED))?;
            // Opened as the sweep opens a name it has looked at: where anyone
            // may rename in the temporary directory, a pipe, a link or another
            // user's directory may be at the name by now, and the open would
            // wait on the one for a writer, or lock and write into what the
            // others are.
            let opened = sweep::reopen(&path, true)
                .ok()
                .filter(sweep::is_own_private_dir);
            if let Some(lock) = opened.filter(|f| f.try_lock().is_ok()) {
                return Ok(TempDir::held(path, lock));
            }
            // A name that says "locked" on a directory nothing locks would
            // let a later run take it for over a minute after it last
            // changed: where the lock cannot be taken (a filesystem without
            // locks) the directory is one of an unlocked run's instead.
            let _ = std::fs::remove_dir(&path);
            let path = temp_dir(tag)?;
            let opened = sweep::reopen(&path, true).and_then(|f| {
                if sweep::is_own_private_dir(&f) {
                    Ok(f)
                } else {
                    Err(std::io::Error::other("another directory took its name"))
                }
            });
            match opened {
                Ok(dir) => Ok(TempDir::held(path, dir)),
                Err(e) => Err(no_temp("directory", &e)),
            }
        }
        #[cfg(not(unix))]
        Ok(TempDir {
            dir: Arc::new(WorkDir {
                path: temp_dir(tag)?,
            }),
        })
    }

    #[cfg(unix)]
    fn held(path: PathBuf, dir: File) -> TempDir {
        #[cfg(test)]
        sweep::made(&path);
        TempDir {
            dir: Arc::new(WorkDir {
                path,
                dir: sweep::Dir(dir),
            }),
        }
    }

    /// The name it was made at, for messages: what is in it is reached
    /// through [`TempDir::dir`].
    pub fn path(&self) -> &Path {
        &self.dir.path
    }

    /// The directory, held open.
    pub fn dir(&self) -> &Arc<WorkDir> {
        &self.dir
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        // Before the lock goes with the fields.
        #[cfg(unix)]
        {
            // Through the directory, then the directory itself by its name in
            // the temporary directory: what a swap can have put there is at
            // most an empty directory to remove, or a link, which this fails
            // on.
            self.dir.dir.clear(1);
            let _ = std::fs::remove_dir(&self.dir.path);
        }
        #[cfg(not(unix))]
        let _ = std::fs::remove_dir_all(&self.dir.path);
    }
}

/// A directory of this run's, held open: every file in it is made, read,
/// moved and removed through it, by a name relative to it, whatever has
/// become of the name it was made at. Elsewhere than on Unix, by the path.
#[derive(Debug)]
pub struct WorkDir {
    path: PathBuf,
    #[cfg(unix)]
    dir: sweep::Dir,
}

/// The same directory: the one at the same name.
impl PartialEq for WorkDir {
    fn eq(&self, other: &WorkDir) -> bool {
        self.path == other.path
    }
}

#[cfg(unix)]
fn c_name(name: &str) -> std::io::Result<std::ffi::CString> {
    std::ffi::CString::new(name).map_err(|_| std::io::ErrorKind::InvalidInput.into())
}

impl WorkDir {
    /// Where it was made, for messages.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Makes the file `name` in it, for this user alone, and opens it to
    /// write and read back; only where nothing is, so a file or a link at
    /// the name is not opened.
    pub fn create(&self, name: &str) -> std::io::Result<File> {
        #[cfg(unix)]
        {
            self.dir.create(&c_name(name)?)
        }
        #[cfg(not(unix))]
        {
            OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(self.path.join(name))
        }
    }

    /// The file `name` in it, to read.
    pub fn open(&self, name: &str) -> std::io::Result<File> {
        #[cfg(unix)]
        {
            self.dir.open(&c_name(name)?)
        }
        #[cfg(not(unix))]
        {
            File::open(self.path.join(name))
        }
    }

    /// Removes the file `name` from it.
    pub fn remove(&self, name: &str) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            if self.dir.remove(&c_name(name)?, false) {
                Ok(())
            } else {
                Err(std::io::Error::last_os_error())
            }
        }
        #[cfg(not(unix))]
        {
            std::fs::remove_file(self.path.join(name))
        }
    }

    /// Moves `from` in it to `to` in it, replacing what is there.
    pub fn rename(&self, from: &str, to: &str) -> std::io::Result<()> {
        #[cfg(unix)]
        {
            self.dir.rename(&c_name(from)?, &c_name(to)?)
        }
        #[cfg(not(unix))]
        {
            std::fs::rename(self.path.join(from), self.path.join(to))
        }
    }

    /// Makes the directory `name` in it, for this user alone, held open.
    pub fn make_dir(&self, name: &str) -> std::io::Result<WorkDir> {
        let path = self.path.join(name);
        #[cfg(unix)]
        {
            let dir = self.dir.make_dir(&c_name(name)?)?;
            Ok(WorkDir { path, dir })
        }
        #[cfg(not(unix))]
        {
            std::fs::create_dir(&path)?;
            Ok(WorkDir { path })
        }
    }

    /// The names of what is in it, in no order.
    pub fn names(&self) -> std::io::Result<Vec<String>> {
        let not_text = || std::io::Error::new(std::io::ErrorKind::InvalidData, "a name not UTF-8");
        #[cfg(unix)]
        {
            self.dir
                .names()
                .ok_or_else(|| std::io::Error::other("cannot list it"))?
                .into_iter()
                .map(|n| n.into_string().map_err(|_| not_text()))
                .collect()
        }
        #[cfg(not(unix))]
        {
            std::fs::read_dir(&self.path)?
                .map(|e| e?.file_name().into_string().map_err(|_| not_text()))
                .collect()
        }
    }
}

/// Removes, once a process, the temporary files and directories runs that
/// were killed left behind.
fn sweep_once() {
    #[cfg(unix)]
    {
        static SWEPT: std::sync::Once = std::sync::Once::new();
        SWEPT.call_once(|| {
            sweep::sweep(&std::env::temp_dir());
        });
    }
}

/// Whether a temporary file can be made: stdin, compressed and remote inputs
/// need one, and so does the engine that spills to disk.
pub fn temp_dir_writable() -> bool {
    // Only a look: nothing is swept or locked.
    match new_temp_file("probe") {
        Ok((path, _)) => {
            let _ = std::fs::remove_file(path);
            true
        }
        Err(_) => false,
    }
}

/// The bytes free to this process on the filesystem holding `dir`, where the
/// platform says.
pub fn free_space(dir: &Path) -> Option<u64> {
    #[cfg(all(target_os = "linux", target_pointer_width = "64"))]
    {
        use std::os::unix::ffi::OsStrExt;
        unsafe extern "C" {
            fn statvfs(path: *const std::ffi::c_char, buf: *mut u64) -> std::ffi::c_int;
        }
        let path = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
        // struct statvfs on 64-bit Linux: f_bsize, f_frsize, f_blocks,
        // f_bfree, f_bavail, ... -- all 64-bit, well inside 32 words.
        let mut buf = [0u64; 32];
        // SAFETY: a NUL-terminated path and a buffer larger than the struct.
        if unsafe { statvfs(path.as_ptr(), buf.as_mut_ptr()) } != 0 {
            return None;
        }
        Some(buf[4].saturating_mul(buf[1]))
    }
    #[cfg(not(all(target_os = "linux", target_pointer_width = "64")))]
    {
        let _ = dir;
        None
    }
}

/// Refuses, before anything is written, a temporary file the temporary
/// directory has no room for: an error up front that says how much is needed
/// and where, rather than "No space left on device" partway through a
/// download.
pub(crate) fn room_for(bytes: u64, what: &str) -> Result<()> {
    let dir = std::env::temp_dir();
    match free_space(&dir) {
        Some(free) if bytes > free => Err(Error::new(format!(
            "{what} needs {} in {}, which has {} free; set TMPDIR to a larger disk (in \
             Kubernetes, an emptyDir with room for it)",
            human(bytes),
            dir.display(),
            human(free)
        ))),
        _ => Ok(()),
    }
}

pub fn human(bytes: u64) -> String {
    Human(bytes).to_string()
}

/// [`human`] as a `Display`, for a message that is written where nothing
/// may be allocated ([`crate::alloc::refuse`]).
pub struct Human(pub u64);

impl std::fmt::Display for Human {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
        let mut v = self.0 as f64;
        let mut u = 0;
        while v >= 1024.0 && u + 1 < UNITS.len() {
            v /= 1024.0;
            u += 1;
        }
        if u == 0 {
            write!(f, "{} B", self.0)
        } else {
            write!(f, "{v:.1} {}", UNITS[u])
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(unix)]
    use std::cell::{Cell, RefCell};

    /// What a test does to the name of one of a run's temporary files once
    /// what it holds is written, before it is read back: the file's tag
    /// (`stdin`, `download`, ...), the swap, and the file of someone else's
    /// it swaps in.
    #[cfg(unix)]
    type Swap = (&'static str, fn(&Path, &Path), PathBuf);

    /// A row of the tests below: the kind of input, the tag of the
    /// temporary file it makes, and how it is opened.
    #[cfg(unix)]
    type Case<'a> = (
        &'static str,
        &'static str,
        Box<dyn Fn() -> Result<Source> + 'a>,
    );

    #[cfg(unix)]
    thread_local! {
        /// A decompressed input goes to a temporary file, as it does where
        /// there are no in-memory files or past the memory free.
        pub(super) static ON_DISK: Cell<bool> = const { Cell::new(false) };
        static SWAP: RefCell<Option<Swap>> = const { RefCell::new(None) };
        static SWAPPED: RefCell<Option<PathBuf>> = const { RefCell::new(None) };
    }

    /// Where a run's temporary file has been written and is about to be read
    /// back by what comes next: the input's expansion, the engines, the
    /// combined file a part goes into.
    #[cfg(unix)]
    pub(super) fn written(path: &Path) {
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        let hit = SWAP.with_borrow(|s| {
            s.as_ref().is_some_and(|(tag, _, _)| {
                name.ends_with(&format!("-{tag}")) || name.ends_with(&format!("-{tag}-locked"))
            })
        });
        if hit && let Some((_, swap, theirs)) = SWAP.take() {
            swap(path, &theirs);
            SWAPPED.set(Some(path.to_path_buf()));
        }
    }

    #[cfg(unix)]
    fn aside(path: &Path) -> PathBuf {
        let mut moved = path.as_os_str().to_owned();
        moved.push(".moved");
        PathBuf::from(moved)
    }

    /// Their file moved onto the name, as anyone can where the temporary
    /// directory is open to all without the sticky bit.
    #[cfg(unix)]
    fn onto(path: &Path, theirs: &Path) {
        std::fs::rename(path, aside(path)).unwrap();
        std::fs::rename(theirs, path).unwrap();
    }

    /// A link to their file at the name.
    #[cfg(unix)]
    fn link(path: &Path, theirs: &Path) {
        std::fs::rename(path, aside(path)).unwrap();
        std::os::unix::fs::symlink(theirs, path).unwrap();
    }

    /// A's rows, which B has too; and what another user swaps in.
    #[cfg(unix)]
    const OURS: &[u8] = b"id,v\n1,a\n2,b\n";
    #[cfg(unix)]
    const THEIRS: &[u8] = b"id,v\n1,x\n";

    #[cfg(unix)]
    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        e.write_all(bytes).unwrap();
        e.finish().unwrap()
    }

    /// What was compared, when `open` makes A with its `tag` file swapped
    /// by `swap` once written: `ours`, `theirs`, `refused` (the run stopped
    /// at the swap), or what else came of it.
    #[cfg(unix)]
    fn swapped_and_read(
        scratch: &Path,
        tag: &'static str,
        swap: fn(&Path, &Path),
        open: &dyn Fn() -> Result<Source>,
    ) -> String {
        let theirs = scratch.join("theirs.csv");
        std::fs::write(&theirs, THEIRS).unwrap();
        let b = scratch.join("b.csv");
        std::fs::write(&b, OURS).unwrap();
        SWAP.set(Some((tag, swap, theirs)));
        let said = |e: Error| {
            let e = e.to_string();
            if e.contains("no longer the temporary file") {
                "refused".to_string()
            } else {
                format!("error: {e}")
            }
        };
        let got = match open() {
            Err(e) => said(e),
            Ok(a) => {
                let mut opt = Options::with_key(["id"]);
                match crate::engine::compare(a.path(), &b, &mut opt) {
                    Ok(r) if (r.counts.changed, r.counts.added, r.counts.removed) == (0, 0, 0) => {
                        "ours".to_string()
                    }
                    Ok(_) => "theirs".to_string(),
                    Err(e) => said(e),
                }
            }
        };
        SWAP.set(None);
        match SWAPPED.take() {
            Some(at) => {
                let _ = std::fs::remove_file(aside(&at));
                let _ = std::fs::remove_file(&at);
                got
            }
            None => format!("not swapped ({got})"),
        }
    }

    /// A run's temporary file -- standard input spooled, an input expanded,
    /// the files of a directory combined -- is read back after it is
    /// written. Where anyone may rename in the temporary directory, its name
    /// may by then be someone else's file, or a link to one: what is read
    /// must be the file the run wrote, or nothing.
    #[cfg(unix)]
    #[test]
    fn a_temporary_file_is_read_back_only_as_the_run_wrote_it() {
        let scratch =
            std::env::temp_dir().join(format!("compara-read-back-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&scratch);
        std::fs::create_dir(&scratch).unwrap();
        let gz = scratch.join("a.csv.gz");
        std::fs::write(&gz, gzip(OURS)).unwrap();
        let zst = scratch.join("a.csv.zst");
        std::fs::write(&zst, zstd::stream::encode_all(OURS, 3).unwrap()).unwrap();
        let parts = scratch.join("parts");
        std::fs::create_dir(&parts).unwrap();
        std::fs::write(parts.join("1.csv"), b"id,v\n1,a\n").unwrap();
        std::fs::write(parts.join("2.csv"), b"id,v\n2,b\n").unwrap();
        let spec = |p: &Path| p.to_str().unwrap().to_string();
        let (gz, zst, parts) = (spec(&gz), spec(&zst), spec(&parts));
        let cases: [Case; 5] = [
            (
                "stdin",
                "stdin",
                Box::new(|| Source::spool(&mut &OURS[..])?.ready()),
            ),
            (
                "stdin, gzip",
                "stdin",
                Box::new(|| Source::spool(&mut &gzip(OURS)[..])?.ready()),
            ),
            (
                "gzip file",
                "plain",
                Box::new(|| Source::open(&gz, &Want::all())),
            ),
            (
                "zstd file",
                "plain",
                Box::new(|| Source::open(&zst, &Want::all())),
            ),
            (
                "directory",
                "combined",
                Box::new(|| Source::open(&parts, &Want::all())),
            ),
        ];
        // Unswapped, each reads its own.
        ON_DISK.set(true);
        let mut got = Vec::new();
        for (kind, tag, open) in &cases {
            got.push((
                *kind,
                "none",
                swapped_and_read(&scratch, "none", onto, open),
            ));
            for (how, swap) in [("theirs", onto as fn(&Path, &Path)), ("link", link)] {
                got.push((*kind, how, swapped_and_read(&scratch, tag, swap, open)));
            }
        }
        ON_DISK.set(false);
        let _ = std::fs::remove_dir_all(&scratch);
        let want: Vec<_> = got
            .iter()
            .map(|&(kind, how, _)| {
                let read = if how == "none" {
                    "not swapped (ours)"
                } else {
                    "refused"
                };
                (kind, how, read.to_string())
            })
            .collect();
        assert_eq!(got, want);
    }

    /// The same for what a download makes: an object or a URL fetched, and
    /// the objects under a prefix, fetched into one file or, compressed,
    /// each to a file of its own first. From a local stand-in for object
    /// storage (`tests/object_server.py`, skipped where there is no Python),
    /// in a child -- this test binary again -- whose environment points S3
    /// at it.
    #[cfg(all(unix, feature = "cloud"))]
    #[test]
    fn a_download_is_read_back_only_as_the_run_wrote_it() {
        use std::io::BufRead;
        const CHILD: &str = "COMPARA_TEST_READ_BACK_SERVERS";
        if let Ok(urls) = std::env::var(CHILD) {
            let (url, whole) = urls.split_once(' ').unwrap();
            downloads_swapped(url, whole);
            return;
        }
        let Some(python) = ["python3", "python"].into_iter().find(|p| {
            std::process::Command::new(p)
                .arg("--version")
                .output()
                .is_ok_and(|o| o.status.success())
        }) else {
            return;
        };
        let root =
            std::env::temp_dir().join(format!("compara-read-back-objects-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let bucket = root.join("b");
        for dir in ["gz", "plain"] {
            std::fs::create_dir_all(bucket.join(dir)).unwrap();
        }
        std::fs::write(bucket.join("a.csv"), OURS).unwrap();
        std::fs::write(bucket.join("a.csv.gz"), gzip(OURS)).unwrap();
        for (name, rows) in [("1.csv", &b"id,v\n1,a\n"[..]), ("2.csv", b"id,v\n2,b\n")] {
            std::fs::write(bucket.join("plain").join(name), rows).unwrap();
            std::fs::write(bucket.join("gz").join(format!("{name}.gz")), gzip(rows)).unwrap();
        }
        let mut servers = Vec::new();
        let mut urls = Vec::new();
        for extra in [&[][..], &["--no-ranges"]] {
            let mut child = std::process::Command::new(python)
                .arg(concat!(
                    env!("CARGO_MANIFEST_DIR"),
                    "/tests/object_server.py"
                ))
                .arg("--root")
                .arg(&root)
                .arg("--log")
                .arg(root.join(format!("log-{}.jsonl", servers.len())))
                .args(extra)
                .stdout(std::process::Stdio::piped())
                .spawn()
                .unwrap();
            let mut line = String::new();
            std::io::BufReader::new(child.stdout.take().unwrap())
                .read_line(&mut line)
                .unwrap();
            urls.push(line.trim().strip_prefix("LISTENING ").unwrap().to_string());
            servers.push(child);
        }
        let exe = std::env::current_exe().expect("the test binary");
        let out = std::process::Command::new(exe)
            .args([
                "--exact",
                "source::tests::a_download_is_read_back_only_as_the_run_wrote_it",
                "--test-threads=1",
                "--nocapture",
            ])
            .env(CHILD, urls.join(" "))
            .env("AWS_ENDPOINT_URL", &urls[0])
            .env("AWS_ACCESS_KEY_ID", "test")
            .env("AWS_SECRET_ACCESS_KEY", "test")
            .env("AWS_REGION", "us-east-1")
            .env("AWS_EC2_METADATA_DISABLED", "true")
            .output()
            .expect("the test binary runs");
        for mut server in servers {
            let _ = server.kill();
            let _ = server.wait();
        }
        let _ = std::fs::remove_dir_all(&root);
        assert!(
            out.status.success(),
            "{}{}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    /// [`a_download_is_read_back_only_as_the_run_wrote_it`], in the child:
    /// `url` serves ranges, `whole` does not.
    #[cfg(all(unix, feature = "cloud"))]
    fn downloads_swapped(url: &str, whole: &str) {
        let scratch =
            std::env::temp_dir().join(format!("compara-read-back-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&scratch);
        std::fs::create_dir(&scratch).unwrap();
        let open = |spec: String| move || Source::open(&spec, &Want::all());
        let cases: [Case; 6] = [
            ("url", "download", Box::new(open(format!("{url}/b/a.csv")))),
            (
                "url, no ranges",
                "download",
                Box::new(open(format!("{whole}/b/a.csv"))),
            ),
            (
                "url, gzip",
                "download",
                Box::new(open(format!("{url}/b/a.csv.gz"))),
            ),
            (
                "s3 object",
                "download",
                Box::new(open("s3://b/a.csv".into())),
            ),
            (
                "s3 prefix",
                "combined",
                Box::new(open("s3://b/plain/".into())),
            ),
            (
                "s3 prefix, gzip",
                "part",
                Box::new(open("s3://b/gz/".into())),
            ),
        ];
        let mut got = Vec::new();
        for (kind, tag, open) in &cases {
            got.push((
                *kind,
                "none",
                swapped_and_read(&scratch, "none", onto, open),
            ));
            for (how, swap) in [("theirs", onto as fn(&Path, &Path)), ("link", link)] {
                got.push((*kind, how, swapped_and_read(&scratch, tag, swap, open)));
            }
        }
        let _ = std::fs::remove_dir_all(&scratch);
        let want: Vec<_> = got
            .iter()
            .map(|&(kind, how, _)| {
                let read = if how == "none" {
                    "not swapped (ours)"
                } else {
                    "refused"
                };
                (kind, how, read.to_string())
            })
            .collect();
        assert_eq!(got, want);
    }

    #[test]
    fn a_temp_file_with_no_room_is_refused_up_front() {
        assert!(room_for(0, "nothing").is_ok());
        if free_space(&std::env::temp_dir()).is_some() {
            let e = room_for(u64::MAX, "s3://b/huge.csv")
                .unwrap_err()
                .to_string();
            assert!(
                e.contains("s3://b/huge.csv needs") && e.contains("TMPDIR"),
                "{e}"
            );
        }
    }

    /// A temporary file holds the data being compared, in a directory most
    /// machines share between their users: no one else may read it, and the
    /// same for a directory and what is written inside it. They were made
    /// with the default mode, readable by every user.
    #[cfg(unix)]
    #[test]
    fn temporary_files_and_directories_are_this_user_s_alone() {
        use std::os::unix::fs::PermissionsExt;
        let others = |path: &Path| std::fs::metadata(path).unwrap().permissions().mode() & 0o077;
        let (temp, file) = temp_file("probe").unwrap();
        let path = temp.path().to_path_buf();
        assert_eq!(others(&path), 0, "{}", path.display());
        drop(file);
        drop(temp);
        assert!(!path.exists(), "the file goes when it is dropped");

        let dir = TempDir::new("probe").unwrap();
        assert_eq!(others(dir.path()), 0, "{}", dir.path().display());
        // Two of one tag are two directories: neither takes the other's.
        let second = TempDir::new("probe").unwrap();
        assert_ne!(dir.path(), second.path());
        let kept = dir.path().to_path_buf();
        drop(dir);
        assert!(!kept.exists(), "the directory goes when it is dropped");
    }

    /// The name is not one another user could have taken first.
    #[test]
    fn a_temporary_name_cannot_be_guessed_from_the_process_id() {
        let name = temp_path("x")
            .file_name()
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let parts: Vec<&str> = name.split('-').collect();
        assert_eq!(parts[0], "compara");
        assert_eq!(parts[1], std::process::id().to_string());
        assert_eq!(parts[2].len(), 8, "{name}");
        assert!(parts[2].bytes().all(|b| b.is_ascii_hexdigit()), "{name}");
    }

    #[test]
    fn urls_are_remote_and_paths_are_not() {
        for spec in [
            "s3://bucket/a.csv",
            "S3://bucket/a.csv",
            "gs://b/x.parquet",
            "az://c/d.ndjson",
            "abfss://c@acct.dfs.core.windows.net/x",
            "https://example.com/a.csv",
            "http://127.0.0.1:9000/b/a.csv",
        ] {
            assert!(is_remote(spec), "{spec}");
        }
        for spec in [
            "a.csv",
            "/tmp/a.csv",
            "-",
            "C:\\data\\a.csv",
            "s3:/x",
            "ftp://h/a.csv",
            "s3://",
        ] {
            assert!(!is_remote(spec), "{spec}");
        }
    }

    #[test]
    fn a_want_reads_the_key_and_what_is_compared() {
        let mut opt = Options::with_key(["id"]);
        opt.ignore = vec!["updated_at".into(), "id".into()];
        let w = Want::for_options(&opt);
        assert!(w.needs("id"), "a key column is read even when ignored");
        assert!(!w.needs("updated_at"));
        assert!(w.needs("amount"));
        opt.compare = Some(vec!["amount".into()]);
        let w = Want::for_options(&opt);
        assert!(w.needs("amount") && w.needs("id") && !w.needs("note"));
        assert!(!Want::footer_only().needs("id"));
        assert!(Want::all().needs("anything"));
    }

    #[test]
    fn a_name_is_the_last_segment() {
        let s = |spec: &str| Source {
            spec: spec.into(),
            path: PathBuf::new(),
            stored: 0,
            temps: Vec::new(),
            steps: Vec::new(),
            held: None,
        };
        assert_eq!(s("s3://b/dir/a.csv").name(), "a.csv");
        assert_eq!(
            s("https://h/x/y.parquet?X-Amz-Signature=1").name(),
            "y.parquet"
        );
        assert_eq!(s("data/b.csv").name(), "b.csv");
        assert_eq!(s("-").name(), "stdin");
    }

    #[test]
    fn gzip_and_zstd_inputs_read_as_their_contents() {
        let text = b"id,v\n1,a\n2,b\n".repeat(1000);
        let dir = std::env::temp_dir();
        let gz = dir.join(format!("compara-test-{}-a.csv.gz", std::process::id()));
        let zs = dir.join(format!("compara-test-{}-a.csv.zst", std::process::id()));
        {
            let mut e = flate2::write::GzEncoder::new(
                File::create(&gz).unwrap(),
                flate2::Compression::fast(),
            );
            e.write_all(&text).unwrap();
            e.finish().unwrap();
        }
        std::fs::write(&zs, zstd::stream::encode_all(&text[..], 3).unwrap()).unwrap();
        for p in [&gz, &zs] {
            let s = Source::open(p.to_str().unwrap(), &Want::all()).unwrap();
            assert_ne!(s.path(), p.as_path());
            assert_eq!(std::fs::read(s.path()).unwrap(), text);
            assert_eq!(s.steps().len(), 1, "{:?}", s.steps());
            let expanded = s.path().to_path_buf();
            drop(s);
            assert!(!expanded.exists(), "the expanded copy goes with the source");
            assert!(p.exists(), "the caller's file stays");
            std::fs::remove_file(p).unwrap();
        }
    }

    #[test]
    fn other_compressions_are_named_not_misread() {
        let p = std::env::temp_dir().join(format!("compara-test-{}-a.csv.bz2", std::process::id()));
        std::fs::write(&p, b"BZh91AY&SY").unwrap();
        let e = Source::open(p.to_str().unwrap(), &Want::all())
            .err()
            .unwrap();
        assert!(e.to_string().contains("bzip2"), "{e}");
        std::fs::remove_file(&p).unwrap();
    }
}
