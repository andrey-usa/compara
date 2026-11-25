//! The out-of-core engine: sort both files by key, then walk them together.
//!
//! The other engines hold at least one whole file in memory, so the largest
//! comparison is bounded by the machine. This one is bounded by disk instead.
//! Batches of rows are sorted and spilled, the spilled runs are merged back as
//! one ordered stream, and the join is a single ordered pass down both sides at
//! once — so memory holds a few batches, one row per run, and the capped
//! report, and nothing else grows with the file.
//!
//! The two files are sorted at once, on two threads, and each spills a batch on
//! a thread of its own while the next one is read: two batches per file are in
//! memory at the peak. A row is held as text packed into the batch's one buffer
//! ([`Packed`]), not as a `String` per cell, which is where this engine used to
//! spend most of its time.
//!
//! That is the classical answer to comparing data larger than memory, and it is
//! what reconciliation systems have done since the files lived on tape. It is
//! not the fastest engine here and is not meant to be: sorting is `O(n log n)`
//! where a hash join is linear, and the spill writes the data twice more. What
//! it buys is that the answer does not depend on how much RAM you have.
//!
//! Duplicate keys are nearly free, because sorting puts the repeats next to each
//! other, and the sections come out in key order for the same reason, so nothing
//! is sorted a second time at the end.

use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::sync::{Arc, mpsc};

use crate::columns::{compare_keys, differs, resolve};
use crate::contract::{Cell, CellDiff, ColumnStat, Counts, EngineResult, Section, Val};
use crate::error::{Error, Result};
use crate::options::Options;
use crate::phases::Phases;
use crate::rowstore::{Joined, Packed};
use crate::sections::{ExportDir, assemble, export_headers, finish_export};
use crate::source::WorkDir;

/// How much memory a batch may occupy before it is spilled: its packed text,
/// four bytes a cell and the sort's four bytes a row.
const DEFAULT_BATCH_BYTES: usize = 32 << 20;

/// Overrides the batch budget so the spill and merge path can be exercised on a
/// file small enough to assert about. A comparison below the default never
/// spills at all, which would leave the interesting half of this module untested.
pub const BATCH_BYTES_ENV: &str = "COMPARA_SORTMERGE_BATCH_BYTES";

/// A spilled run's read buffer when the merge has the room: the size every
/// run was read through before the budget sized them.
const RUN_BUFFER_MAX: usize = 64 << 10;

/// The least a spilled run is read through, a page. The merge reads at most
/// as many runs at once as its memory holds buffers of this size; more are
/// merged a group at a time first.
const RUN_BUFFER_MIN: usize = 4 << 10;

/// The batch size: the override, or the default -- unless the process has a
/// heap limit (`ulimit -d`, or a container's, see `memory::contain`) or a stated budget
/// (`--memory`), when it is a sixteenth of that, so a comparison handed to
/// this engine for want of memory fits what it was handed with. Both files
/// are sorted at once, each with a batch being filled and one being written,
/// and a batch can briefly hold twice its size while it grows. Of the limit,
/// what an engine before this one left `held` is not this one's.
fn batch_bytes(opt: &Options, held: u64) -> usize {
    if let Some(n) = std::env::var(BATCH_BYTES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
    {
        return n;
    }
    let allowance = [
        crate::memory::heap_limit().map(|h| h.saturating_sub(held)),
        crate::memory::stated(opt.memory),
    ]
    .into_iter()
    .flatten()
    .min();
    match allowance {
        Some(bytes) => ((bytes / 16) as usize).clamp(1 << 20, DEFAULT_BATCH_BYTES),
        None => DEFAULT_BATCH_BYTES,
    }
}

pub fn compare(a_path: &Path, b_path: &Path, opt: &Options) -> Result<EngineResult> {
    compare_after(a_path, b_path, opt, 0)
}

/// [`compare`], in a process where the engine that ran before it left `held`
/// bytes of the heap limit mapped: `auto` hands a pair `turbo` refused to this
/// engine, and what `turbo` mapped -- its threads' stacks, which the C library
/// keeps for the next thread, and their heaps -- stays counted against the
/// limit. Sized from the whole limit, the batches and the merge's buffers did
/// not fit beside it (the 2M-row pair at 16 threads, 31 and 32 MB: 2 of 6
/// runs refused where a named sortmerge answered).
pub(crate) fn compare_after(
    a_path: &Path,
    b_path: &Path,
    opt: &Options,
    held: u64,
) -> Result<EngineResult> {
    let budget = batch_bytes(opt, held);
    let a_header = read_header(a_path, opt)?;
    let b_header = read_header(b_path, opt)?;
    let resolved = resolve(&a_header, &b_header, opt)?;

    let key_size = opt.key.len();
    let width = key_size + resolved.compared.len();
    // The runs hold every row of both files: the directory is this user's
    // alone, and goes with what is in it however the run ends.
    let guard = crate::source::TempDir::new("sortmerge")?;
    let work = guard.dir();

    let mut phases = Phases::new("");
    let sort = |side: &str, path: &Path, header: &[String]| -> Result<Runs> {
        stream_into(work, side, path, header, &resolved.compared, opt, budget)
    };
    let sort_b = || sort("b", b_path, &b_header);
    let (a_runs, b_runs) = if opt.thread_budget() > 1 {
        std::thread::scope(|scope| {
            // Refused a thread, B is sorted here, after A.
            let b = crate::parallel::spawn(scope, &sort_b);
            let a = sort("a", a_path, &a_header);
            let b = b
                .join()
                .unwrap_or_else(|_| Err(Error::new("sorting the B file panicked")));
            (a, b)
        })
    } else {
        (sort("a", a_path, &a_header), sort_b())
    };
    let (a_runs, b_runs) = (a_runs?, b_runs?);
    let (a_rows, b_rows) = (a_runs.rows, b_runs.rows);
    phases.mark("read and sort (2 ways)");

    let (ca, cb) = (a_runs.sorted()?, b_runs.sorted()?);

    let mut dup_a = Dups::new(&opt.key, opt.max_rows);
    let mut dup_b = Dups::new(&opt.key, opt.max_rows);
    let mut join = |mut ca: Box<dyn Cursor>, mut cb: Box<dyn Cursor>| {
        merge_join(
            &mut ca,
            &mut cb,
            opt,
            &resolved.compared,
            a_rows,
            b_rows,
            &mut dup_a,
            &mut dup_b,
            opt.export_dir.is_some(),
        )
    };
    let joined = if opt.thread_budget() > 2 {
        std::thread::scope(|scope| {
            let ca = Ahead::start(scope, ca, width)?;
            let cb = Ahead::start(scope, cb, width)?;
            join(ca, cb)
        })?
    } else {
        join(ca, cb)?
    };
    phases.mark("merge join");

    let meta = resolved.meta(&opt.key, a_header.len(), b_header.len());
    let out = assemble(
        meta,
        joined,
        dup_a.section(),
        dup_b.section(),
        opt,
        &resolved.compared,
    );
    drop(guard);
    out
}

// ---------------------------------------------------------------------------
// Reading
// ---------------------------------------------------------------------------

fn reader(path: &Path, opt: &Options) -> Result<csv::Reader<File>> {
    use std::io::{Seek, SeekFrom};

    let mut file = crate::source::open_input(path)
        .map_err(|e| Error::new(format!("cannot read {}: {e}", path.display())))?;
    let delimiter = match opt.delimiter_byte()? {
        Some(d) => d,
        None => {
            // As bytes: a header that is not UTF-8 is for the reader below
            // to refuse by name, not for this line to fail on.
            let mut line = Vec::new();
            BufReader::new(&mut file)
                .read_until(b'\n', &mut line)
                .map_err(|e| Error::new(format!("cannot read {}: {e}", path.display())))?;
            file.seek(SeekFrom::Start(0))?;
            crate::columns::detect_delimiter(&String::from_utf8_lossy(&line))
        }
    };
    Ok(csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .flexible(true)
        .from_reader(file))
}

fn read_header(path: &Path, opt: &Options) -> Result<Vec<String>> {
    let mut head = Vec::with_capacity(1024);
    crate::source::open_input(path)
        .and_then(|f| f.take(1024).read_to_end(&mut head))
        .map_err(|e| Error::new(format!("cannot read {}: {e}", path.display())))?;
    crate::engine::turbo::refuse_non_text(&head, path)?;
    let mut rdr = reader(path, opt)?;
    let headers = rdr.headers().map_err(|e| match e.kind() {
        csv::ErrorKind::Utf8 { .. } => crate::engine::native::read_error(path, e, "sortmerge"),
        _ => Error::new(format!("file has no header row: {}", path.display())),
    })?;
    if headers.is_empty() {
        return Err(Error::new(format!(
            "file has no header row: {}",
            path.display()
        )));
    }
    Ok(headers.iter().map(str::to_string).collect())
}

/// Streams a file into the sorter, projecting and normalising a row at a time.
fn stream_into(
    work: &WorkDir,
    side: &str,
    path: &Path,
    header: &[String],
    compared: &[String],
    opt: &Options,
    budget: usize,
) -> Result<Runs> {
    let width = opt.key.len() + compared.len();
    let dir = Arc::new(work.make_dir(side)?);
    let index: Vec<Option<usize>> = opt
        .key
        .iter()
        .chain(compared)
        .map(|name| header.iter().position(|c| c == name))
        .collect();
    let key_size = opt.key.len();
    let halves = room_halves(&index, opt.ignore_case);

    // Sorts a full batch and writes it as the run numbered `n`. The order and
    // the write buffer are asked for with a refusal: under a heap limit they
    // were what a sort aborted on (`ulimit -d` 8-17 MB, the 200k- and 2M-row
    // pairs).
    let spill = |batch: &Packed, n: usize, rowbuf: &mut Vec<u8>| -> Result<String> {
        let name = format!("run-{n}.bin");
        let order = sorted_order(batch, key_size)?;
        let mut w = RunWriter::create(dir.create(&name)?, RUN_BUFFER_MAX, SPILL_BUFFER)?;
        for row in order {
            write_row(&mut w, batch, row as usize, rowbuf)?;
        }
        let file = w.finish()?;
        crate::memory::write_out(&file);
        Ok(name)
    };

    // Full batches go to a thread that sorts and writes them while this one
    // reads on. The channel holds one, so at most two are in memory: the one
    // being written and the one being filled. A batch written comes back to
    // be filled again, so after the first two the reading allocates nothing.
    let (to_spill, batches) = mpsc::sync_channel::<Packed>(0);
    let (written, back) = mpsc::channel::<Packed>();
    std::thread::scope(|scope| {
        let spill = &spill;
        let spiller = crate::parallel::spawn_scoped(scope, move || -> Result<Vec<String>> {
            let mut spilled = Vec::new();
            let mut rowbuf: Vec<u8> = Vec::new();
            for batch in batches {
                let name = spill(&batch, spilled.len(), &mut rowbuf)?;
                crate::alloc::push(&mut spilled, name, RUN_NAMES)?;
                let _ = written.send(batch);
            }
            Ok(spilled)
        });
        // A thread the OS refuses (a process limit, or a data limit its stack
        // counts against) leaves the batches to be written here, between reads.
        let mut here: Option<(Vec<String>, Vec<u8>)> =
            spiller.is_err().then(|| (Vec::new(), Vec::new()));

        let mut read = || -> Result<(Packed, i64)> {
            let mut batch = Packed::new(width);
            let mut rows = 0i64;
            let mut sent = false;
            // Batches at the spiller, not back yet.
            let mut out = 0usize;
            // Hands the batch over to be sorted and written, and starts the
            // next in one that came back, if one has; `false` if the spiller
            // has failed (it says why when it is joined).
            let mut hand_over = |batch: &mut Packed, out: &mut usize| -> Result<bool> {
                let next = match back.try_recv() {
                    Ok(b) => {
                        *out -= 1;
                        b
                    }
                    Err(_) => Packed::new(width),
                };
                let full = std::mem::replace(batch, next);
                batch.clear();
                if let Some((spilled, rowbuf)) = here.as_mut() {
                    let name = spill(&full, spilled.len(), rowbuf)?;
                    crate::alloc::push(spilled, name, RUN_NAMES)?;
                    *batch = full;
                    batch.clear();
                } else if to_spill.send(full).is_err() {
                    return Ok(false);
                } else {
                    *out += 1;
                }
                Ok(true)
            };
            let mut rdr = reader(path, opt)?;
            let mut record = csv::StringRecord::new();
            while rdr
                .read_record(&mut record)
                .map_err(|e| crate::engine::native::read_error(path, e, "sortmerge"))?
            {
                rows += 1;
                // A batch that cannot grow to take the row -- under a heap
                // limit, `ulimit -d` or a container's -- is spilled as it is,
                // early, rather than aborting the process. An empty one waits
                // for the batch being written to come back, and only a row no
                // batch has room for, with both here, is refused.
                let need = record.as_slice().len() * halves / 2;
                while !batch.try_room(need) {
                    if !batch.is_empty() {
                        sent = true;
                        if !hand_over(&mut batch, &mut out)? {
                            return Ok((batch, rows)); // the spiller says why
                        }
                    } else if out > 0 {
                        match back.recv() {
                            Ok(b) => {
                                out -= 1;
                                batch = b;
                                batch.clear();
                            }
                            Err(_) => return Ok((batch, rows)),
                        }
                    } else {
                        return Err(no_room_to_sort(need, width));
                    }
                }
                batch
                    .push_normalised(index.iter().map(|at| at.and_then(|i| record.get(i))), opt)?;
                if batch.bytes() + batch.len() * 4 >= budget {
                    sent = true;
                    if !hand_over(&mut batch, &mut out)? {
                        break; // the spiller failed, and says why below
                    }
                }
            }
            // Once anything has spilled, the rest is one more run; a file that
            // never filled a batch stays in memory and is sorted here.
            if sent && !batch.is_empty() {
                let rest = std::mem::replace(&mut batch, Packed::new(width));
                if let Some((spilled, rowbuf)) = here.as_mut() {
                    let name = spill(&rest, spilled.len(), rowbuf)?;
                    crate::alloc::push(spilled, name, RUN_NAMES)?;
                } else {
                    let _ = to_spill.send(rest);
                }
            }
            Ok((batch, rows))
        };
        let got = read();
        drop(to_spill);
        let spilled = match spiller {
            Ok(spiller) => spiller
                .join()
                .unwrap_or_else(|_| Err(Error::new("spilling a sorted run panicked")))?,
            Err(_) => here.map(|(spilled, _)| spilled).unwrap_or_default(),
        };
        // The batches the spiller gave back are not filled again, and the
        // channel holding them lives as long as this call: they go now, not
        // after the passes below, which would otherwise find two batches'
        // room taken (the 2M-row pair was refused a run's buffer at 13-20 MB
        // of heap with them held).
        while back.try_recv().is_ok() {}
        let (last, rows) = got?;
        // Here rather than when the merge starts, so the two files' passes,
        // if they need any, run at once as their sorts did.
        let merge_budget = budget;
        let spilled = merge_down(&dir, spilled, width, key_size, merge_budget)?;
        let order = if spilled.is_empty() {
            sorted_order(&last, key_size)?
        } else {
            Vec::new()
        };
        Ok(Runs {
            width,
            key_size,
            dir: dir.clone(),
            spilled,
            last,
            order,
            rows,
            merge_budget,
        })
    })
}

/// How long a row can be against its record, in halves: the room a row asks
/// a batch for, so that it is never written past. A column read twice
/// (`-k id,id`) is held twice, and lower case can be half as long again
/// (U+023A is two bytes, its lower case three; see
/// `lower_case_grows_by_half_at_most`).
fn room_halves(index: &[Option<usize>], ignore_case: bool) -> usize {
    let mut read: Vec<usize> = index.iter().flatten().copied().collect();
    read.sort_unstable();
    let repeats = read.chunk_by(|x, y| x == y).map(<[usize]>::len).max();
    repeats.unwrap_or(1) * if ignore_case { 3 } else { 2 }
}

// ---------------------------------------------------------------------------
// External sort
// ---------------------------------------------------------------------------

/// A batch's rows in key order. Equal keys keep file order: batches are
/// filled in file order and the merge breaks equal keys by run number, so the
/// first row carrying a key is still the first row the join sees -- which is
/// what makes first-occurrence-wins mean the same thing here as in every other
/// engine.
///
/// Equal keys are ordered by row number, so the sort is the stable one
/// without being a stable sort: the standard library's takes a scratch copy
/// of what it sorts from the allocator, with no way to refuse it, and under a
/// heap limit that copy was the allocation the process aborted on ("memory
/// allocation of 79040 bytes failed", the 200k- and 2M-row pairs at 8-17 MB).
/// What is held here is asked for with a refusal instead.
fn sorted_order(batch: &Packed, key_size: usize) -> Result<Vec<u32>> {
    let n = batch.len();
    let mut order: Vec<u32> = crate::alloc::sized(n, SORT_ORDER)?;
    if key_size == 0 || n < 2 {
        order.extend(0..n as u32);
        order.sort_unstable_by(|&x, &y| {
            batch
                .key_order(x as usize, batch, y as usize, key_size)
                .then(x.cmp(&y))
        });
        return Ok(order);
    }
    // The first key cell's bytes after what every row in the batch shares
    // (ids that all begin `ACC-0000`), eight of them, as a big-endian number,
    // above the row's number: the rows are sorted as plain integers, which
    // reads nothing from the batch's text, where a sort on 144 thousand rows
    // spent its time (a cache miss for each cell of each comparison). Rows
    // whose eight bytes tie, or whose cell is absent, come out in file order,
    // and are then put in the order `key_order` gives, equal keys by row.
    let mut shared = usize::MAX;
    let first = (0..n).find_map(|r| batch.cell_bytes(r, 0));
    if let Some(first) = first {
        for r in 0..n {
            if let Some(c) = batch.cell_bytes(r, 0) {
                let l = first.iter().zip(c).take_while(|(x, y)| x == y).count();
                shared = shared.min(l);
                if shared == 0 {
                    break;
                }
            }
        }
    }
    let shared = if shared == usize::MAX { 0 } else { shared };
    let mut rows: Vec<u128> = crate::alloc::sized(n, SORT_ORDER)?;
    rows.extend((0..n as u32).map(|r| {
        let prefix = match batch.cell_bytes(r as usize, 0) {
            None => u64::MAX,
            Some(c) => {
                let mut word = [0u8; 8];
                let rest = c.get(shared..).unwrap_or(&[]);
                let k = rest.len().min(8);
                word[..k].copy_from_slice(&rest[..k]);
                u64::from_be_bytes(word)
            }
        };
        u128::from(prefix) << 32 | u128::from(r)
    }));
    rows.sort_unstable();
    let row = |x: u128| x as u32 as usize;
    let mut at = 0;
    while at < n {
        let prefix = rows[at] >> 32;
        let tied = rows[at + 1..]
            .iter()
            .take_while(|&&x| x >> 32 == prefix)
            .count();
        if tied > 0 {
            rows[at..=at + tied].sort_unstable_by(|&x, &y| {
                batch
                    .key_order(row(x), batch, row(y), key_size)
                    .then(x.cmp(&y))
            });
        }
        at += tied + 1;
    }
    order.extend(rows.iter().map(|&x| row(x) as u32));
    Ok(order)
}

/// The list of a file's spilled runs, in a refusal: a batch that cannot grow
/// is spilled early, so under a tight heap a file spills thousands of runs,
/// and the list growing by doubling was another allocation the sort aborted
/// on ("memory allocation of 98304 bytes failed", the 2M-row pair at 8 MB).
const RUN_NAMES: &str = "the list of sorted runs";

/// What [`sorted_order`] holds, in a refusal.
const SORT_ORDER: &str = "the sort order of a batch of rows";

/// One file, sorted: the runs spilled to disk, or the one batch it took and
/// that batch's order.
struct Runs {
    width: usize,
    key_size: usize,
    /// The directory the runs are in, held open.
    dir: Arc<WorkDir>,
    spilled: Vec<String>,
    last: Packed,
    order: Vec<u32>,
    rows: i64,
    /// What the merge may spend on its read buffers: a batch's worth. The
    /// sort holds none once it is done, but the other file may still be
    /// sorting, with two of its own, while this one merges in passes.
    merge_budget: usize,
}

impl Runs {
    /// Finishes the sort and returns the rows in key order. A sort that never
    /// spilled is returned straight from memory, so the common case of a file
    /// that fits pays nothing for the machinery that handles one that does not.
    fn sorted(self) -> Result<Box<dyn Cursor + Send>> {
        if self.spilled.is_empty() {
            return Ok(Box::new(BatchCursor {
                rows: self.last,
                order: self.order,
                at: 0,
            }));
        }
        Ok(Box::new(MergeCursor::open(
            &self.dir,
            &self.spilled,
            self.width,
            self.key_size,
            run_buffer(self.merge_budget, self.spilled.len()),
        )?))
    }
}

/// What a run costs the merge besides its buffer: its reader, its head row
/// and that row's text, and its place in the heap, with room to spare.
const RUN_OVERHEAD: usize = 512;

/// How many runs one merge reads at once: as many as its budget holds
/// buffers of [`RUN_BUFFER_MIN`] for (one more is written, in a pass), and as
/// many as can be open while the other file merges too.
fn fan_in(budget: usize) -> usize {
    let by_memory = (budget / (RUN_BUFFER_MIN + RUN_OVERHEAD)).saturating_sub(1);
    // `ulimit -n`, less some for the inputs, the report and the export; where
    // it cannot be asked, macOS's default of 256.
    let files = crate::memory::open_files_limit().unwrap_or(256);
    let by_files = usize::try_from(files.saturating_sub(64) / 2)
        .unwrap_or(usize::MAX)
        .saturating_sub(1);
    by_memory.min(by_files).max(2)
}

/// Each run's read buffer when `files` share `budget`: whole pages, from
/// [`RUN_BUFFER_MIN`] to [`RUN_BUFFER_MAX`].
fn run_buffer(budget: usize, files: usize) -> usize {
    (budget / files.max(1)).clamp(RUN_BUFFER_MIN, RUN_BUFFER_MAX) & !(RUN_BUFFER_MIN - 1)
}

/// Merges runs into one, in order, until no more are left than [`fan_in`]
/// lets the merge read at once.
///
/// A file spills about sixteen runs per heap it is larger than, so under a
/// heap limit the count grows with the file, not with the limit, and a buffer
/// per run outgrew the limit it was started under: the 2M-row pair at 30 MB
/// aborted opening its runs. Only the first runs are merged, and only as many
/// as bring the count down, so a file a little over the line rewrites a
/// little of itself, not all of it. The groups are consecutive and the merge
/// breaks equal keys by run, so a key's rows keep their order in the file.
fn merge_down(
    dir: &WorkDir,
    mut runs: Vec<String>,
    width: usize,
    key_size: usize,
    budget: usize,
) -> Result<Vec<String>> {
    let fan_in = fan_in(budget);
    let mut pass = 0;
    while runs.len() > fan_in {
        let excess = runs.len() - fan_in;
        let mut merged = Vec::new();
        let (mut at, mut gone) = (0, 0);
        while gone < excess {
            // Merging k runs into one takes away k - 1.
            let k = (excess - gone + 1).min(fan_in).min(runs.len() - at);
            if k < 2 {
                break;
            }
            let name = format!("merge-{pass}-{}.bin", merged.len());
            merge_runs(dir, &runs[at..at + k], &name, width, key_size, budget)?;
            for done in &runs[at..at + k] {
                let _ = dir.remove(done);
            }
            merged.push(name);
            at += k;
            gone += k - 1;
        }
        merged.extend_from_slice(&runs[at..]);
        runs = merged;
        pass += 1;
    }
    Ok(runs)
}

/// Merges `runs` into one run, `name`, in `dir`.
fn merge_runs(
    dir: &WorkDir,
    runs: &[String],
    name: &str,
    width: usize,
    key_size: usize,
    budget: usize,
) -> Result<()> {
    let buffer = run_buffer(budget, runs.len() + 1);
    let mut from = MergeCursor::open(dir, runs, width, key_size, buffer)?;
    let mut to = RunWriter::create(dir.create(name)?, buffer, MERGE_BUFFER)?;
    let mut rowbuf = Vec::new();
    while let Some((rows, row)) = from.peek() {
        write_row(&mut to, rows, row, &mut rowbuf)?;
        from.next()?;
    }
    let file = to.finish()?;
    crate::memory::write_out(&file);
    Ok(())
}

/// The refusal of a run's buffer: the merge reads its runs through buffers
/// sized to fit, so only a heap that is full already gets here.
fn no_room_to_merge(bytes: usize) -> Error {
    no_room_for(MERGE_BUFFER, bytes)
}

/// What a run's read buffer is, in a refusal.
const MERGE_BUFFER: &str = "a sorted run's buffer for the merge";

/// What a sorted batch is written through, in a refusal.
const SPILL_BUFFER: &str = "the buffer a sorted run is written through";

/// The refusal of a buffer, a row or a table the sort or the merge needs:
/// they are sized to fit, so only a heap that is full already gets here.
fn no_room_for(what: &str, bytes: usize) -> Error {
    refused(what, bytes)
}

/// [`crate::alloc::refuse`], with what to do. Under a heap limit `refuse` has
/// said it, in room set aside for the refusal: the heap is full to the byte
/// here, and writing the same words again, into a string of its own, was the
/// allocation the refusal aborted on ("memory allocation of 64 bytes failed",
/// the 2M-row pair at 6 and 9.5 MB). With no limit set, it is the machine
/// that has no more to give, and that is said here.
fn refused(what: &str, bytes: usize) -> Error {
    let refusal = crate::alloc::refuse(what, bytes);
    if crate::memory::heap_limit().is_some() {
        return refusal;
    }
    refusal.without_hint().context_after(what_to_do(""))
}

/// A buffer for a run, from the allocator, or the refusal naming `what`.
fn run_buffer_of(size: usize, what: &str) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    buf.try_reserve_exact(size)
        .map_err(|_| no_room_for(what, size))?;
    Ok(buf)
}

/// A spilled run read through a buffer of the size the merge chose, taken
/// from the allocator with a refusal rather than the abort a `BufReader`'s
/// would end in.
struct RunReader {
    file: File,
    buf: Box<[u8]>,
    at: usize,
    end: usize,
}

impl RunReader {
    fn open(file: File, size: usize) -> Result<Self> {
        let mut buf = run_buffer_of(size, MERGE_BUFFER)?;
        buf.resize(size, 0);
        Ok(RunReader {
            file,
            buf: buf.into_boxed_slice(),
            at: 0,
            end: 0,
        })
    }
}

impl Read for RunReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if self.at == self.end && out.len() >= self.buf.len() {
            return self.file.read(out);
        }
        let have = self.fill_buf()?;
        let n = have.len().min(out.len());
        out[..n].copy_from_slice(&have[..n]);
        self.consume(n);
        Ok(n)
    }

    /// What the buffer holds already is copied without a call per piece:
    /// a row is read as two of these.
    #[inline]
    fn read_exact(&mut self, mut out: &mut [u8]) -> std::io::Result<()> {
        if self.end - self.at >= out.len() {
            out.copy_from_slice(&self.buf[self.at..self.at + out.len()]);
            self.at += out.len();
            return Ok(());
        }
        while !out.is_empty() {
            match self.read(out) {
                Ok(0) => return Err(std::io::ErrorKind::UnexpectedEof.into()),
                Ok(n) => out = &mut out[n..],
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }
}

impl BufRead for RunReader {
    #[inline]
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        if self.at >= self.end {
            self.end = loop {
                match self.file.read(&mut self.buf) {
                    Ok(n) => break n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(e) => return Err(e),
                }
            };
            self.at = 0;
        }
        Ok(&self.buf[self.at..self.end])
    }

    #[inline]
    fn consume(&mut self, n: usize) {
        self.at = (self.at + n).min(self.end);
    }
}

/// A run written by the sort or a merge pass, through a buffer taken as
/// [`RunReader`]'s is.
struct RunWriter {
    file: File,
    buf: Vec<u8>,
}

impl RunWriter {
    fn create(file: File, size: usize, what: &str) -> Result<Self> {
        let buf = run_buffer_of(size, what)?;
        Ok(RunWriter { file, buf })
    }

    fn finish(mut self) -> Result<File> {
        self.flush()?;
        Ok(self.file)
    }
}

impl Write for RunWriter {
    #[inline]
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if self.buf.len() + data.len() > self.buf.capacity() {
            self.flush()?;
        }
        if data.len() >= self.buf.capacity() {
            return self.file.write(data);
        }
        self.buf.extend_from_slice(data);
        Ok(data.len())
    }

    /// What fits is copied without the loop the default runs, as
    /// `BufWriter`'s does: a row is written as two of these.
    #[inline]
    fn write_all(&mut self, data: &[u8]) -> std::io::Result<()> {
        if self.buf.capacity() - self.buf.len() >= data.len() {
            self.buf.extend_from_slice(data);
            return Ok(());
        }
        self.write_all_past(data)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file.write_all(&self.buf)?;
        self.buf.clear();
        Ok(())
    }
}

impl RunWriter {
    /// [`Write::write_all`] for what does not fit: the buffer is written
    /// out first, and what is as large as the buffer goes straight through.
    #[cold]
    #[inline(never)]
    fn write_all_past(&mut self, data: &[u8]) -> std::io::Result<()> {
        self.flush()?;
        if data.len() >= self.buf.capacity() {
            return self.file.write_all(data);
        }
        self.buf.extend_from_slice(data);
        Ok(())
    }
}

/// One ordered pass over sorted rows.
trait Cursor {
    /// The row at the cursor, as a row of a packed store, or `None` once the
    /// rows are exhausted.
    fn peek(&mut self) -> Option<(&Packed, usize)>;
    /// Advances past the current row.
    fn next(&mut self) -> Result<()>;
}

/// How many rows a cursor running ahead hands over at a time.
const AHEAD_ROWS: usize = 4096;

/// A cursor run on a thread of its own, a block of rows ahead of whoever reads
/// it, so that both files' merges and the join run at once rather than in turn.
///
/// A block read to its end goes back to the thread to be filled again, so
/// once the first few have grown to their size the read-ahead allocates
/// nothing. Under a heap limit (`ulimit -d`, a container's) a block that
/// cannot grow is handed over as it is, and a row with no room at all waits
/// for the reader to give a block back: the merge slows down rather than
/// aborting, and only a row that fits no block -- with every block back --
/// is refused, with what to do.
struct Ahead {
    blocks: mpsc::Receiver<Result<Packed>>,
    /// Where a block read to its end goes back to be filled again.
    spent: mpsc::Sender<Packed>,
    block: Packed,
    at: usize,
}

impl Ahead {
    /// `inner` run ahead on a thread, or, where the OS refuses one (a process
    /// limit, or a data limit its stack counts against), `inner` itself, read
    /// in turn with the other file's.
    fn start<'s>(
        scope: &'s std::thread::Scope<'s, '_>,
        inner: Box<dyn Cursor + Send>,
        width: usize,
    ) -> Result<Box<dyn Cursor>> {
        let (to_join, blocks) = mpsc::sync_channel(2);
        let (spent, returned) = mpsc::channel::<Packed>();
        // Handed over through a slot rather than moved: a refused spawn drops
        // its closure, and the cursor must outlive that.
        let slot = std::sync::Arc::new(std::sync::Mutex::new(Some(inner)));
        let theirs = std::sync::Arc::clone(&slot);
        let spawned = crate::parallel::spawn_scoped(scope, move || {
            let taken = theirs.lock().unwrap_or_else(|e| e.into_inner()).take();
            let Some(mut inner) = taken else {
                return;
            };
            // Blocks handed over and not given back yet: the reader holds one
            // and the channel up to two.
            let mut out = 0usize;
            loop {
                let mut block = match returned.try_recv() {
                    Ok(b) => {
                        out -= 1;
                        b
                    }
                    Err(_) => Packed::new(width),
                };
                block.clear();
                while block.len() < AHEAD_ROWS {
                    let Some((rows, row)) = inner.peek() else {
                        break;
                    };
                    if !block.try_push_row_of(rows, row) {
                        if !block.is_empty() {
                            break; // handed over as it is; the next starts empty
                        }
                        if out > 0 {
                            // The room a block the reader is done with holds
                            // is this row's: wait for one, and try again in it.
                            match returned.recv() {
                                Ok(b) => {
                                    out -= 1;
                                    block = b;
                                    block.clear();
                                    continue;
                                }
                                Err(_) => return,
                            }
                        }
                        // Every block is back and the row still has no room:
                        // the last try, which names what it needed.
                        if let Err(e) = block.push_row_of(rows, row) {
                            let _ = to_join.send(Err(no_room(e)));
                            return;
                        }
                    }
                    if let Err(e) = inner.next() {
                        let _ = to_join.send(Err(e));
                        return;
                    }
                }
                if block.is_empty() || to_join.send(Ok(block)).is_err() {
                    return;
                }
                out += 1;
            }
        });
        if spawned.is_err() {
            let inner = slot.lock().unwrap_or_else(|e| e.into_inner()).take();
            return inner
                .map(|c| c as Box<dyn Cursor>)
                .ok_or_else(|| Error::new("a merge cursor was taken twice"));
        }
        let mut ahead = Ahead {
            blocks,
            spent,
            block: Packed::new(width),
            at: 0,
        };
        ahead.refill()?;
        Ok(Box::new(ahead))
    }

    /// The next block, or an empty one once the rows are done. An error on the
    /// other thread arrives here rather than looking like the end of the file.
    /// The block read to its end goes back first, so a read-ahead waiting for
    /// room has it before this waits on the read-ahead.
    fn refill(&mut self) -> Result<()> {
        self.at = 0;
        let done = std::mem::take(&mut self.block);
        if !done.is_empty() {
            let _ = self.spent.send(done);
        }
        if let Ok(block) = self.blocks.recv() {
            self.block = block?;
        }
        Ok(())
    }
}

/// The refusal of a row the sort has no room for, with no batch left to
/// spill or wait for.
fn no_room_to_sort(text: usize, width: usize) -> Error {
    refused("a batch of rows to sort", text + width * 4 + 8)
}

/// The refusal of a row the read-ahead has no room for with every block back.
fn no_room(e: Error) -> Error {
    e.without_hint().context_after(format!(
        " to read the sorted runs ahead of the merge{}",
        what_to_do(", or `--threads 2` merges without reading ahead")
    ))
}

/// What a refusal of room says after what ran out: the heap limit, if there
/// is one, and what to do.
fn what_to_do(or: &str) -> String {
    match crate::memory::heap_limit() {
        Some(h) => format!(
            ", and this process may use {} (a container limit or `ulimit -d`): raise it{or}",
            crate::source::human(h)
        ),
        None => format!(": the machine has no more to give; free some{or}"),
    }
}

impl Cursor for Ahead {
    fn peek(&mut self) -> Option<(&Packed, usize)> {
        (self.at < self.block.len()).then_some((&self.block, self.at))
    }

    fn next(&mut self) -> Result<()> {
        self.at += 1;
        if self.at >= self.block.len() && !self.block.is_empty() {
            self.refill()?;
        }
        Ok(())
    }
}

struct BatchCursor {
    rows: Packed,
    order: Vec<u32>,
    at: usize,
}

impl Cursor for BatchCursor {
    fn peek(&mut self) -> Option<(&Packed, usize)> {
        let row = *self.order.get(self.at)?;
        Some((&self.rows, row as usize))
    }

    fn next(&mut self) -> Result<()> {
        self.at += 1;
        Ok(())
    }
}

/// A k-way merge over the spilled runs, holding one row from each.
struct MergeCursor {
    readers: Vec<RunReader>,
    /// Each run's current row, or an empty store once the run is done.
    heads: Vec<Packed>,
    /// The runs that still have rows, as a binary heap ordered by their heads:
    /// the smallest at the top. A file of ten million rows spills about eighty
    /// runs, and a scan of every head per row was eighty key comparisons a row
    /// where the heap takes about a dozen.
    heap: Vec<usize>,
    key_size: usize,
    scratch: Vec<u8>,
    ends: Vec<u32>,
}

impl MergeCursor {
    /// The runs `names` in `dir`, each read through `buffer` bytes.
    fn open(
        dir: &WorkDir,
        names: &[String],
        width: usize,
        key_size: usize,
        buffer: usize,
    ) -> Result<Self> {
        let mut c = Self {
            readers: Vec::new(),
            heads: Vec::new(),
            heap: Vec::new(),
            key_size,
            scratch: Vec::new(),
            ends: Vec::new(),
        };
        // Taken whole and checked: a thousand runs' heads are 80 KB, and
        // grown by doubling they were the allocation a heap near its limit
        // aborted on.
        let n = names.len();
        let each = std::mem::size_of::<RunReader>() + std::mem::size_of::<Packed>() + 8;
        if c.readers.try_reserve_exact(n).is_err()
            || c.heads.try_reserve_exact(n).is_err()
            || c.heap.try_reserve_exact(n).is_err()
        {
            return Err(no_room_to_merge(n * each));
        }
        for name in names {
            c.readers.push(RunReader::open(dir.open(name)?, buffer)?);
            c.heads.push(Packed::new(width));
        }
        for run in 0..c.readers.len() {
            c.pull(run)?;
            if !c.heads[run].is_empty() {
                c.heap.push(run);
            }
        }
        for at in (0..c.heap.len() / 2).rev() {
            c.sift_down(at);
        }
        Ok(c)
    }

    fn pull(&mut self, run: usize) -> Result<()> {
        self.heads[run].clear();
        read_row(
            &mut self.readers[run],
            &mut self.heads[run],
            &mut self.scratch,
            &mut self.ends,
        )
    }

    /// Whether run `x`'s head comes before run `y`'s: by key, and between equal
    /// keys by run number, so that file order survives the merge.
    fn before(&self, x: usize, y: usize) -> bool {
        match self.heads[x].key_order(0, &self.heads[y], 0, self.key_size) {
            Ordering::Less => true,
            Ordering::Equal => x < y,
            Ordering::Greater => false,
        }
    }

    fn sift_down(&mut self, mut at: usize) {
        loop {
            let (l, r) = (2 * at + 1, 2 * at + 2);
            let mut least = at;
            if l < self.heap.len() && self.before(self.heap[l], self.heap[least]) {
                least = l;
            }
            if r < self.heap.len() && self.before(self.heap[r], self.heap[least]) {
                least = r;
            }
            if least == at {
                return;
            }
            self.heap.swap(at, least);
            at = least;
        }
    }
}

impl Cursor for MergeCursor {
    fn peek(&mut self) -> Option<(&Packed, usize)> {
        Some((&self.heads[*self.heap.first()?], 0))
    }

    fn next(&mut self) -> Result<()> {
        let Some(&run) = self.heap.first() else {
            return Ok(());
        };
        self.pull(run)?;
        if self.heads[run].is_empty() {
            let last = self.heap.pop().expect("the heap holds this run");
            if self.heap.is_empty() {
                return Ok(());
            }
            self.heap[0] = last;
        }
        self.sift_down(0);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// The spill format: per row, a little-endian u32 with the length of what
// follows, a varint per cell (zero for an absent cell, otherwise its length
// plus one), then the row's text. A row comes back in two reads, and the
// lengths are decoded from memory, not from the file a cell at a time.
// ---------------------------------------------------------------------------

fn write_row<W: Write>(w: &mut W, rows: &Packed, row: usize, buf: &mut Vec<u8>) -> Result<()> {
    let (ends, text) = rows.row_raw(row);
    buf.clear();
    buf.extend_from_slice(&[0; 4]);
    let mut prev = 0u32;
    for &e in ends {
        if e & Packed::ABSENT_FLAG != 0 {
            buf.push(0);
        } else {
            push_varint(buf, u64::from(e - prev) + 1);
        }
        prev = e & !Packed::ABSENT_FLAG;
    }
    let len = buf.len() - 4 + text.len();
    let len = u32::try_from(len).map_err(|_| Error::new("a row holds more than 4 GB of text"))?;
    buf[..4].copy_from_slice(&len.to_le_bytes());
    w.write_all(buf)?;
    w.write_all(text)?;
    Ok(())
}

fn push_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push((value as u8 & 0x7F) | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Reads one row from a run and appends it to `into`; appends nothing at the
/// end of the run. A damaged run is an error: a row cut short, lengths that do
/// not add up to the text, or text that is not UTF-8.
fn read_row<R: BufRead>(
    r: &mut R,
    into: &mut Packed,
    scratch: &mut Vec<u8>,
    ends: &mut Vec<u32>,
) -> Result<()> {
    let width = into.width();
    if r.fill_buf()?.is_empty() {
        return Ok(());
    }
    let short = |_| Error::new("spilled run ended mid-row".to_string());
    let corrupt = || Error::new("corrupt spilled run: cell lengths do not match the row");
    let mut head = [0u8; 4];
    r.read_exact(&mut head).map_err(short)?;
    let len = u32::from_le_bytes(head) as usize;
    scratch.clear();
    if len <= 1 << 20 {
        if scratch.capacity() < len {
            room_to_read(scratch, len)?;
        }
        scratch.resize(len, 0);
        r.read_exact(&mut scratch[..]).map_err(short)?;
    } else {
        // A length this large is rare; do not trust it to size a buffer.
        r.by_ref().take(len as u64).read_to_end(scratch)?;
        if scratch.len() != len {
            return Err(Error::new("spilled run ended mid-row".to_string()));
        }
    }
    ends.clear();
    let mut at = 0usize;
    let mut end = 0usize;
    for _ in 0..width {
        // Nearly every length is one byte; take that without a loop.
        let b = *scratch.get(at).ok_or_else(corrupt)?;
        at += 1;
        let v = if b < 0x80 {
            u64::from(b)
        } else {
            let mut v = u64::from(b & 0x7F);
            let mut shift = 7;
            loop {
                let b = *scratch.get(at).ok_or_else(corrupt)?;
                at += 1;
                v |= u64::from(b & 0x7F) << shift;
                if b & 0x80 == 0 {
                    break;
                }
                shift += 7;
                if shift > 28 {
                    return Err(corrupt());
                }
            }
            v
        };
        // The ends only grow, so the last one is the one to check below.
        if v == 0 {
            ends.push(end as u32 | Packed::ABSENT_FLAG);
        } else {
            end += (v - 1) as usize;
            ends.push(end as u32);
        }
    }
    if scratch.len() - at != end || end >= Packed::ABSENT_FLAG as usize {
        return Err(corrupt());
    }
    // A head row longer than the one before it grows its store: near a heap
    // limit that small allocation was another the merge aborted on.
    if !into.push_raw(ends, &scratch[at..])? {
        return Err(no_room_for(RUN_ROW, end + width * 4 + 8));
    }
    Ok(())
}

/// Room for `len` bytes in the buffer [`read_row`] reads a row into, which
/// grows only for a row longer than any before it, or the refusal.
#[cold]
#[inline(never)]
fn room_to_read(v: &mut Vec<u8>, len: usize) -> Result<()> {
    v.try_reserve(len.saturating_sub(v.len()))
        .map_err(|_| no_room_for(RUN_ROW, len))
}

/// What [`read_row`] holds, in a refusal.
const RUN_ROW: &str = "a row read back from a sorted run";

/// The spill record: what is written reads back, and a damaged run is an
/// error, not a panic or a wrong row.
#[cfg(test)]
mod run_format {
    use super::*;
    use std::io::Cursor;

    fn run(rows: &[&[Option<&str>]]) -> Vec<u8> {
        let mut p = Packed::new(rows[0].len());
        for r in rows {
            p.push(r.iter().copied()).unwrap();
        }
        let (mut out, mut buf) = (Vec::new(), Vec::new());
        for i in 0..p.len() {
            write_row(&mut out, &p, i, &mut buf).unwrap();
        }
        out
    }

    fn read_all(bytes: &[u8], width: usize) -> Result<Packed> {
        let mut r = Cursor::new(bytes.to_vec());
        let mut p = Packed::new(width);
        let (mut s, mut e) = (Vec::new(), Vec::new());
        loop {
            let before = p.len();
            read_row(&mut r, &mut p, &mut s, &mut e)?;
            if p.len() == before {
                return Ok(p);
            }
        }
    }

    #[test]
    fn a_run_reads_back_as_written() {
        let bytes = run(&[
            &[Some("a"), None, Some(""), Some("héllo")],
            &[None, None, None, None],
            &[Some("x"), Some("y"), Some("z"), None],
        ]);
        let p = read_all(&bytes, 4).unwrap();
        assert_eq!(p.len(), 3);
        assert_eq!(p.cell(0, 1), None);
        assert_eq!(p.cell(0, 2), Some(""));
        assert_eq!(p.cell(0, 3), Some("héllo"));
        assert_eq!(p.cell(1, 0), None);
        assert_eq!(p.cell(2, 3), None);
        assert_eq!(p.cell(2, 1), Some("y"));
    }

    #[test]
    fn a_run_cut_short_is_an_error() {
        let bytes = run(&[&[Some("abc"), Some("def")], &[Some("ghi"), Some("jkl")]]);
        for cut in [2, 5, bytes.len() / 2 + 1, bytes.len() - 1] {
            assert!(read_all(&bytes[..cut], 2).is_err(), "cut at {cut}");
        }
    }

    #[test]
    fn lengths_that_do_not_fit_the_text_are_an_error() {
        let mut bytes = run(&[&[Some("abc"), Some("def")]]);
        bytes[4] += 1; // first cell claims one byte more
        assert!(read_all(&bytes, 2).is_err());
        let mut bytes = run(&[&[Some("abc"), Some("def")]]);
        bytes[4] = 0x7f; // a length well past the text
        assert!(read_all(&bytes, 2).is_err());
        let bytes = run(&[&[Some("abc"), Some("def")]]);
        assert!(read_all(&bytes, 3).is_err(), "a row wider than the run's");
    }

    #[test]
    fn invalid_utf8_and_a_split_character_are_errors() {
        let mut bytes = run(&[&[Some("abc"), Some("def")]]);
        let last = bytes.len() - 1;
        bytes[last] = 0xFF;
        assert!(read_all(&bytes, 2).is_err());
        // "é" is two bytes; cut a cell between them.
        let mut bytes = run(&[&[Some("é"), Some("")]]);
        bytes[4] = 2; // first cell is one byte, second is zero bytes, one byte left over
        bytes[5] = 2;
        assert!(read_all(&bytes, 2).is_err());
    }

    #[test]
    fn a_huge_claimed_length_does_not_allocate_it() {
        let mut bytes = vec![0xFF, 0xFF, 0xFF, 0x7F];
        bytes.extend_from_slice(b"abc");
        assert!(read_all(&bytes, 2).is_err());
    }
}

// ---------------------------------------------------------------------------
// The join
// ---------------------------------------------------------------------------

/// Walks two sorted cursors and builds the finished join.
///
/// Every other engine indexes a whole file so it can ask "is this key on the
/// other side?". This one never asks. Both sides arrive in key order, so the
/// smaller key can only be missing from the other file, equal keys are a match,
/// and the answer falls out of walking the two cursors forward.
#[allow(clippy::too_many_arguments)]
fn merge_join(
    a: &mut Box<dyn Cursor>,
    b: &mut Box<dyn Cursor>,
    opt: &Options,
    compared: &[String],
    a_rows: i64,
    b_rows: i64,
    dup_a: &mut Dups,
    dup_b: &mut Dups,
    exporting: bool,
) -> Result<Joined> {
    let key_size = opt.key.len();
    let nc = compared.len();
    let width = key_size + nc;

    let mut changed_per = vec![0i64; nc];
    let mut blanked_per = vec![0i64; nc];
    let mut filled_per = vec![0i64; nc];

    // The rows a report shows: only when there is a report to show them. A
    // summary was building fifty thousand of each anyway, decoded into
    // strings, which is most of what this engine holds in memory -- and it is
    // the engine a comparison falls back to for want of memory.
    let cap = if opt.row_lists { opt.max_rows } else { 0 };
    // An export's changed rows are written as the merge finds them, so the
    // report's list stays at its cap either way.
    let mut changed = Capped::new(if exporting { opt.max_rows } else { cap }, false);
    let mut added = Capped::new(cap, exporting);
    let mut removed = Capped::new(cap, exporting);
    let mut changed_a: Vec<Vec<Val>> = Vec::new();
    let mut changed_b: Vec<Vec<Val>> = Vec::new();
    // The merge visits matched keys in key order, which is the order of the
    // file, so each changed row goes straight to it and is dropped.
    let mut changed_out = match (&opt.export_dir, exporting) {
        (Some(_), true) => {
            let (_, both) = export_headers(opt, compared);
            Some(ExportDir::of(opt)?.file("changed.csv", &both)?)
        }
        _ => None,
    };

    let (mut matched, mut a_keys, mut b_keys) = (0i64, 0i64, 0i64);
    // Each side's current row, copied out of its cursor: the cursor moves on
    // past the row's duplicates while the row is still being compared.
    let (mut ar, mut br) = (Packed::new(width), Packed::new(width));
    current(a, &mut ar)?;
    current(b, &mut br)?;

    loop {
        let order = match (ar.is_empty(), br.is_empty()) {
            (true, true) => break,
            (false, true) => Ordering::Less,
            (true, false) => Ordering::Greater,
            (false, false) => ar.key_order(0, &br, 0, key_size),
        };

        match order {
            Ordering::Less => {
                a_keys += 1;
                removed.push_with(|| to_cells(&ar.row(0)));
                skip_key(a, &mut ar, key_size, dup_a)?;
            }
            Ordering::Greater => {
                b_keys += 1;
                added.push_with(|| to_cells(&br.row(0)));
                skip_key(b, &mut br, key_size, dup_b)?;
            }
            Ordering::Equal => {
                a_keys += 1;
                b_keys += 1;
                matched += 1;

                let mut cells: Vec<CellDiff> = Vec::new();
                for i in 0..nc {
                    // Equal text is never a difference, with or without a
                    // tolerance, so only cells that differ as text become values.
                    if ar.cell_bytes(0, key_size + i) == br.cell_bytes(0, key_size + i) {
                        continue;
                    }
                    let (x, y) = (ar.cell(0, key_size + i), br.cell(0, key_size + i));
                    let (x, y) = (x.map(str::to_string), y.map(str::to_string));
                    if differs(&x, &y, opt) {
                        if y.is_none() {
                            blanked_per[i] += 1;
                        }
                        if x.is_none() {
                            filled_per[i] += 1;
                        }
                        changed_per[i] += 1;
                        cells.push(CellDiff {
                            column: i,
                            a: x,
                            b: y,
                        });
                    }
                }
                if !cells.is_empty() {
                    let arow = ar.row(0);
                    if let Some(out) = changed_out.as_mut() {
                        let brow = br.row(0);
                        let mut record: Vec<&str> = Vec::with_capacity(key_size + 2 * nc);
                        for v in &arow[..key_size] {
                            record.push(v.as_deref().unwrap_or(""));
                        }
                        for i in 0..nc {
                            record.push(arow[key_size + i].as_deref().unwrap_or(""));
                            record.push(brow[key_size + i].as_deref().unwrap_or(""));
                        }
                        out.write_record(&record)?;
                    }
                    changed.push_with(|| {
                        let mut row: Vec<Cell> = arow[..key_size]
                            .iter()
                            .map(|v| Cell::Value(v.clone()))
                            .collect();
                        row.push(Cell::Diffs(cells));
                        row
                    });
                    if !exporting && opt.row_lists && changed_a.len() <= opt.max_rows {
                        changed_a.push(arow);
                        changed_b.push(br.row(0));
                    }
                }
                skip_key(a, &mut ar, key_size, dup_a)?;
                skip_key(b, &mut br, key_size, dup_b)?;
            }
        }
    }

    if let (Some(out), Some(dir)) = (changed_out.take(), &opt.export_dir) {
        finish_export(out, &std::path::Path::new(dir).join("changed.csv"))?;
    }
    let columns: Vec<ColumnStat> = (0..nc)
        .map(|i| ColumnStat {
            name: compared[i].clone(),
            changed: changed_per[i],
            blanked: blanked_per[i],
            filled: filled_per[i],
        })
        .collect();

    let counts = Counts {
        a_rows,
        b_rows,
        a_keys,
        b_keys,
        matched,
        unchanged: matched - changed.total,
        changed: changed.total,
        added: added.total,
        removed: removed.total,
        a_dup_keys: dup_a.keys,
        a_dup_rows: dup_a.rows,
        b_dup_keys: dup_b.keys,
        b_dup_rows: dup_b.rows,
    };

    Ok(Joined {
        counts,
        columns,
        changed: changed.held,
        added: added.held,
        removed: removed.held,
        changed_a,
        changed_b,
        changed_written: exporting,
    })
}

/// Copies the cursor's row into `into`, which is left empty past the end.
fn current(c: &mut Box<dyn Cursor>, into: &mut Packed) -> Result<()> {
    into.clear();
    match c.peek() {
        Some((rows, row)) => into.push_row_of(rows, row),
        None => Ok(()),
    }
}

/// Advances past every row sharing the current key, counting the repeats as
/// duplicates, and loads the next key's first row into `row`. The first row
/// of a run is the one the join used, which is first-occurrence-wins: the sort
/// is stable and the merge breaks ties by run.
fn skip_key(
    c: &mut Box<dyn Cursor>,
    row: &mut Packed,
    key_size: usize,
    dups: &mut Dups,
) -> Result<()> {
    c.next()?;
    let mut repeats = 0i64;
    while let Some((rows, at)) = c.peek() {
        if rows.key_order(at, row, 0, key_size) != Ordering::Equal {
            break;
        }
        repeats += 1;
        c.next()?;
    }
    if repeats > 0 {
        dups.record(&row.row(0), repeats + 1);
    }
    current(c, row)
}

fn to_cells(row: &[Val]) -> Vec<Cell> {
    row.iter().map(|v| Cell::Value(v.clone())).collect()
}

/// A row list that stops growing at the report cap but keeps counting.
///
/// The counts in the contract are always exact and the embedded rows are always
/// capped, so holding more than the cap only ever serves `--export-dir`. Not
/// holding them is what lets this engine compare a file far larger than memory.
struct Capped {
    held: Vec<Vec<Cell>>,
    cap: usize,
    unbounded: bool,
    total: i64,
}

impl Capped {
    fn new(cap: usize, unbounded: bool) -> Self {
        Self {
            held: Vec::new(),
            cap,
            unbounded,
            total: 0,
        }
    }

    /// Counts a row, and builds it only if it will be held.
    fn push_with(&mut self, row: impl FnOnce() -> Vec<Cell>) {
        self.total += 1;
        // One past the cap, so a section can still report that it was truncated.
        if self.unbounded || self.held.len() <= self.cap {
            self.held.push(row());
        }
    }
}

/// The duplicate-key report, kept to the top `--max-rows` by count.
///
/// A bounded list of the worst offenders, rather than every duplicate, so a
/// pathological file where every key repeats does not undo the memory bound.
/// Ordering matches every other engine: most duplicated first, then by key.
struct Dups {
    key_cols: Vec<String>,
    key_size: usize,
    cap: usize,
    best: Vec<(Vec<Val>, i64)>,
    keys: i64,
    rows: i64,
}

impl Dups {
    fn new(key_cols: &[String], cap: usize) -> Self {
        Self {
            key_cols: key_cols.to_vec(),
            key_size: key_cols.len(),
            cap,
            best: Vec::new(),
            keys: 0,
            rows: 0,
        }
    }

    fn record(&mut self, row: &[Val], count: i64) {
        self.keys += 1;
        self.rows += count;
        self.best.push((row[..self.key_size].to_vec(), count));
        if self.best.len() > self.cap + 1 {
            self.sort_best();
            self.best.truncate(self.cap + 1);
        }
    }

    fn sort_best(&mut self) {
        let key_size = self.key_size;
        self.best.sort_by(|x, y| {
            y.1.cmp(&x.1)
                .then_with(|| compare_keys(&x.0, &y.0, key_size))
        });
    }

    fn section(&mut self) -> Section {
        self.sort_best();
        let rows: Vec<Vec<Cell>> = self
            .best
            .iter()
            .take(self.cap)
            .map(|(key, count)| {
                let mut row: Vec<Cell> = key.iter().map(|v| Cell::Value(v.clone())).collect();
                row.push(Cell::Count(*count));
                row
            })
            .collect();
        let mut cols = self.key_cols.clone();
        cols.push("count".to_string());
        Section {
            cols,
            rows,
            truncated: self.keys > self.cap as i64,
        }
    }
}

/// The sort-merge engine has no optional dependency.
pub fn available() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The room the sort asks for a lower-cased row: no character's lower
    /// case is more than half as long again in UTF-8.
    #[test]
    fn lower_case_grows_by_half_at_most() {
        for c in (0..=0x10FFFF).filter_map(char::from_u32) {
            let lower: usize = c.to_lowercase().map(char::len_utf8).sum();
            assert!(
                2 * lower <= 3 * c.len_utf8(),
                "{c:?} is {} bytes and {lower} lower-cased",
                c.len_utf8()
            );
        }
        // The bound is met, not loose: U+023A's lower case is a byte longer.
        assert_eq!("\u{23A}".to_lowercase().len(), 3);
    }

    #[test]
    fn a_row_asks_room_for_columns_read_twice_and_lower_case() {
        assert_eq!(room_halves(&[Some(0), Some(2), None], false), 2);
        assert_eq!(room_halves(&[Some(0), Some(2), None], true), 3);
        assert_eq!(room_halves(&[Some(1), Some(1), Some(0)], false), 4);
        assert_eq!(room_halves(&[Some(1), Some(1), Some(1)], true), 9);
        assert_eq!(room_halves(&[None, None], false), 2);
    }

    /// What the sort did before the prefix: every comparison on the cells.
    fn reference(batch: &Packed, key_size: usize) -> Vec<u32> {
        let mut order: Vec<u32> = (0..batch.len() as u32).collect();
        order.sort_by(|&x, &y| batch.key_order(x as usize, batch, y as usize, key_size));
        order
    }

    #[test]
    fn the_prefix_sort_gives_the_order_the_cells_give() {
        let mut seed = 0x2545_f491_4f6c_dd1du64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        // Cells that share a lead, are shorter or longer than eight bytes past
        // it, hold a NUL or a high byte, repeat, or are absent.
        let pool: Vec<Option<String>> = {
            let mut v: Vec<Option<String>> = vec![None, Some(String::new())];
            for lead in ["", "ACC-0000", "ACC-0000"] {
                for tail in [
                    "",
                    "a",
                    "ab",
                    "ab\0",
                    "ab\0c",
                    "12345678",
                    "123456789",
                    "12345678z",
                    "\u{ff}\u{ff}",
                    "z",
                ] {
                    v.push(Some(format!("{lead}{tail}")));
                }
            }
            v
        };
        for key_size in 1..=3usize {
            for round in 0..40 {
                let n = 2 + (next() % 300) as usize;
                let mut batch = Packed::new(key_size + 1);
                for r in 0..n {
                    let mut cells: Vec<Option<String>> = Vec::new();
                    for _ in 0..key_size {
                        // Some rounds draw from one lead only, so the shared part is long.
                        let pick = if round % 3 == 0 {
                            2 + (next() % 10) as usize
                        } else {
                            (next() % pool.len() as u64) as usize
                        };
                        cells.push(pool[pick].clone());
                    }
                    cells.push(Some(r.to_string()));
                    batch.push(cells.iter().map(|c| c.as_deref())).unwrap();
                }
                assert_eq!(
                    sorted_order(&batch, key_size).unwrap(),
                    reference(&batch, key_size),
                    "key_size {key_size} round {round}"
                );
            }
        }
    }
}
