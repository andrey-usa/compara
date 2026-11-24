//! Capping the report sections and writing the uncapped CSV exports.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::contract::{Cell, EngineMeta, EngineResult, Section, Val};
use crate::error::{Error, Result};
use crate::options::Options;
use crate::output::Output;
use crate::rowstore::Joined;
use crate::source::WorkDir;

/// Turns a completed join into the capped report sections, and writes the
/// uncapped CSV exports when `--export-dir` is set.
pub fn assemble(
    meta: EngineMeta,
    joined: Joined,
    dup_a: Section,
    dup_b: Section,
    opt: &Options,
    compared: &[String],
) -> Result<EngineResult> {
    if opt.export_dir.is_some() {
        export(&ExportDir::of(opt)?, &joined, opt, compared)?;
    }
    Ok(sections(meta, joined, dup_a, dup_b, opt, compared))
}

/// The capped report sections alone, for an engine that writes its export
/// itself rather than from a [`Joined`].
pub fn sections(
    meta: EngineMeta,
    joined: Joined,
    dup_a: Section,
    dup_b: Section,
    opt: &Options,
    compared: &[String],
) -> EngineResult {
    let (cols, _) = export_headers(opt, compared);
    EngineResult {
        meta,
        counts: joined.counts,
        columns: joined.columns,
        changed: Section::capped(opt.key.clone(), joined.changed, opt.max_rows),
        added: Section::capped(cols.clone(), joined.added, opt.max_rows),
        removed: Section::capped(cols, joined.removed, opt.max_rows),
        dup_a,
        dup_b,
    }
}

/// The columns of the export files: an added or removed row is the key and
/// the compared columns, and a changed row puts the two sides of each compared
/// column side by side, which the sparse report payload cannot show.
pub fn export_headers(opt: &Options, compared: &[String]) -> (Vec<String>, Vec<String>) {
    let mut rows = opt.key.clone();
    rows.extend_from_slice(compared);
    let mut both = opt.key.clone();
    for c in compared {
        both.push(format!("{c} (A)"));
        both.push(format!("{c} (B)"));
    }
    (rows, both)
}

/// Creates the export directory if it is missing.
///
/// A link at the directory's name is refused, as one at a file's is: the
/// files would land wherever it points.
pub fn create_export_dir(dir: &Path) -> Result<()> {
    crate::output::refuse_link(dir)?;
    fs::create_dir_all(dir)
        .map_err(|e| Error::new(format!("cannot create export-dir {}: {e}", dir.display())))
}

/// Where the export files go: the directory `--export-dir` names, or
/// compara's own staging directory for an export bound for object storage,
/// held open, the files made through it.
pub struct ExportDir {
    path: PathBuf,
    staged: Option<Arc<WorkDir>>,
}

impl ExportDir {
    /// The export directory of `opt`, created if it is missing.
    pub fn of(opt: &Options) -> Result<ExportDir> {
        let path = PathBuf::from(opt.export_dir.as_deref().unwrap_or("."));
        let staged = opt.export_staged.clone();
        if staged.is_none() {
            create_export_dir(&path)?;
        }
        Ok(ExportDir { path, staged })
    }

    /// The export file `name`, for messages.
    pub fn path(&self, name: &str) -> PathBuf {
        self.path.join(name)
    }

    /// Opens the export file `name` and writes its header, as
    /// [`export_file`] does.
    pub fn file(&self, name: &str, header: &[String]) -> Result<csv::Writer<Output>> {
        let out = match &self.staged {
            Some(dir) => Output::create_in(dir, name)?,
            None => Output::create(&self.path(name))?,
        };
        let mut w = csv::Writer::from_writer(out);
        w.write_record(header)?;
        Ok(w)
    }
}

/// Opens one export file and writes its header. Every engine writes through
/// this, so the files are quoted and terminated the same way whichever path
/// produced them.
///
/// The file goes through [`Output`]: it reaches its name only when the
/// writer is committed, and a link at the name is refused.
pub fn export_file(path: &Path, header: &[String]) -> Result<csv::Writer<Output>> {
    let mut w = csv::Writer::from_writer(Output::create(path)?);
    w.write_record(header)?;
    Ok(w)
}

/// Flushes an export file and puts it at its name.
pub fn finish_export(w: csv::Writer<Output>, path: &Path) -> Result<()> {
    w.into_inner()
        .map_err(|e| Error::new(format!("cannot write {}: {e}", path.display())))?
        .commit()
}

fn export(dir: &ExportDir, joined: &Joined, opt: &Options, compared: &[String]) -> Result<()> {
    let (cols, both) = export_headers(opt, compared);
    write_cells(dir, "added.csv", &cols, &joined.added)?;
    write_cells(dir, "removed.csv", &cols, &joined.removed)?;

    if joined.changed_written {
        return Ok(());
    }
    let key_size = opt.key.len();
    let rows: Vec<Vec<Val>> = joined
        .changed_a
        .iter()
        .zip(&joined.changed_b)
        .map(|(ar, br)| {
            let mut row: Vec<Val> = ar[..key_size].to_vec();
            for i in 0..compared.len() {
                row.push(ar[key_size + i].clone());
                row.push(br[key_size + i].clone());
            }
            row
        })
        .collect();
    write_rows(dir, "changed.csv", &both, &rows)
}

fn write_cells(dir: &ExportDir, name: &str, header: &[String], rows: &[Vec<Cell>]) -> Result<()> {
    let values: Vec<Vec<Val>> = rows
        .iter()
        .map(|row| row.iter().map(|c| c.value().map(str::to_string)).collect())
        .collect();
    write_rows(dir, name, header, &values)
}

/// Writes an uncapped export file from rows of values.
fn write_rows(dir: &ExportDir, name: &str, header: &[String], rows: &[Vec<Val>]) -> Result<()> {
    let mut w = dir.file(name, header)?;
    let mut record: Vec<&str> = Vec::with_capacity(header.len());
    for row in rows {
        record.clear();
        for i in 0..header.len() {
            record.push(row.get(i).and_then(Option::as_deref).unwrap_or(""));
        }
        w.write_record(&record)?;
    }
    finish_export(w, &dir.path(name))
}
