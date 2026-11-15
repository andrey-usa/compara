//! Named comparison profiles from `compara.toml`, so a recurring comparison does
//! not get retyped.
//!
//! ```toml
//! [profiles.orders]
//! key       = ["order_id", "line_no"]
//! compare   = ["qty", "price", "status"]   # omit for all common non-key columns
//! ignore    = ["updated_at"]
//! trim      = true
//! tolerance = 0.005
//! ```
//!
//! The file format is shared with the other implementations.

use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{Error, Result};
use crate::options::Options;

/// One profile. Every field is optional because "absent" and "false" mean
/// different things: only the keys actually present override the defaults.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Profile {
    pub key: Option<Vec<String>>,
    pub compare: Option<Vec<String>>,
    pub ignore: Option<Vec<String>>,
    pub trim: Option<bool>,
    pub ignore_case: Option<bool>,
    pub empty_is_null: Option<bool>,
    pub tolerance: Option<f64>,
    pub max_rows: Option<usize>,
    pub delimiter: Option<char>,
    pub encoding: Option<String>,
    pub engine: Option<String>,
}

impl Profile {
    /// Lays the profile down under the command line, which then overrides it.
    pub fn apply_to(&self, opt: &mut Options) {
        if let Some(v) = &self.key {
            opt.key = v.clone();
        }
        if let Some(v) = &self.compare {
            opt.compare = Some(v.clone());
        }
        if let Some(v) = &self.ignore {
            opt.ignore = v.clone();
        }
        if let Some(v) = self.trim {
            opt.trim = v;
        }
        if let Some(v) = self.ignore_case {
            opt.ignore_case = v;
        }
        if let Some(v) = self.empty_is_null {
            opt.empty_is_null = v;
        }
        if let Some(v) = self.tolerance {
            opt.tolerance = v;
        }
        if let Some(v) = self.max_rows {
            opt.max_rows = v;
        }
        if let Some(v) = self.delimiter {
            opt.delimiter = Some(v);
        }
        if let Some(v) = &self.encoding {
            opt.encoding = v.clone();
        }
        if let Some(v) = &self.engine {
            opt.engine = v.clone();
        }
    }
}

/// Where a config file is looked for when no path is given.
pub fn search_path() -> Vec<PathBuf> {
    let mut paths = vec![PathBuf::from("compara.toml")];
    if let Ok(home) = std::env::var("HOME") {
        paths.push(PathBuf::from(home).join(".config/compara/compara.toml"));
    }
    paths
}

/// Reads the profile tables from a config file, or returns nothing when there is none.
///
/// This reads the subset of TOML the config format actually uses — `[profiles.x]`
/// tables of scalars and string arrays — rather than pulling in a parser for a
/// file that is a dozen lines long.
pub fn load(explicit: Option<&str>) -> Result<HashMap<String, Profile>> {
    find(explicit).map(|(_, profiles)| profiles)
}

/// [`load`], with the file the profiles came from: `None` when no path was
/// given and neither place a config file is looked for has one.
pub fn find(explicit: Option<&str>) -> Result<(Option<PathBuf>, HashMap<String, Profile>)> {
    let candidates: Vec<PathBuf> = match explicit {
        Some(p) => vec![PathBuf::from(p)],
        None => search_path(),
    };
    for path in candidates {
        if !path.is_file() {
            continue;
        }
        let text = fs::read_to_string(&path)
            .map_err(|e| Error::new(format!("cannot read {}: {e}", path.display())))?;
        let profiles = parse(&text)
            .map_err(|e| Error::input(format!("cannot parse {}: {e}", path.display())))?;
        return Ok((Some(path), profiles));
    }
    match explicit {
        Some(p) => Err(Error::new(format!("config file not found: {p}"))),
        None => Ok((None, HashMap::new())),
    }
}

/// The refusal for a profile name the config file does not have, with what
/// it does have: a name is usually one letter off one of them.
pub fn not_found(name: &str, file: Option<&Path>, profiles: &HashMap<String, Profile>) -> Error {
    let mut names: Vec<&str> = profiles.keys().map(String::as_str).collect();
    names.sort_unstable();
    let has = match (file, names.is_empty()) {
        (None, _) => "no compara.toml here or in ~/.config/compara; --config PATH names one".into(),
        (Some(path), true) => format!("{} has no [profiles.NAME] table", path.display()),
        (Some(path), false) => format!("{} has: {}", path.display(), names.join(", ")),
    };
    Error::input(format!("profile not found: {name} ({has})"))
}

/// The keys a profile may set, as the config file spells them.
const KEYS: [&str; 11] = [
    "key",
    "compare",
    "ignore",
    "trim",
    "ignore_case",
    "empty_is_null",
    "tolerance",
    "max_rows",
    "delimiter",
    "encoding",
    "engine",
];

/// Parses the profile tables out of a config file's text.
///
/// Everything in a `[profiles.NAME]` table is read or refused: a key this
/// does not know, or a value of the wrong kind, is a mistake that would
/// otherwise change nothing and say nothing -- `ignore_cas = true` compared
/// with case, and `trim = yes` did not trim. Tables that are not profiles
/// belong to something else and are passed over.
pub fn parse(text: &str) -> Result<HashMap<String, Profile>> {
    let mut out: HashMap<String, Profile> = HashMap::new();
    let mut current: Option<String> = None;
    let mut lines = text.lines().enumerate();
    while let Some((n, raw_line)) = lines.next() {
        let at = |e: Error| e.context(format!("line {}: ", n + 1));
        let line = strip_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }
        if let Some(table) = line.strip_prefix('[') {
            let name = table
                .strip_suffix(']')
                .ok_or_else(|| at(Error::new(format!("a table name is never closed: {line}"))))?
                .trim();
            current = match name.strip_prefix("profiles.") {
                Some(profile) => Some(text_of(profile.trim()).map_err(at)?),
                None => None,
            };
            if let Some(name) = &current {
                out.entry(name.clone()).or_default();
            }
            continue;
        }
        let Some(name) = &current else { continue };
        let Some((key, value)) = line.split_once('=') else {
            return Err(at(Error::new(format!(
                "expected `key = value` in profile {name}, got: {line}"
            ))));
        };
        // An array may be written over several lines, one name to a line,
        // as formatters write it: read as one value. Taken a line at a
        // time, `key = [` was an empty key and the lines under it were
        // dropped.
        let mut value = value.trim().to_string();
        while value.starts_with('[') && !array_closed(&value) {
            let Some((_, more)) = lines.next() else {
                return Err(at(Error::new(format!(
                    "the list for `{}` is never closed with a `]`",
                    key.trim()
                ))));
            };
            value.push(' ');
            value.push_str(strip_comment(more).trim());
        }
        let profile = out.entry(name.clone()).or_default();
        assign(profile, name, key.trim(), &value).map_err(at)?;
    }
    Ok(out)
}

fn assign(profile: &mut Profile, name: &str, key: &str, raw: &str) -> Result<()> {
    let list = || -> Result<Vec<String>> {
        let items = if raw.starts_with('[') {
            list_of(raw)?
        } else {
            vec![text_of(raw)?]
        };
        Ok(items.into_iter().filter(|s| !s.is_empty()).collect())
    };
    let flag = || match raw {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(Error::new(format!(
            "{key} must be true or false, got: {raw}"
        ))),
    };
    let number = || Error::new(format!("{key} must be a number, got: {raw}"));
    match key {
        "key" => profile.key = Some(list()?),
        "compare" => profile.compare = Some(list()?),
        "ignore" => profile.ignore = Some(list()?),
        "trim" => profile.trim = Some(flag()?),
        "ignore_case" => profile.ignore_case = Some(flag()?),
        "empty_is_null" => profile.empty_is_null = Some(flag()?),
        "tolerance" => profile.tolerance = Some(raw.parse().map_err(|_| number())?),
        "max_rows" => profile.max_rows = Some(raw.parse().map_err(|_| number())?),
        "delimiter" => {
            profile.delimiter = crate::options::parse_delimiter(&text_of(raw)?)
                .map_err(|e| Error::new(e.to_string().replace("--delimiter", "delimiter")))?;
        }
        "encoding" => profile.encoding = Some(text_of(raw)?),
        "engine" => profile.engine = Some(text_of(raw)?),
        _ => {
            let meant = KEYS
                .iter()
                .map(|k| (distance(key, k), k))
                .filter(|(d, _)| *d <= 2)
                .min()
                .map(|(_, k)| format!(" (did you mean `{k}`?)"))
                .unwrap_or_default();
            return Err(Error::new(format!(
                "unknown key `{key}` in profile {name}{meant}: a profile sets {}",
                KEYS.join(", ")
            )));
        }
    }
    Ok(())
}

/// Removes a trailing `#` comment: one outside any string.
fn strip_comment(value: &str) -> &str {
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for (i, c) in value.char_indices() {
        match quote {
            Some('"') if escaped => escaped = false,
            Some('"') if c == '\\' => escaped = true,
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == '"' || c == '\'' => quote = Some(c),
            None if c == '#' => return &value[..i],
            None => {}
        }
    }
    value
}

/// Whether the list that opens `value` has met its closing bracket, outside
/// any string.
fn array_closed(value: &str) -> bool {
    let mut quote: Option<char> = None;
    let mut escaped = false;
    for c in value.chars() {
        match quote {
            Some('"') if escaped => escaped = false,
            Some('"') if c == '\\' => escaped = true,
            Some(q) if c == q => quote = None,
            Some(_) => {}
            None if c == '"' || c == '\'' => quote = Some(c),
            None if c == ']' => return true,
            None => {}
        }
    }
    false
}

/// One value as text: a `"basic"` string with its escapes read, a
/// `'literal'` one as written, or a bare word.
fn text_of(raw: &str) -> Result<String> {
    let (text, rest) = string_at(raw.trim())?;
    if rest.trim().is_empty() {
        Ok(text)
    } else {
        Err(Error::new(format!(
            "unexpected text after a value: {}",
            rest.trim()
        )))
    }
}

/// The string or bare word `raw` opens with, and what follows it.
fn string_at(raw: &str) -> Result<(String, &str)> {
    let mut chars = raw.char_indices();
    match chars.next() {
        Some((_, '\'')) => match raw[1..].find('\'') {
            Some(end) => Ok((raw[1..1 + end].to_string(), &raw[end + 2..])),
            None => Err(Error::new(format!("a string is never closed: {raw}"))),
        },
        Some((_, '"')) => {
            let mut out = String::new();
            while let Some((i, c)) = chars.next() {
                match c {
                    '"' => return Ok((out, &raw[i + 1..])),
                    '\\' => match chars.next().map(|(_, e)| e) {
                        Some('t') => out.push('\t'),
                        Some('n') => out.push('\n'),
                        Some('r') => out.push('\r'),
                        Some('"') => out.push('"'),
                        Some('\\') => out.push('\\'),
                        Some('u') => {
                            let hex: String = chars.by_ref().take(4).map(|(_, h)| h).collect();
                            let c = u32::from_str_radix(&hex, 16)
                                .ok()
                                .filter(|_| hex.len() == 4)
                                .and_then(char::from_u32)
                                .ok_or_else(|| {
                                    Error::new(format!("\\u{hex} is not a character in: {raw}"))
                                })?;
                            out.push(c);
                        }
                        other => {
                            return Err(Error::new(format!(
                                "\\{} is not an escape a string has, in: {raw}",
                                other.map(String::from).unwrap_or_default()
                            )));
                        }
                    },
                    c => out.push(c),
                }
            }
            Err(Error::new(format!("a string is never closed: {raw}")))
        }
        _ => {
            let end = raw.find([',', ']']).unwrap_or(raw.len());
            Ok((raw[..end].trim().to_string(), &raw[end..]))
        }
    }
}

/// A `["list", "of", "strings"]`, a comma after the last allowed.
fn list_of(raw: &str) -> Result<Vec<String>> {
    let mut rest = raw.trim().strip_prefix('[').unwrap_or(raw).trim_start();
    let mut items = Vec::new();
    loop {
        if let Some(after) = rest.strip_prefix(']') {
            return if after.trim().is_empty() {
                Ok(items)
            } else {
                Err(Error::new(format!(
                    "unexpected text after a list: {}",
                    after.trim()
                )))
            };
        }
        let (item, after) = string_at(rest)?;
        items.push(item);
        rest = after.trim_start();
        match rest.strip_prefix(',') {
            Some(after) => rest = after.trim_start(),
            None if rest.starts_with(']') => {}
            None => {
                return Err(Error::new(format!(
                    "expected `,` or `]` in a list, got: {rest}"
                )));
            }
        }
    }
}

/// The edits between two short names, for "did you mean".
fn distance(a: &str, b: &str) -> usize {
    let (a, b): (Vec<char>, Vec<char>) = (a.chars().collect(), b.chars().collect());
    let mut row: Vec<usize> = (0..=b.len()).collect();
    for (i, x) in a.iter().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, y) in b.iter().enumerate() {
            let above = row[j + 1];
            row[j + 1] = (diagonal + usize::from(x != y))
                .min(above + 1)
                .min(row[j] + 1);
            diagonal = above;
        }
    }
    row[b.len()]
}
