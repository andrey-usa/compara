//! The README's Quick start, run as it is written: each `$ compara …` line of
//! its console block, from a directory holding the repository's `examples/`,
//! its output compared with the lines under it, and `$ echo $?` with the exit
//! code of the command before. Times and the report's size are masked: they
//! are the only parts of the output that change from run to run.

use std::path::{Path, PathBuf};
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_compara");

/// `0.011s` (or `0s`: a run under a millisecond) and `(20 KB)` become
/// `<time>` and `(<size> KB)`.
fn mask(line: &str) -> String {
    line.split(' ')
        .map(|word| {
            let bare = word.trim_end_matches([',', ')']);
            let tail = &word[bare.len()..];
            let number =
                |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit() || b == b'.');
            if let Some(n) = bare.strip_suffix('s')
                && number(n)
            {
                format!("<time>{tail}")
            } else if let Some(n) = bare.strip_prefix('(')
                && number(n)
            {
                format!("(<size>{tail}")
            } else {
                word.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn scratch() -> PathBuf {
    let dir = std::env::temp_dir().join(format!("compara-readme-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let examples = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples");
    for entry in std::fs::read_dir(&examples).unwrap() {
        let entry = entry.unwrap();
        let to = dir.join("examples").join(entry.file_name());
        std::fs::create_dir_all(to.parent().unwrap()).unwrap();
        std::fs::copy(entry.path(), to).unwrap();
    }
    dir
}

#[test]
fn the_readme_quick_start_runs_as_written() {
    let readme = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md"))
        .unwrap()
        // A Windows checkout has CRLF line ends.
        .replace("\r\n", "\n");
    let section = &readme[readme
        .find("\n## Quick start\n")
        .expect("README.md has a Quick start")..];
    let open = section
        .find("```console\n")
        .expect("the Quick start has a console block")
        + "```console\n".len();
    let block = &section[open..open + section[open..].find("```").unwrap()];
    let lines: Vec<&str> = block.lines().collect();
    let dir = scratch();
    let (mut i, mut last, mut ran): (usize, Option<i32>, u32) = (0, None, 0);
    while i < lines.len() {
        let command = lines[i]
            .strip_prefix("$ ")
            .unwrap_or_else(|| panic!("not a command: {}", lines[i]));
        i += 1;
        let start = i;
        while i < lines.len() && !lines[i].starts_with("$ ") {
            i += 1;
        }
        let expected: Vec<String> = lines[start..i].iter().map(|l| mask(l)).collect();
        let got: Vec<String> = if command == "echo $?" {
            vec![last.expect("`echo $?` after a command").to_string()]
        } else {
            let mut words = command.split_whitespace();
            assert_eq!(
                words.next(),
                Some("compara"),
                "the Quick start runs compara only: {command}"
            );
            let out = Command::new(BIN)
                .args(words)
                .current_dir(&dir)
                .output()
                .unwrap();
            last = out.status.code();
            ran += 1;
            let text = String::from_utf8_lossy(&out.stdout).into_owned()
                + &String::from_utf8_lossy(&out.stderr);
            text.lines().map(mask).collect()
        };
        assert_eq!(
            got, expected,
            "README.md's Quick start: `{command}` printed something else"
        );
    }
    assert!(ran >= 1, "the Quick start ran no command");
    let _ = std::fs::remove_dir_all(&dir);
}
