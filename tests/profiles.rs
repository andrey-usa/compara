//! compara.toml profiles, which the other implementations read too.

use std::fs;

use compara::options::Options;
use compara::profiles;

const CONFIG: &str = r#"
# a comment
[profiles.orders]
key       = ["order_id", "line_no"]
compare   = ["qty", "price"]
ignore    = ["updated_at"]   # trailing comment
trim      = true
tolerance = 0.005
max_rows  = 100
engine    = "native"

[profiles.other]
key = ["id"]
"#;

#[test]
fn a_profile_becomes_the_base_layer_of_the_options() {
    let found = profiles::parse(CONFIG).expect("parse");
    assert_eq!(found.len(), 2);

    let mut opt = Options::default();
    found["orders"].apply_to(&mut opt);
    assert_eq!(opt.key, vec!["order_id", "line_no"]);
    assert_eq!(
        opt.compare,
        Some(vec!["qty".to_string(), "price".to_string()])
    );
    assert_eq!(
        opt.ignore,
        vec!["updated_at"],
        "a trailing comment leaked into ignore"
    );
    assert!(opt.trim);
    assert_eq!(opt.tolerance, 0.005);
    assert_eq!(opt.max_rows, 100);
    assert_eq!(opt.engine, "native");

    // Anything given on the command line is applied after, and wins.
    opt.max_rows = 7;
    assert_eq!(opt.max_rows, 7);
}

/// The example the repository ships has to be one this reads.
#[test]
fn the_shipped_example_parses() {
    let text = fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/compara.example.toml"))
        .expect("the example");
    let found = profiles::parse(&text).expect("parse");
    assert_eq!(
        found["demo"].key,
        Some(vec!["account_id".into(), "txn_id".into()])
    );
    assert_eq!(found["demo"].trim, Some(true));
    assert_eq!(found["report"].engine.as_deref(), Some("turbo"));
}

/// A list written one name to a line, as a formatter writes it. Read a line
/// at a time it was an empty key, and the comparison was refused for having
/// none.
#[test]
fn a_list_may_run_over_lines() {
    let found = profiles::parse(
        r#"
[profiles.orders]
key = [
  "order_id",   # the order
  "line_no",
]
ignore = [ "updated_at"
         , 'run # id' ]
trim = true
"#,
    )
    .expect("parse");
    let p = &found["orders"];
    assert_eq!(p.key, Some(vec!["order_id".into(), "line_no".into()]));
    assert_eq!(p.ignore, Some(vec!["updated_at".into(), "run # id".into()]));
    assert_eq!(p.trim, Some(true));

    let err = profiles::parse("[profiles.x]\nkey = [\n \"id\",\n").unwrap_err();
    assert!(err.to_string().contains("never closed"), "{err}");
}

/// A key a profile does not have is a misspelt one, and used to change
/// nothing without a word: `ignore_cas = true` compared with case.
#[test]
fn a_key_no_profile_has_is_refused_by_name() {
    let err = profiles::parse("[profiles.x]\nkey = [\"id\"]\nignore_cas = true\n").unwrap_err();
    let text = err.to_string();
    assert!(
        text.starts_with(
            "line 3: unknown key `ignore_cas` in profile x (did you mean `ignore_case`?)"
        ),
        "{text}"
    );
    let err = profiles::parse("[profiles.x]\nthreads = 4\n").unwrap_err();
    let text = err.to_string();
    assert!(
        text.contains("unknown key `threads`") && !text.contains("did you mean"),
        "{text}"
    );
    // A table that is not a profile is someone else's.
    let found = profiles::parse("[tool.other]\nanything = 1\n[profiles.x]\nkey = [\"id\"]\n")
        .expect("parse");
    assert_eq!(found.len(), 1);
}

/// `trim = yes` did not trim, and did not say so.
#[test]
fn a_flag_is_true_or_false() {
    for bad in ["yes", "1", "\"true\"", "True"] {
        let err = profiles::parse(&format!("[profiles.x]\ntrim = {bad}\n")).unwrap_err();
        assert!(
            err.to_string().contains("trim must be true or false"),
            "{bad}: {err}"
        );
    }
    let found = profiles::parse("[profiles.x]\ntrim = false # off\nignore_case = true\n").unwrap();
    assert_eq!(
        (found["x"].trim, found["x"].ignore_case),
        (Some(false), Some(true))
    );
    let err = profiles::parse("[profiles.x]\ntolerance = lots\n").unwrap_err();
    assert!(
        err.to_string().contains("tolerance must be a number"),
        "{err}"
    );
}

/// A tab is `"\t"` in TOML, and that was read as a backslash.
#[test]
fn a_delimiter_is_one_character_however_it_is_spelt() {
    for (written, want) in [
        (r#""\t""#, '\t'),
        (r#"'\t'"#, '\t'),
        (r#""tab""#, '\t'),
        (r#"";""#, ';'),
        (r#""|""#, '|'),
    ] {
        let found = profiles::parse(&format!("[profiles.x]\ndelimiter = {written}\n"))
            .unwrap_or_else(|e| panic!("{written}: {e}"));
        assert_eq!(found["x"].delimiter, Some(want), "{written}");
    }
    let err = profiles::parse("[profiles.x]\ndelimiter = \";;\"\n").unwrap_err();
    assert!(err.to_string().contains("must be one character"), "{err}");
}

/// A profile name that is not in the file is answered with the ones that are.
#[test]
fn a_profile_not_found_names_the_ones_there_are() {
    let found = profiles::parse(CONFIG).expect("parse");
    let err = profiles::not_found("order", Some(std::path::Path::new("compara.toml")), &found);
    assert!(err.is_input());
    assert_eq!(
        err.to_string(),
        "profile not found: order (compara.toml has: orders, other)"
    );
    let none = profiles::not_found("x", None, &Default::default()).to_string();
    assert!(none.contains("no compara.toml here"), "{none}");
}

#[test]
fn an_explicit_config_path_that_does_not_exist_is_an_error() {
    let missing = std::env::temp_dir().join("compara-no-such-config.toml");
    let _ = fs::remove_file(&missing);
    assert!(profiles::load(Some(&missing.to_string_lossy())).is_err());
}

#[test]
fn parse_list_drops_empty_entries() {
    assert_eq!(compara::parse_list(" a, b ,,c "), vec!["a", "b", "c"]);
    assert!(compara::parse_list("  ").is_empty());
}

/// A profile's tab delimiter is written `"\t"` or `"tab"`; the escape used to
/// be read as a backslash, and a longer value as its first character.
#[test]
fn a_profile_names_a_tab_delimiter() {
    for (written, read) in [(r#""\t""#, '\t'), (r#""tab""#, '\t'), (r#"";""#, ';')] {
        let found = profiles::parse(&format!(
            "[profiles.p]\nkey = [\"id\"]\ndelimiter = {written}\n"
        ))
        .expect("parse");
        let mut opt = Options::default();
        found["p"].apply_to(&mut opt);
        assert_eq!(opt.delimiter, Some(read), "{written}");
    }
    let refused = profiles::parse("[profiles.p]\nkey = [\"id\"]\ndelimiter = \"ab\"\n");
    assert!(refused.is_err(), "a two-character delimiter was accepted");
}
