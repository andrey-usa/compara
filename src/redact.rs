//! URLs as compara shows them: never with a credential in them.
//!
//! A URL can carry one in two places: before its host (`https://user:token@
//! host/`) and after its path, in the query string a presigned S3 URL, an
//! Azure SAS URL or GitHub's CDN signs with (`?X-Amz-Signature=…`, `?sig=…`),
//! or the fragment. Any URL a request went to can be one of those: the one
//! the user gave, the one a redirect sent it on to, a proxy's, an endpoint's.
//! Every message, step line, report and summary names a URL by its scheme,
//! host and path, with `…` where those parts were, through [`urls`].

use std::borrow::Cow;

/// `text` with every URL in it shown without its user, password, query
/// string or fragment: `https://u:p@host/a.csv?sig=x` reads
/// `https://…@host/a.csv?…`.
///
/// A URL starts at its scheme and ends at white space or a quote, as a URL
/// written into a message does; closing punctuation after a dropped query,
/// the `)` of `for url (…)`, is kept. Object storage URLs (`s3://`, `gs://`,
/// `az://` and the like) and `file://` are kept whole: what follows their
/// host is a key, where `?` is a glob, and the `@` of `abfss://container@
/// account` names a container; their credentials never go in the URL.
pub fn urls(text: &str) -> Cow<'_, str> {
    if !text.contains("://") {
        return Cow::Borrowed(text);
    }
    let bytes = text.as_bytes();
    let mut out = String::new();
    let mut copied = 0;
    let mut from = 0;
    while let Some(found) = text[from..].find("://") {
        let sep = from + found;
        let mut start = sep;
        while start > copied
            && (bytes[start - 1].is_ascii_alphanumeric() || b"+-.".contains(&bytes[start - 1]))
        {
            start -= 1;
        }
        while start < sep && !bytes[start].is_ascii_alphabetic() {
            start += 1;
        }
        let end = text[sep + 3..]
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '<' | '>' | '`'))
            .map_or(text.len(), |n| sep + 3 + n);
        from = end.max(sep + 3);
        if start == sep || kept_whole(&text[start..sep]) {
            continue;
        }
        out.push_str(&text[copied..start]);
        shown(&text[start..end], sep - start, &mut out);
        copied = end;
    }
    if copied == 0 {
        return Cow::Borrowed(text);
    }
    out.push_str(&text[copied..]);
    Cow::Owned(out)
}

/// Schemes whose URLs carry no credential and use `?` and `@` otherwise.
fn kept_whole(scheme: &str) -> bool {
    [
        "s3", "s3a", "gs", "az", "adl", "azure", "abfs", "abfss", "file",
    ]
    .iter()
    .any(|s| scheme.eq_ignore_ascii_case(s))
}

/// One URL, its scheme `scheme_len` bytes long, onto `out` as [`urls`] shows it.
fn shown(url: &str, scheme_len: usize, out: &mut String) {
    let rest = &url[scheme_len + 3..];
    out.push_str(&url[..scheme_len + 3]);
    let authority_len = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, rest) = rest.split_at(authority_len);
    match authority.rfind('@') {
        Some(at) => {
            out.push_str("…@");
            out.push_str(&authority[at + 1..]);
        }
        None => out.push_str(authority),
    }
    let Some(cut) = rest.find(['?', '#']) else {
        out.push_str(rest);
        return;
    };
    out.push_str(&rest[..=cut]);
    out.push('…');
    let dropped = &rest[cut + 1..];
    let closing = dropped
        .trim_end_matches([')', ']', '}', '\'', ',', '.', ';', ':', '!'])
        .len();
    out.push_str(&dropped[closing..]);
}

#[cfg(test)]
mod tests {
    use super::urls;

    #[test]
    fn a_url_keeps_its_scheme_host_and_path_and_nothing_else() {
        for (given, want) in [
            ("https://h.example/a/b.csv", "https://h.example/a/b.csv"),
            (
                "https://h.example/a.csv?sig=SECRET",
                "https://h.example/a.csv?…",
            ),
            (
                "https://h.example/a.csv#SECRET",
                "https://h.example/a.csv#…",
            ),
            ("https://h.example?SECRET", "https://h.example?…"),
            ("http://u:SECRET@h:8080/a.csv", "http://…@h:8080/a.csv"),
            ("http://SECRET@h", "http://…@h"),
            ("http://u:p@SECRET@h/a?x@y", "http://…@h/a?…"),
            ("HTTPS://h/a?SECRET", "HTTPS://h/a?…"),
            (
                "error sending request for url (http://h/a?sig=SE)CRET): refused",
                "error sending request for url (http://h/a?…): refused",
            ),
            (
                "not found: https://h/a.csv?X-Amz-Signature=SECRET&X-Amz-Date=1",
                "not found: https://h/a.csv?…",
            ),
            (
                "GET \"https://acct.blob.core.windows.net/c/a?sv=1&sig=SECRET\" failed",
                "GET \"https://acct.blob.core.windows.net/c/a?…\" failed",
            ),
            (
                "a: http://x/a?SECRET b: socks5://u:SECRET@p:1080 c",
                "a: http://x/a?… b: socks5://…@p:1080 c",
            ),
            ("(proxy http://u:SECRET@p:3128)", "(proxy http://…@p:3128)"),
        ] {
            let got = urls(given);
            assert_eq!(got, want, "{given}");
            assert!(!got.contains("SECRET"), "{given}");
        }
    }

    #[test]
    fn text_without_a_url_and_storage_urls_are_left_as_they_are() {
        for text in [
            "no url here: a?b@c",
            "s3://bkt/t_?.csv",
            "abfss://container@account.dfs.core.windows.net/a.parquet",
            "gs://b/k?x",
            "file:///tmp/a?b.csv",
            "://h/a?b",
            "/tmp/a.csv",
        ] {
            assert!(
                matches!(urls(text), std::borrow::Cow::Borrowed(_)),
                "{text}"
            );
        }
    }
}
