use super::*;

/// `split_query_param_pairs` breaks a query string on `&` only when a
/// KNOWN key follows it. That is not the usual rule, and it exists because
/// a `LogQL` matcher can contain an ampersand -- splitting on every one
/// would cut a query in half and leave both halves unparseable.
#[test]
pub(crate) fn a_query_string_splits_only_before_a_known_key() {
    fn split(query: &str) -> Vec<&str> {
        super::super::prelude::split_query_param_pairs(query, &["query", "start", "end"])
    }

    check!(split("query=up") == vec!["query=up"]);
    check!(split("query=up&start=1") == vec!["query=up", "start=1"]);
    check!(split("query=up&start=1&end=2") == vec!["query=up", "start=1", "end=2"]);

    // An ampersand inside a value is kept, because what follows it is not
    // a known key. This is the case the whole function exists for.
    check!(
        split(r#"query={app="a&b"}&start=1"#) == vec![r#"query={app="a&b"}"#, "start=1"],
        "the matcher keeps its ampersand"
    );
    check!(
        split("query=a&b=c") == vec!["query=a&b=c"],
        "b is not a known key"
    );

    // A known key needs its `=` to count as one: "&start" alone is text.
    check!(split("query=a&start") == vec!["query=a&start"]);
    check!(
        split("query=a&startle=1") == vec!["query=a&startle=1"],
        "not a prefix match"
    );

    // Empty segments are dropped rather than yielded as empty strings.
    check!(split("") == Vec::<&str>::new());
    check!(split("&query=a") == vec!["query=a"]);
    // A trailing `&` is KEPT, since nothing follows it to be a known key.
    // The rule is about what comes after the ampersand, not about the
    // ampersand itself.
    check!(split("query=a&") == vec!["query=a&"]);
}
