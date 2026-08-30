use super::*;

/// `split_top_level_set_query` is the third splitter, over `PromQL`'s set
/// operators. Unlike the symbol splitters these are WORDS, so a match must
/// also stand alone: "android" starts with "and" and is not a set
/// operation. That word-boundary test is the whole difference between this
/// splitter and the other two.
///
/// Two things here cannot be tested and are not: the order the three
/// operators are tried in, since none is a prefix of another and the
/// boundary test applies to each; and the `is_ascii_alphabetic` precheck,
/// which only skips characters where `starts_with` would fail anyway. Both
/// are equivalent mutations rather than gaps.
#[test]
pub(crate) fn a_top_level_set_split_needs_a_whole_word() {
    let split = super::super::prelude::split_top_level_set_query;

    check!(split("a and b") == Some(("a ", "and", "b")));
    check!(split("a or b") == Some(("a ", "or", "b")));
    check!(split("a unless b") == Some(("a ", "unless", "b")));

    // A word that merely starts with an operator is not one. Each of the
    // three has its own trap, since only a shared boundary test saves
    // them all at once.
    check!(split("android").is_none(), "and is not a prefix match");
    check!(split("orders").is_none());
    check!(split("unlessened").is_none());
    check!(split("a android b").is_none(), "nor mid-query");

    // Nor is one glued to its neighbours without spaces.
    check!(split("aand b").is_none());
    check!(split("a andb").is_none());

    // Nested operators are not top level, and a quoted one is text.
    check!(split("sum(a and b) or c") == Some(("sum(a and b) ", "or", "c")));
    check!(split(r#"{app="a or b"}"#).is_none());
    check!(split("rate(x[5m]) unless y") == Some(("rate(x[5m]) ", "unless", "y")));

    // Nothing to split.
    check!(split("a").is_none());
    check!(split("").is_none());
}
