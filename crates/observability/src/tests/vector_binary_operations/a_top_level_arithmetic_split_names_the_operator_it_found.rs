use super::*;

/// `split_top_level_arithmetic_query` is the comparison splitter's twin
/// over the six arithmetic operators. It has the same depth guard, and it
/// maps the matched character back to a static string -- an arm returning
/// a neighbour's symbol still produces a valid split, so every operator is
/// pinned to its own.
///
/// The first operator wins, which matters because these are scanned left
/// to right with no precedence: "a - b * c" splits at the minus.

fn split_matches(
    actual: Option<crate::http::params_format::metric_formatting::TopLevelQuerySplit<'_>>,
    left: &str,
    operator: &str,
    right: &str,
) -> bool {
    actual.is_some_and(|split| {
        split.left == left && split.operator == operator && split.right == right
    })
}

#[test]
pub(crate) fn a_top_level_arithmetic_split_names_the_operator_it_found() {
    let split = super::super::prelude::split_top_level_arithmetic_query;

    check!(split_matches(split("a + b"), "a ", "+", "b"));
    check!(split_matches(split("a - b"), "a ", "-", "b"));
    check!(split_matches(split("a * b"), "a ", "*", "b"));
    check!(split_matches(split("a / b"), "a ", "/", "b"));
    check!(split_matches(split("a % b"), "a ", "%", "b"));
    check!(split_matches(split("a ^ b"), "a ", "^", "b"));

    // Leftmost wins, with no precedence applied during the split.
    check!(split_matches(split("a - b * c"), "a ", "-", "b * c"));
    check!(split_matches(split("a * b - c"), "a ", "*", "b - c"));

    // Nested operators are not top level, in each kind of bracket.
    check!(split_matches(
        split("sum(a + b) * 2"),
        "sum(a + b) ",
        "*",
        "2"
    ));
    check!(split_matches(
        split("rate(x[5m]) * 2"),
        "rate(x[5m]) ",
        "*",
        "2"
    ));
    check!(split_matches(
        split(r#"{app="a-b"} * 2"#),
        r#"{app="a-b"} "#,
        "*",
        "2"
    ));

    // And an operator inside a quoted string is just text.
    check!(split(r#"{app="a+b"}"#).is_none());
    check!(split("sum(a + b)").is_none());
    check!(split("a").is_none());
}
