use super::*;

/// `split_top_level_arithmetic_query` is the comparison splitter's twin
/// over the six arithmetic operators. It has the same depth guard, and it
/// maps the matched character back to a static string -- an arm returning
/// a neighbour's symbol still produces a valid split, so every operator is
/// pinned to its own.
///
/// The first operator wins, which matters because these are scanned left
/// to right with no precedence: "a - b * c" splits at the minus.
#[test]
pub(crate) fn a_top_level_arithmetic_split_names_the_operator_it_found() {
    let split = super::super::prelude::split_top_level_arithmetic_query;

    check!(split("a + b") == Some(("a ", "+", "b")));
    check!(split("a - b") == Some(("a ", "-", "b")));
    check!(split("a * b") == Some(("a ", "*", "b")));
    check!(split("a / b") == Some(("a ", "/", "b")));
    check!(split("a % b") == Some(("a ", "%", "b")));
    check!(split("a ^ b") == Some(("a ", "^", "b")));

    // Leftmost wins, with no precedence applied during the split.
    check!(split("a - b * c") == Some(("a ", "-", "b * c")));
    check!(split("a * b - c") == Some(("a ", "*", "b - c")));

    // Nested operators are not top level, in each kind of bracket.
    check!(split("sum(a + b) * 2") == Some(("sum(a + b) ", "*", "2")));
    check!(split("rate(x[5m]) * 2") == Some(("rate(x[5m]) ", "*", "2")));
    check!(split(r#"{app="a-b"} * 2"#) == Some((r#"{app="a-b"} "#, "*", "2")));

    // And an operator inside a quoted string is just text.
    check!(split(r#"{app="a+b"}"#).is_none());
    check!(split("sum(a + b)").is_none());
    check!(split("a").is_none());
}
