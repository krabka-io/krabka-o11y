use super::*;

/// `split_top_level_comparison_query` finds the comparison a `PromQL` query
/// is rooted at, ignoring operators nested inside brackets or quotes. The
/// depth guard is three counters joined by `&&`, and each has to reject on
/// its own -- so a matcher inside braces and a comparison inside
/// parentheses are both checked, each of which a loosened guard would
/// split at instead.

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
pub(crate) fn a_top_level_comparison_ignores_operators_nested_inside_the_query() {
    let split = super::super::prelude::split_top_level_comparison_query;

    // Every operator, and the longest match wins: `>=` is not `>`.
    check!(split_matches(split("up > 1"), "up ", ">", "1"));
    check!(split_matches(split("up >= 1"), "up ", ">=", "1"));
    check!(split_matches(split("up < 1"), "up ", "<", "1"));
    check!(split_matches(split("up <= 1"), "up ", "<=", "1"));
    check!(split_matches(split("up == 1"), "up ", "==", "1"));
    check!(split_matches(split("up != 1"), "up ", "!=", "1"));
    check!(
        split_matches(split("up>=1"), "up", ">=", "1"),
        "without spaces"
    );

    // A label matcher inside braces is not a top-level comparison. This is
    // the case the brace counter exists for: loosening the guard splits
    // the query at the matcher's own `!=` and leaves a broken left side.
    check!(split_matches(
        split(r#"{app!="a"} > 1"#),
        r#"{app!="a"} "#,
        ">",
        "1"
    ));

    // Nor is a comparison inside parentheses -- the outer one wins.
    check!(split_matches(split("(a > b) > 2"), "(a > b) ", ">", "2"));

    // Nor one inside a quoted string, which the quote tracking skips.
    check!(split_matches(
        split(r#"{app="x>y"} > 1"#),
        r#"{app="x>y"} "#,
        ">",
        "1"
    ));

    // A range selector's brackets nest too.
    check!(split_matches(
        split("sum(rate(up[5m])) > 0.5"),
        "sum(rate(up[5m])) ",
        ">",
        "0.5"
    ));

    // The bracket counter is defensive: no real range selector contains a
    // comparison, so nothing valid exercises it. This input is not a
    // PromQL query, but the scanner takes any string, and the counter's
    // whole purpose is to not split inside brackets.
    check!(split_matches(split("a[>]b > 1"), "a[>]b ", ">", "1"));

    // No top-level comparison at all.
    check!(split("up").is_none());
    check!(split("sum(rate(up[5m]))").is_none());
    check!(
        split(r#"{app!="a"}"#).is_none(),
        "a matcher alone is not one"
    );
}
