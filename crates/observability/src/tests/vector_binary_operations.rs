use crate::tests::prelude::{
    check, formatted_sample, instant_series, json, range_pairs, range_series, timed_sample,
};

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
    let split = crate::tests::prelude::split_top_level_arithmetic_query;

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

/// `split_top_level_comparison_query` finds the comparison a `PromQL` query
/// is rooted at, ignoring operators nested inside brackets or quotes. The
/// depth guard is three counters joined by `&&`, and each has to reject on
/// its own -- so a matcher inside braces and a comparison inside
/// parentheses are both checked, each of which a loosened guard would
/// split at instead.

#[test]
pub(crate) fn a_top_level_comparison_ignores_operators_nested_inside_the_query() {
    let split = crate::tests::prelude::split_top_level_comparison_query;

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
    let split = crate::tests::prelude::split_top_level_set_query;

    check!(split_matches(split("a and b"), "a ", "and", "b"));
    check!(split_matches(split("a or b"), "a ", "or", "b"));
    check!(split_matches(split("a unless b"), "a ", "unless", "b"));

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
    check!(split_matches(
        split("sum(a and b) or c"),
        "sum(a and b) ",
        "or",
        "c"
    ));
    check!(split(r#"{app="a or b"}"#).is_none());
    check!(split_matches(
        split("rate(x[5m]) unless y"),
        "rate(x[5m]) ",
        "unless",
        "y"
    ));

    // Nothing to split.
    check!(split("a").is_none());
    check!(split("").is_none());
}

/// `apply_metric_binary_set_to_series` filters one series against another
/// by the set operator. All three operators are applied to the SAME pair,
/// so each keeps a different subset -- with one operator alone, a rule
/// that returns a constant looks correct on whichever half it agrees with.
///
/// The filter walks with an index and removes in place, only advancing
/// when it keeps. A removal that also advanced would skip the sample that
/// slid into the gap, so the dropped samples are adjacent here.
#[test]
pub(crate) fn a_binary_set_operator_keeps_the_subset_it_names() {
    use krabka_logql::MetricBinarySetOp;

    #[derive(Debug, PartialEq)]
    struct SetResult {
        kept: bool,
        timestamps: Vec<i64>,
    }

    let range = |samples: &[i64]| {
        serde_json::json!({
            "metric": {"app": "api"},
            "values": samples
                .iter()
                .map(|ts| serde_json::json!([ts, ts.to_string()]))
                .collect::<Vec<_>>(),
        })
    };
    let timestamps = |series: &serde_json::Value| {
        series
            .get("values")
            .and_then(serde_json::Value::as_array)
            .expect("the series has values")
            .iter()
            .map(|sample| sample[0].as_i64().expect("a timestamp"))
            .collect::<Vec<_>>()
    };
    // 2 and 3 are adjacent, so `and` and `unless` each drop a run rather
    // than isolated samples.
    let right = range(&[2, 3]);
    let apply = |op| {
        let mut left = range(&[1, 2, 3, 4]);
        let kept = crate::tests::prelude::apply_metric_binary_set_to_series(&mut left, &right, op);
        SetResult {
            kept,
            timestamps: timestamps(&left),
        }
    };

    check!(
        apply(MetricBinarySetOp::And)
            == (SetResult {
                kept: true,
                timestamps: vec![2, 3]
            })
    );
    check!(
        apply(MetricBinarySetOp::Unless)
            == (SetResult {
                kept: true,
                timestamps: vec![1, 4]
            })
    );
    check!(
        apply(MetricBinarySetOp::Or)
            == (SetResult {
                kept: true,
                timestamps: vec![1, 2, 3, 4]
            })
    );

    // When the filter empties the series it reports false, so the caller
    // can drop it rather than emitting an empty series.
    let mut left = range(&[1, 4]);
    check!(!crate::tests::prelude::apply_metric_binary_set_to_series(
        &mut left,
        &right,
        MetricBinarySetOp::And
    ));
    check!(timestamps(&left).is_empty());

    // `or` keeps a series the right side never matches at all.
    let mut left = range(&[9]);
    check!(crate::tests::prelude::apply_metric_binary_set_to_series(
        &mut left,
        &right,
        MetricBinarySetOp::Or
    ));

    // An instant vector carries one `value` rather than `values`, and the
    // same three rules apply to it.
    let instant = |ts: i64| serde_json::json!({"metric": {}, "value": [ts, ts.to_string()]});
    let mut matching = instant(5);
    check!(crate::tests::prelude::apply_metric_binary_set_to_series(
        &mut matching,
        &instant(5),
        MetricBinarySetOp::And
    ));
    let mut differing = instant(5);
    check!(!crate::tests::prelude::apply_metric_binary_set_to_series(
        &mut differing,
        &instant(6),
        MetricBinarySetOp::And
    ));
    let mut differing = instant(5);
    check!(crate::tests::prelude::apply_metric_binary_set_to_series(
        &mut differing,
        &instant(6),
        MetricBinarySetOp::Unless
    ));

    // A series with neither shape matches nothing.
    let mut empty = serde_json::json!({"metric": {}});
    check!(!crate::tests::prelude::apply_metric_binary_set_to_series(
        &mut empty,
        &right,
        MetricBinarySetOp::Or
    ));
}

/// A `PromQL` comparison between two vectors drops the samples that fail it
/// and gives the survivors the LEFT operand's value -- the comparison is a
/// filter, not a rewrite to a boolean. With the `bool` modifier it becomes
/// the opposite: nothing is dropped and every value becomes "1" or "0".
/// Both modes are checked over the same pair, since a mutant that ignores
/// the modifier agrees with whichever mode it happens to implement.
///
/// Samples are removed in place from two different sites -- one for a
/// timestamp the left side lacks, one for a failed comparison -- and
/// neither may advance the index. So the fixture drops an ADJACENT PAIR at
/// each site: a lone drop cannot show a skipped neighbour.
#[test]
pub(crate) fn a_vector_comparison_filters_and_takes_the_left_operand() {
    use krabka_logql::ComparisonOp;

    use crate::tests::prelude::{ComparisonResult, MetricComparison};

    let left = range_series(&[
        timed_sample(1, "10"),
        timed_sample(4, "20"),
        timed_sample(5, "20"),
    ]);
    // 2 and 3 have no left sample; 4 and 5 fail the comparison. Each pair
    // is adjacent, so an index that advanced on removal would keep one.
    let right = || {
        range_series(&[
            timed_sample(1, "1"),
            timed_sample(2, "1"),
            timed_sample(3, "1"),
            timed_sample(4, "30"),
            timed_sample(5, "30"),
            timed_sample(6, "1"),
        ])
    };

    let mut output = right();
    check!(
        crate::tests::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &left,
            MetricComparison {
                op: ComparisonOp::Greater,
                result: ComparisonResult::Filter,
            },
        )
    );
    check!(
        range_pairs(&output) == vec![formatted_sample(1, "10")],
        "only 10 > 1 survives, carrying the LEFT value"
    );

    // With `bool`, the failures stay and report 0 -- but a sample the left
    // side never had is still dropped, because there is nothing to compare.
    let mut output = right();
    check!(
        crate::tests::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &left,
            MetricComparison {
                op: ComparisonOp::Greater,
                result: ComparisonResult::Bool,
            },
        )
    );
    check!(
        range_pairs(&output)
            == vec![
                formatted_sample(1, "1"),
                formatted_sample(4, "0"),
                formatted_sample(5, "0")
            ]
    );

    // The operator is honoured, not assumed: the same pair under `<`
    // keeps exactly the samples `>` dropped.
    let mut output = right();
    check!(
        crate::tests::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &left,
            MetricComparison {
                op: ComparisonOp::Less,
                result: ComparisonResult::Filter,
            },
        )
    );
    check!(range_pairs(&output) == vec![formatted_sample(4, "20"), formatted_sample(5, "20")]);

    // Everything filtered out reports false so the caller drops the series.
    let mut output = range_series(&[timed_sample(1, "99")]);
    check!(
        !crate::tests::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &left,
            MetricComparison {
                op: ComparisonOp::Greater,
                result: ComparisonResult::Filter,
            },
        )
    );

    // A left series with no values at all matches nothing.
    let mut output = right();
    check!(
        !crate::tests::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &json!({"metric": {}}),
            MetricComparison {
                op: ComparisonOp::Greater,
                result: ComparisonResult::Filter,
            },
        )
    );

    // The instant-vector shape carries one `value`, and nothing pre-matches
    // its timestamp the way the range path does -- so the comparison itself
    // has to refuse two samples from different instants. Comparing them
    // would report a result for a moment neither side observed.
    let mut output = instant_series(timed_sample(1, "1"));
    check!(
        crate::tests::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &instant_series(timed_sample(1, "10")),
            MetricComparison {
                op: ComparisonOp::Greater,
                result: ComparisonResult::Filter,
            },
        ),
        "same instant, and 10 > 1"
    );
    check!(output["value"][1] == "10", "and it takes the left value");

    let mut output = instant_series(timed_sample(1, "1"));
    check!(
        !crate::tests::prelude::apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output,
            &instant_series(timed_sample(2, "10")),
            MetricComparison {
                op: ComparisonOp::Greater,
                result: ComparisonResult::Filter,
            },
        ),
        "different instants do not compare, however the values order"
    );
}

/// Vector arithmetic replaces each sample's value with `left op right`,
/// keeping only the timestamps both sides carry. The operator is
/// non-commutative here on purpose: subtraction and division both give a
/// different answer with the operands swapped, which a fixture using only
/// `+` or `*` could never show.
///
/// Like its comparison twin, this removes in place at two sites and the
/// index must not advance on either, so the dropped samples are adjacent.
#[test]
pub(crate) fn vector_arithmetic_computes_left_op_right_where_both_have_a_sample() {
    use krabka_logql::MetricScalarArithmeticOp;

    let left = range_series(&[
        timed_sample(1, "10"),
        timed_sample(4, "20"),
        timed_sample(5, "20"),
    ]);
    // 2, 3 and 6 have no left sample; 2 and 3 are adjacent.
    let right = || {
        range_series(&[
            timed_sample(1, "2"),
            timed_sample(2, "1"),
            timed_sample(3, "1"),
            timed_sample(4, "5"),
            timed_sample(5, "5"),
            timed_sample(6, "1"),
        ])
    };
    let apply = |op| {
        let mut output = right();
        let kept =
            crate::tests::prelude::apply_metric_binary_arithmetic_to_series_with_left_operand(
                &mut output,
                &left,
                op,
            );
        crate::tests::series_samples::BinaryOperationResult {
            kept,
            samples: range_pairs(&output),
        }
    };

    // 10-2, 20-5, 20-5 -- not 2-10, which is what a swap would give.
    check!(
        apply(MetricScalarArithmeticOp::Subtract)
            == crate::tests::series_samples::BinaryOperationResult {
                kept: true,
                samples: vec![
                    formatted_sample(1, "8"),
                    formatted_sample(4, "15"),
                    formatted_sample(5, "15")
                ],
            }
    );
    check!(
        apply(MetricScalarArithmeticOp::Divide)
            == crate::tests::series_samples::BinaryOperationResult {
                kept: true,
                samples: vec![
                    formatted_sample(1, "5"),
                    formatted_sample(4, "4"),
                    formatted_sample(5, "4")
                ],
            }
    );
    check!(
        apply(MetricScalarArithmeticOp::Multiply)
            == crate::tests::series_samples::BinaryOperationResult {
                kept: true,
                samples: vec![
                    formatted_sample(1, "20"),
                    formatted_sample(4, "100"),
                    formatted_sample(5, "100")
                ],
            }
    );

    // A division with no answer drops its sample rather than emitting one.
    let mut output = range_series(&[timed_sample(1, "0")]);
    check!(
        !crate::tests::prelude::apply_metric_binary_arithmetic_to_series_with_left_operand(
            &mut output,
            &left,
            MetricScalarArithmeticOp::Divide,
        )
    );

    // The instant shape again, where nothing pre-matches the timestamps.
    let mut output = instant_series(timed_sample(1, "2"));
    check!(
        crate::tests::prelude::apply_metric_binary_arithmetic_to_series_with_left_operand(
            &mut output,
            &instant_series(timed_sample(1, "10")),
            MetricScalarArithmeticOp::Subtract,
        )
    );
    check!(output["value"][1] == "8");

    let mut output = instant_series(timed_sample(1, "2"));
    check!(
        !crate::tests::prelude::apply_metric_binary_arithmetic_to_series_with_left_operand(
            &mut output,
            &instant_series(timed_sample(2, "10")),
            MetricScalarArithmeticOp::Subtract,
        ),
        "two different instants have no arithmetic between them"
    );
}
