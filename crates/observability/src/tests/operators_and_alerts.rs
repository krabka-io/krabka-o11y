use super::prelude::*;

/// `parse_metric_arithmetic_operator` names the six `PromQL` scalar
/// operators. The variants are asserted pairwise distinct, so an arm
/// returning a neighbour's operator cannot pass -- and every unrecognised
/// spelling is refused rather than defaulted, since a silent default here
/// would compute the wrong arithmetic instead of failing the query.
#[test]
pub(crate) fn every_promql_scalar_operator_parses_to_its_own_variant() {
    let parse = super::parse_metric_arithmetic_operator;

    check!(parse("+") == Some(MetricScalarArithmeticOp::Add));
    check!(parse("-") == Some(MetricScalarArithmeticOp::Subtract));
    check!(parse("*") == Some(MetricScalarArithmeticOp::Multiply));
    check!(parse("/") == Some(MetricScalarArithmeticOp::Divide));
    check!(parse("%") == Some(MetricScalarArithmeticOp::Modulo));
    check!(parse("^") == Some(MetricScalarArithmeticOp::Power));

    // Nothing else parses, including operators PromQL has elsewhere.
    check!(parse("").is_none());
    check!(parse("**").is_none());
    check!(parse("+ ").is_none(), "the operator is not trimmed here");
    check!(parse("and").is_none());
    check!(parse("==").is_none(), "a comparison is not arithmetic");

    let variants = [
        parse("+"),
        parse("-"),
        parse("*"),
        parse("/"),
        parse("%"),
        parse("^"),
    ];
    for (index, left) in variants.iter().enumerate() {
        for right in &variants[index + 1..] {
            check!(left != right, "two operators share a variant: {left:?}");
        }
    }
}

/// `split_leading_vector_group_modifier` peels a `group_left`/`group_right`
/// off the front of a vector-match clause, with or without a label list.
/// Four routes leave the function and each returns a different shape, so
/// each is pinned: no modifier, a bare one, one with labels, one with an
/// empty list, and an unclosed list -- which returns the query untouched
/// rather than a half-parsed modifier.
#[test]
pub(crate) fn a_leading_vector_group_modifier_is_peeled_with_its_labels() {
    let split = super::split_leading_vector_group_modifier;

    // No modifier: the query comes back whole.
    check!(split("foo") == (None, "foo"));
    check!(split("  foo") == (None, "foo"), "but trimmed at the front");
    check!(split("") == (None, ""));

    // A bare modifier, with the remainder handed back trimmed.
    check!(split("group_left foo") == (Some("group_left".to_string()), "foo"));
    check!(split("group_right foo") == (Some("group_right".to_string()), "foo"));

    // With labels, which are folded into the modifier's own text.
    check!(
        split("group_left(instance) foo") == (Some("group_left (instance)".to_string()), " foo")
    );
    check!(split("group_right(a,b) foo") == (Some("group_right (a,b)".to_string()), " foo"));

    // An empty label list is the bare modifier again, not "group_left ()".
    check!(split("group_left() foo") == (Some("group_left".to_string()), " foo"));

    // An unclosed label list is not a modifier at all: the query is
    // returned untouched rather than half-consumed.
    check!(split("group_left(instance foo") == (None, "group_left(instance foo"));

    // The match is a bare prefix test, not a word match, so a longer
    // identifier starting with a modifier name is split mid-word. That is
    // current behaviour rather than obviously desirable, and it is pinned
    // so a change to it is deliberate.
    //
    // The order the two modifiers are tried in cannot matter: neither is
    // a prefix of the other, so at most one can ever strip. Swapping them
    // is an equivalent mutation, not an untested one.
    check!(split("group_rightish") == (Some("group_right".to_string()), "ish"));
}

/// A `Prometheus` alert is PENDING until it has been continuously active for
/// its `for` duration, then FIRING. The transition is at `>=`, so an alert
/// exactly at its hold duration is already firing -- one nanosecond either
/// side of that instant is the only pair separating `>=` from `>`.
///
/// `active_at` is remembered across evaluations, which is what makes the
/// duration a duration rather than a single-evaluation check: the same
/// alert is evaluated three times here against one shared state.
#[test]
pub(crate) fn an_alert_fires_once_it_has_held_for_its_configured_duration() {
    let states = super::SharedPrometheusAlertStates::default();
    let fields: serde_yaml::Mapping =
        serde_yaml::from_str("for: 5m\n").expect("the rule fields parse");
    let result = serde_json::json!({
        "data": {
            "result": [{
                "metric": {"job": "api"},
                "value": [0, "1"],
            }],
        }
    });
    let hold_ns = 5 * 60 * 1_000_000_000_i64;
    let started = 1_000_000_000_000_i64;
    let evaluate = |at| {
        super::prometheus_alerts_from_query_result(
            &states,
            "tenant",
            "HighErrors",
            &fields,
            "up",
            at,
            &result,
        )
    };
    let state_at = |at| {
        let alerts = evaluate(at);
        check!(alerts.len() == 1, "one sample means one alert");
        alerts[0]["state"].as_str().expect("a state").to_string()
    };

    // First evaluation starts the clock: nothing has held yet.
    check!(state_at(started) == "pending");

    // One nanosecond short of the hold duration is still pending, and
    // exactly at it is firing. Those two evaluations are the test.
    check!(state_at(started + hold_ns - 1) == "pending");
    check!(state_at(started + hold_ns) == "firing");
    check!(state_at(started + hold_ns + 1) == "firing");

    // The alert carries the labels of its sample plus its own name, and
    // reports the value the query returned.
    let alerts = evaluate(started + hold_ns);
    check!(alerts[0]["labels"]["job"] == "api");
    check!(
        alerts[0]["labels"]["alertname"] == "HighErrors",
        "the rule's name is added to the sample's labels"
    );
    check!(alerts[0]["value"] == "1");

    // A rule with no `for` fires on its first evaluation, since a zero
    // hold duration is satisfied immediately.
    let immediate = super::SharedPrometheusAlertStates::default();
    let no_hold: serde_yaml::Mapping =
        serde_yaml::from_str("severity: page\n").expect("the rule fields parse");
    let alerts = super::prometheus_alerts_from_query_result(
        &immediate,
        "tenant",
        "Immediate",
        &no_hold,
        "up",
        started,
        &result,
    );
    check!(alerts[0]["state"] == "firing");
}

/// Evaluating one rule prunes the alert states belonging to *it*, and
/// leaves every other rule's alone. The three identity fields are or-ed,
/// so a state is kept when any one of them differs -- and each has to be
/// the only difference, or a mutant that requires two of them would still
/// keep it.
#[test]
pub(crate) fn evaluating_one_alert_rule_leaves_the_other_rules_states_alone() {
    let states = super::SharedPrometheusAlertStates::default();
    let fields: serde_yaml::Mapping =
        serde_yaml::from_str("severity: page\n").expect("the rule fields parse");
    let result = serde_json::json!({
        "data": { "result": [{ "metric": {"job": "api"}, "value": [0, "1"] }] }
    });
    let evaluate = |tenant: &str, alert: &str, query: &str| {
        super::prometheus_alerts_from_query_result(
            &states, tenant, alert, &fields, query, 1_000, &result,
        );
    };
    let held = || {
        states
            .alerts
            .lock()
            .expect("the alert states lock is not poisoned")
            .keys()
            .map(|key| {
                (
                    key.tenant.clone(),
                    key.alert_name.clone(),
                    key.query.clone(),
                )
            })
            .collect::<Vec<_>>()
    };

    evaluate("t1", "A", "up");
    check!(held().len() == 1);

    // Each of these differs from the first in exactly one field, and none
    // of them may disturb it.
    evaluate("t2", "A", "up");
    evaluate("t1", "B", "up");
    evaluate("t1", "A", "down");

    let mut keys = held();
    keys.sort();
    check!(
        keys == vec![
            ("t1".to_string(), "A".to_string(), "down".to_string()),
            ("t1".to_string(), "A".to_string(), "up".to_string()),
            ("t1".to_string(), "B".to_string(), "up".to_string()),
            ("t2".to_string(), "A".to_string(), "up".to_string()),
        ],
        "got {keys:?}"
    );
}

/// `signed_vector_function_literal_error` catches `vector(+1)`, which `LogQL`
/// does not accept -- the argument must be a bare number. It skips any
/// whitespace after the parenthesis before looking, so the reported column
/// is the SIGN's, not the parenthesis's, and the message names which sign
/// was found.
///
/// As with the unspaced-operator detector, the column counts characters:
/// one case puts multi-byte text ahead of the call so a byte offset gives
/// a different number.
#[test]
pub(crate) fn a_signed_vector_literal_is_reported_at_the_sign() {
    let error = super::signed_vector_function_literal_error;
    let column = |query: &str| {
        error(query).map(|message| {
            message
                .split("col ")
                .nth(1)
                .and_then(|rest| rest.split(':').next())
                .expect("the message names a column")
                .parse::<usize>()
                .expect("the column is a number")
        })
    };

    check!(column("vector(+1)") == Some(8));
    check!(column("vector(-1)") == Some(8));

    // Whitespace after the parenthesis is skipped, so the column follows
    // the sign rather than sitting on the bracket.
    check!(column("vector( +1)") == Some(9));
    check!(column("vector(   -1)") == Some(11));

    // The message names the sign it found, not a fixed one.
    check!(
        error("vector(+1)")
            .expect("a signed literal is an error")
            .contains("unexpected +, expecting NUMBER")
    );
    check!(
        error("vector(-1)")
            .expect("a signed literal is an error")
            .contains("unexpected -, expecting NUMBER")
    );

    // An unsigned argument is fine, and so is anything that is not a sign.
    check!(error("vector(1)").is_none());
    check!(error("vector( 1)").is_none());
    check!(error("vector(x)").is_none());
    check!(error("vector()").is_none());

    // Characters, not bytes: fourteen characters precede the sign here
    // but sixteen bytes do.
    check!(column("(\"\u{e9}\u{e9}\")+vector(-1)") == Some(15));

    // A `vector(` inside a string is text.
    check!(error("(\"vector(+1)\")").is_none());

    // The parenthesis is part of the match, not assumed to follow. Without
    // it, "vector -1" would land the offset straight on the minus and
    // report a signed literal for a call that was never made.
    check!(error("vector -1").is_none());
    check!(error("vector_total -1").is_none());
}

/// `unspaced_vector_set_operator_error` catches `)and` written without a
/// space -- a `LogQL` typo that would otherwise fail somewhere unhelpful --
/// and reports the column the operator starts at.
///
/// That column is a CHARACTER count, not a byte offset, so one case puts
/// multi-byte text before the parenthesis: with ASCII alone the two are
/// the same number and a byte count passes.
#[test]
pub(crate) fn an_unspaced_set_operator_is_reported_at_its_own_column() {
    let error = super::unspaced_vector_set_operator_error;
    let column = |query: &str| {
        error(query).map(|message| {
            message
                .split("col ")
                .nth(1)
                .and_then(|rest| rest.split(':').next())
                .expect("the message names a column")
                .parse::<usize>()
                .expect("the column is a number")
        })
    };

    // All three operators, each glued to the closing parenthesis.
    check!(column("vector(1)and vector(2)") == Some(10));
    check!(column("vector(1)or vector(2)") == Some(10));
    check!(column("vector(1)unless vector(2)") == Some(10));

    // Properly spaced is not an error.
    check!(error("vector(1) and vector(2)").is_none());
    check!(error("vector(1)").is_none());

    // A closing parenthesis followed by anything else is fine.
    check!(error("vector(1)+vector(2)").is_none());

    // Unlike the set-operator SPLITTER, this detector has no word-boundary
    // test, so `)android` is reported as an unspaced `and`. That is a
    // false positive, but it fires only on a query that is already a
    // syntax error, so it turns one bad message into a better-placed one.
    // Pinned because it is behaviour, not because it is desirable.
    check!(column("vector(1)android") == Some(10));

    // The column counts characters. Six characters precede the operator
    // here but eight bytes do, because each accented letter takes two.
    check!(column("(\"\u{e9}\u{e9}\")and 1") == Some(7));

    // A `)and` inside a string is text, not an operator.
    check!(error("vector(1)").is_none());
    check!(error("(\")and\")").is_none());

    // The check only applies to scalar-vector expressions: an aggregation
    // is parsed elsewhere and must not be second-guessed here.
    check!(error("sum(rate(x[5m]))and y").is_none());
}

/// `format_vector_aggregation_query` renders an aggregation back to its
/// `LogQL` spelling. Most operators take an optional grouping clause, but
/// three -- `approx_topk`, sort and `sort_desc` -- have no grouped form and
/// must refuse rather than render one, so each is checked BOTH ways.
///
/// The two limit-taking operators put their limit inside the parentheses
/// ahead of the inner query, where the ungrouped ones do not, which is why
/// the names alone are not enough to pin them.
#[test]
pub(crate) fn a_vector_aggregation_renders_only_the_groupings_its_operator_allows() {
    use krabka_logql::{VectorAggregation, VectorAggregationOp, VectorGrouping};

    let render = |op, grouping| {
        super::format_vector_aggregation_query(&VectorAggregation { op, grouping }, "up")
    };
    let by = || {
        Some(VectorGrouping::By(vec![
            "job".to_string(),
            "app".to_string(),
        ]))
    };
    let without = || Some(VectorGrouping::Without(vec!["pod".to_string()]));

    // Plain operators, ungrouped and grouped both ways.
    check!(render(VectorAggregationOp::Sum, None) == Some("sum(up)".to_string()));
    check!(render(VectorAggregationOp::Count, None) == Some("count(up)".to_string()));
    check!(render(VectorAggregationOp::Min, None) == Some("min(up)".to_string()));
    check!(render(VectorAggregationOp::Max, None) == Some("max(up)".to_string()));
    check!(render(VectorAggregationOp::Avg, None) == Some("avg(up)".to_string()));
    check!(render(VectorAggregationOp::Stddev, None) == Some("stddev(up)".to_string()));
    check!(render(VectorAggregationOp::Stdvar, None) == Some("stdvar(up)".to_string()));

    // The grouping is joined with a comma and sits before the parentheses.
    check!(render(VectorAggregationOp::Sum, by()) == Some("sum by (job,app)(up)".to_string()));
    check!(
        render(VectorAggregationOp::Max, without()) == Some("max without (pod)(up)".to_string())
    );

    // The limit-taking operators put their limit inside, before the inner
    // query, and still accept a grouping.
    check!(render(VectorAggregationOp::TopK(3), None) == Some("topk(3,up)".to_string()));
    check!(render(VectorAggregationOp::BottomK(3), None) == Some("bottomk(3,up)".to_string()));
    check!(
        render(VectorAggregationOp::TopK(5), by()) == Some("topk by (job,app)(5,up)".to_string())
    );

    // These three have no grouped form: rendered ungrouped, refused with
    // a grouping. Both directions matter -- a mutant that dropped the
    // guard would render an expression LogQL cannot parse back.
    check!(render(VectorAggregationOp::Sort, None) == Some("sort(up)".to_string()));
    check!(render(VectorAggregationOp::Sort, by()).is_none());
    check!(render(VectorAggregationOp::SortDesc, None) == Some("sort_desc(up)".to_string()));
    check!(render(VectorAggregationOp::SortDesc, without()).is_none());
    check!(
        render(VectorAggregationOp::ApproxTopK(4), None) == Some("approx_topk(4,up)".to_string())
    );
    check!(render(VectorAggregationOp::ApproxTopK(4), by()).is_none());

    // count_values has no rendering at all, grouped or not.
    check!(render(VectorAggregationOp::CountValues("x".to_string()), None).is_none());
    check!(render(VectorAggregationOp::CountValues("x".to_string()), by()).is_none());
}
