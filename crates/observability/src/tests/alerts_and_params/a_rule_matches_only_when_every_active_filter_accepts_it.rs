use super::*;

/// `matches_rule` filters the rules response by kind, by name, and by label
/// selector. The three are independent AND conditions, each inactive when
/// its filter is unset, so each is broken on its own against a rule the
/// other two accept.
///
/// The label selectors nest differently from the rest: SELECTORS are
/// or-ed and the matchers WITHIN a selector are and-ed, which is Loki's
/// `match[]` semantics. A single selector with a single matcher cannot
/// show that, so both nestings are exercised.
#[test]
pub(crate) fn a_rule_matches_only_when_every_active_filter_accepts_it() {
    use krabka_logql::{LabelMatcher, MatchOp, StreamQuery};

    let rule = serde_json::json!({"type": "alerting", "name": "HighErrors"});
    let source: serde_yaml::Value =
        serde_yaml::from_str("labels:\n  severity: page\n  team: infra\n")
            .expect("the source rule parses");
    let matcher = |name: &str, value: &str| LabelMatcher {
        name: name.to_string(),
        op: MatchOp::Equal,
        value: value.to_string(),
    };
    let selector = |matchers: Vec<LabelMatcher>| StreamQuery {
        matchers,
        pipeline: Vec::new(),
    };
    let filters = |kind, names: &[&str], selectors: Vec<StreamQuery>| {
        super::super::prelude::PrometheusRulesFilters {
            rule_kind: kind,
            rule_names: names.iter().map(|name| (*name).to_string()).collect(),
            label_selectors: selectors,
            ..super::super::prelude::PrometheusRulesFilters::default()
        }
    };

    // No filters at all accepts everything.
    check!(filters(None, &[], Vec::new()).matches_rule(&rule, &source));

    // Each filter alone, accepting and rejecting.
    check!(filters(Some("alerting"), &[], Vec::new()).matches_rule(&rule, &source));
    check!(!filters(Some("recording"), &[], Vec::new()).matches_rule(&rule, &source));
    check!(filters(None, &["HighErrors"], Vec::new()).matches_rule(&rule, &source));
    check!(!filters(None, &["Other"], Vec::new()).matches_rule(&rule, &source));
    check!(
        filters(None, &["Other", "HighErrors"], Vec::new()).matches_rule(&rule, &source),
        "any of the named rules"
    );

    // Matchers WITHIN a selector are and-ed: one wrong matcher rejects.
    check!(
        filters(None, &[], vec![selector(vec![matcher("severity", "page")])])
            .matches_rule(&rule, &source)
    );
    check!(
        !filters(
            None,
            &[],
            vec![selector(vec![
                matcher("severity", "page"),
                matcher("team", "billing"),
            ])],
        )
        .matches_rule(&rule, &source),
        "every matcher in a selector must match"
    );

    // SELECTORS are or-ed: one that fails does not reject if another
    // succeeds.
    check!(
        filters(
            None,
            &[],
            vec![
                selector(vec![matcher("team", "billing")]),
                selector(vec![matcher("team", "infra")]),
            ],
        )
        .matches_rule(&rule, &source),
        "any selector may match"
    );

    // All three active together, with one of them failing.
    check!(
        !filters(
            Some("alerting"),
            &["HighErrors"],
            vec![selector(vec![matcher("team", "billing")])],
        )
        .matches_rule(&rule, &source),
        "the label selector still rejects"
    );
}
