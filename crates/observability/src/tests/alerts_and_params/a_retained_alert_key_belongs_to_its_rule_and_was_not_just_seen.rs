use super::*;

/// `prometheus_alert_key_matches_rule` picks out the alerts belonging to
/// one rule that were NOT seen in this evaluation -- the ones that may need
/// retaining as resolved. All four conditions are and-ed, so each is broken
/// on its own against a key the other three accept.
///
/// The last is the negated one: a key still active this round is excluded,
/// which is what stops a firing alert being retained twice.
#[test]
pub(crate) fn a_retained_alert_key_belongs_to_its_rule_and_was_not_just_seen() {
    let key = |tenant: &str, alert: &str, query: &str| super::super::prelude::PrometheusAlertKey {
        tenant: tenant.to_string(),
        alert_name: alert.to_string(),
        query: query.to_string(),
        labels: Labels::default(),
    };
    let subject = key("tenant", "HighErrors", "up");
    let active = BTreeSet::new();
    let templates = Labels::default();
    let params = |active_keys| super::super::prelude::PrometheusRetainedAlertParams {
        tenant: "tenant",
        alert_name: "HighErrors",
        query: "up",
        evaluation_time: 0,
        hold_duration_ns: 0,
        keep_firing_for_ns: 0,
        active_keys,
        annotation_templates: &templates,
    };

    check!(super::prelude::prometheus_alert_key_matches_rule(
        &subject,
        &params(&active)
    ));

    // Each of the three identity fields, wrong on its own.
    check!(!super::prelude::prometheus_alert_key_matches_rule(
        &key("other", "HighErrors", "up"),
        &params(&active)
    ));
    check!(!super::prelude::prometheus_alert_key_matches_rule(
        &key("tenant", "Other", "up"),
        &params(&active)
    ));
    check!(!super::prelude::prometheus_alert_key_matches_rule(
        &key("tenant", "HighErrors", "down"),
        &params(&active)
    ));

    // And the negated one: a key seen this round is not retained.
    let mut seen = BTreeSet::new();
    seen.insert(subject.clone());
    check!(
        !super::prelude::prometheus_alert_key_matches_rule(&subject, &params(&seen)),
        "an alert still firing is not also retained"
    );
    // A different key being active does not exclude this one.
    let mut other_seen = BTreeSet::new();
    other_seen.insert(key("tenant", "HighErrors", "other"));
    check!(super::prelude::prometheus_alert_key_matches_rule(
        &subject,
        &params(&other_seen)
    ));
}
