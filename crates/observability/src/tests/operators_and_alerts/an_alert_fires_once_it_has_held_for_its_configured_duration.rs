use super::*;

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
    let states = super::super::prelude::SharedPrometheusAlertStates::default();
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
        super::super::prelude::prometheus_alerts_from_query_result(
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
    let immediate = super::super::prelude::SharedPrometheusAlertStates::default();
    let no_hold: serde_yaml::Mapping =
        serde_yaml::from_str("severity: page\n").expect("the rule fields parse");
    let alerts = super::super::prelude::prometheus_alerts_from_query_result(
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
