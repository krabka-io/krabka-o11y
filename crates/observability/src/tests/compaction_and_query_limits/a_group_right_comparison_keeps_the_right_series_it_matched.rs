use super::*;

/// `group_right` keeps the *right* side's series and carries the named
/// labels over from the left. The match is by the `on` key, and a series
/// the comparison rejects is dropped rather than kept at either value.
#[test]
pub(crate) fn a_group_right_comparison_keeps_the_right_series_it_matched() {
    let series = |labels: Value, value: &str| json!({"metric": labels, "value": [0, value]});
    let mut left = json!({"data": {"result": [
        series(json!({"app": "api", "env": "prod"}), "5")
    ]}});
    let right = json!({"data": {"result": [
        series(json!({"app": "api", "instance": "a"}), "1"),
        series(json!({"app": "api", "instance": "b"}), "9"),
        series(json!({"app": "worker", "instance": "c"}), "0")
    ]}});
    let matching = MetricVectorMatching::On {
        labels: vec!["app".to_owned()],
        group: Some(MetricVectorGroupModifier::Right(vec!["env".to_owned()])),
    };

    apply_metric_binary_comparison_to_loki_result(
        &mut left,
        &right,
        ComparisonOp::Greater,
        false,
        Some(&matching),
    );

    // Only the right series the left one beat survives, wearing the
    // right's own labels plus `env` carried over, and the left operand's
    // value. The `worker` series matches no left key at all.
    check!(
        left == json!({"data": {"result": [{
            "metric": {"app": "api", "instance": "a", "env": "prod"},
            "value": [0, "5"]
        }]}})
    );
}
