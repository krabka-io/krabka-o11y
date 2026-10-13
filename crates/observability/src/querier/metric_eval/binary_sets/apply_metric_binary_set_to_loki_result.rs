use super::{
    BTreeSet, MetricBinarySetOp, MetricVectorMatching, Value, apply_metric_binary_set_to_series,
    metric_series_labels, metric_vector_matching_key, sort_loki_metric_results_by_labels,
};

pub(crate) fn apply_metric_binary_set_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: MetricBinarySetOp,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(left_results) = left
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(right_results) = right.pointer("/data/result").and_then(Value::as_array) else {
        if matches!(op, MetricBinarySetOp::And) {
            left_results.clear();
        }
        return;
    };

    if matches!(op, MetricBinarySetOp::Or) {
        let left_label_sets = left_results
            .iter()
            .filter_map(metric_series_labels)
            .map(|labels| metric_vector_matching_key(&labels, matching))
            .collect::<BTreeSet<_>>();
        for right_series in right_results {
            let Some(right_labels) = metric_series_labels(right_series) else {
                continue;
            };
            let right_key = metric_vector_matching_key(&right_labels, matching);
            if !left_label_sets.contains(&right_key) {
                left_results.push(right_series.clone());
            }
        }
        sort_loki_metric_results_by_labels(left_results);
        return;
    }

    super::super::matching_metric_result_series::retain_matching_metric_result_series(
        left_results,
        right_results,
        matching,
        |left_series, right_series| match (op, right_series) {
            (MetricBinarySetOp::And | MetricBinarySetOp::Unless, Some(right_series)) => {
                apply_metric_binary_set_to_series(left_series, right_series, op)
            }
            (MetricBinarySetOp::Unless, None) => true,
            (MetricBinarySetOp::And | MetricBinarySetOp::Or, None) => false,
            (MetricBinarySetOp::Or, Some(_)) => true,
        },
    );
}
