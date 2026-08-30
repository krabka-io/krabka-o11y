use super::{
    ComparisonOp, MetricVectorGroupModifier, MetricVectorMatching, Value,
    apply_metric_binary_comparison_group_right_to_results,
    apply_metric_binary_comparison_to_series, include_metric_group_labels, metric_series_labels,
    metric_vector_group_modifier, metric_vector_matching_key,
};

pub(crate) fn apply_metric_binary_comparison_to_loki_result(
    left: &mut Value,
    right: &Value,
    op: ComparisonOp,
    bool_modifier: bool,
    matching: Option<&MetricVectorMatching>,
) {
    let Some(left_results) = left
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    let Some(right_results) = right.pointer("/data/result").and_then(Value::as_array) else {
        left_results.clear();
        return;
    };

    if let Some(MetricVectorGroupModifier::Right(group_labels)) =
        metric_vector_group_modifier(matching)
    {
        apply_metric_binary_comparison_group_right_to_results(
            left_results,
            right_results,
            op,
            bool_modifier,
            matching,
            group_labels,
        );
        return;
    }

    let mut index = 0;
    while index < left_results.len() {
        let Some(left_labels) = metric_series_labels(&left_results[index]) else {
            left_results.remove(index);
            continue;
        };
        let left_key = metric_vector_matching_key(&left_labels, matching);
        let Some(right_series) = right_results.iter().find(|series| {
            metric_series_labels(series).is_some_and(|right_labels| {
                metric_vector_matching_key(&right_labels, matching) == left_key
            })
        }) else {
            left_results.remove(index);
            continue;
        };

        if apply_metric_binary_comparison_to_series(
            &mut left_results[index],
            right_series,
            op,
            bool_modifier,
        ) {
            if let Some(MetricVectorGroupModifier::Left(group_labels)) =
                metric_vector_group_modifier(matching)
            {
                include_metric_group_labels(&mut left_results[index], right_series, group_labels);
            }
            index += 1;
        } else {
            left_results.remove(index);
        }
    }
}
