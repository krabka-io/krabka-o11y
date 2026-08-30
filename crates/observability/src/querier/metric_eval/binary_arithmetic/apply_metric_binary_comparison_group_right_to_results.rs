use super::*;

pub(crate) fn apply_metric_binary_comparison_group_right_to_results(
    left_results: &mut Vec<Value>,
    right_results: &[Value],
    op: ComparisonOp,
    bool_modifier: bool,
    matching: Option<&MetricVectorMatching>,
    group_labels: &[String],
) {
    let original_left = std::mem::take(left_results);
    for right_series in right_results {
        let Some(right_labels) = metric_series_labels(right_series) else {
            continue;
        };
        let right_key = metric_vector_matching_key(&right_labels, matching);
        let Some(left_series) = original_left.iter().find(|series| {
            metric_series_labels(series)
                .is_some_and(|labels| metric_vector_matching_key(&labels, matching) == right_key)
        }) else {
            continue;
        };
        let mut output_series = right_series.clone();
        if apply_metric_binary_comparison_to_series_with_left_operand(
            &mut output_series,
            left_series,
            op,
            bool_modifier,
        ) {
            include_metric_group_labels(&mut output_series, left_series, group_labels);
            left_results.push(output_series);
        }
    }
}
