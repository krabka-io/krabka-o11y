use super::{FormattedMetricSeries, Ordering, VectorAggregationOp, parse_metric_sample_value};

pub(crate) fn sort_formatted_vector_samples(
    series: &mut FormattedMetricSeries,
    op: &VectorAggregationOp,
) {
    match op {
        VectorAggregationOp::Sort | VectorAggregationOp::SortDesc => {
            series.sort_by(|left, right| {
                let left_value = left
                    .1
                    .first()
                    .and_then(|sample| parse_metric_sample_value(&sample[1]))
                    .unwrap_or_default();
                let right_value = right
                    .1
                    .first()
                    .and_then(|sample| parse_metric_sample_value(&sample[1]))
                    .unwrap_or_default();
                let value_order = match op {
                    VectorAggregationOp::Sort => left_value.cmp_value(right_value),
                    VectorAggregationOp::SortDesc => right_value.cmp_value(left_value),
                    _ => Ordering::Equal,
                };
                value_order.then_with(|| left.0.cmp(&right.0))
            });
        }
        _ => {}
    }
}
