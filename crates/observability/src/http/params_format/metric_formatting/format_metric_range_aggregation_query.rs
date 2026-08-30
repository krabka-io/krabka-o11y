use super::*;

pub(crate) fn format_metric_range_aggregation_query(query: &MetricQuery) -> Option<String> {
    let range = format_metric_range_selector(query)?;
    if let RangeAggregation::QuantileOverTime(quantile) = query.aggregation {
        return Some(format!(
            "quantile_over_time({},{range})",
            format_quantile(quantile),
        ));
    }
    Some(format!(
        "{}({range})",
        format_range_aggregation_name(&query.aggregation)?,
    ))
}
