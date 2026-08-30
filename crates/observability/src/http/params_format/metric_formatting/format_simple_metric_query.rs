use super::{MetricQuery, format_metric_range_aggregation_query};

pub(crate) fn format_simple_metric_query(query: &MetricQuery) -> Option<String> {
    if query.vector_aggregation.is_some() || query.range_grouping.is_some() {
        return None;
    }
    format_metric_range_aggregation_query(query)
}
