use super::{
    MetricQuery, format_metric_range_aggregation_query, format_vector_aggregation_query,
    format_vector_grouping,
};

pub(crate) fn format_metric_query(query: &MetricQuery) -> Option<String> {
    let mut formatted = format_metric_range_aggregation_query(query)?;
    if let Some(grouping) = &query.range_grouping {
        formatted = format!("{formatted} {}", format_vector_grouping(grouping));
    }
    if let Some(vector_aggregation) = &query.vector_aggregation {
        formatted = format_vector_aggregation_query(vector_aggregation, &formatted)?;
    }
    Some(formatted)
}
