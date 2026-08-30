use super::{
    MetricQuery, format_loki_duration_ns, format_loki_offset_duration_ns, format_stream_query,
};

pub(crate) fn format_metric_range_selector(query: &MetricQuery) -> Option<String> {
    let range = format_loki_duration_ns(query.range_ns.0)?;
    let offset = if query.offset_ns.0 == 0 {
        String::new()
    } else {
        let sign = if query.offset_ns.0 < 0 { "-" } else { "" };
        let duration = format_loki_offset_duration_ns(query.offset_ns.0.checked_abs()?)?;
        format!(" offset {sign}{duration}")
    };
    Some(format!(
        "{}[{range}]{offset}",
        format_stream_query(&query.stream)
    ))
}
