use super::*;

pub(crate) fn format_metric_label_replace_query(query: &str) -> Option<String> {
    let label_replace = parse_metric_label_replace_query(query).ok()?;
    let metric = format_metric_query(&label_replace.query)?;
    Some(format!(
        "label_replace({metric},{},{},{},{})",
        format_logql_quoted_string(&label_replace.destination_label),
        format_logql_quoted_string(&label_replace.replacement),
        format_logql_quoted_string(&label_replace.source_label),
        format_logql_quoted_string(&label_replace.pattern),
    ))
}
