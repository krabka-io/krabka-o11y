use super::*;

pub(crate) fn stream_plan_scan_sql_for_time_range(
    plan: &StreamPlan,
    time_range: TimeRange,
) -> String {
    let mut predicates = vec![format!(
        "timestamp_ns >= {} and timestamp_ns <= {}",
        time_range.start_ns, time_range.end_ns
    )];
    if !plan.fingerprints.is_empty() {
        let fingerprints = plan
            .fingerprints
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        predicates.push(format!("series_fingerprint in ({fingerprints})"));
    }
    predicates.extend(literal_line_filter_sql_predicates(&plan.query.pipeline));
    format!(
        "select series_fingerprint, timestamp_ns, line, structured_metadata \
         from logs \
         where {} \
         order by series_fingerprint, timestamp_ns",
        predicates.join(" and ")
    )
}
