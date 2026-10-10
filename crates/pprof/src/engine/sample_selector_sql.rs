use super::{
    PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION, PCOL_TRACE_ID, PCOL_VALUE,
    SampleSelector,
};

/// The stack-value query over a scan's samples table. A trace selector reads
/// unaggregated rows with their trace id, so the caller can filter them; any
/// other selector sums values per stack, restricted to the selected spans.
pub(crate) fn sample_selector_sql(
    scan: &crate::ProfileScan,
    sample_selector: SampleSelector<'_>,
) -> String {
    let span_where = match sample_selector {
        SampleSelector::Span(ids) => format!(
            " WHERE {PCOL_SPAN_ID} IN ({})",
            ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
        ),
        SampleSelector::None | SampleSelector::Trace(_) => String::new(),
    };
    if matches!(sample_selector, SampleSelector::Trace(_)) {
        format!(
            "SELECT {PCOL_STACKTRACE_PARTITION}, {PCOL_STACKTRACE_ID}, {PCOL_VALUE}, {PCOL_TRACE_ID} \
             FROM {} ORDER BY {PCOL_STACKTRACE_PARTITION}, {PCOL_STACKTRACE_ID}",
            scan.samples_table
        )
    } else {
        format!(
            "SELECT {PCOL_STACKTRACE_PARTITION}, {PCOL_STACKTRACE_ID}, SUM({PCOL_VALUE}) AS v \
             FROM {}{span_where} \
             GROUP BY {PCOL_STACKTRACE_PARTITION}, {PCOL_STACKTRACE_ID} \
             ORDER BY {PCOL_STACKTRACE_PARTITION}, {PCOL_STACKTRACE_ID}",
            scan.samples_table
        )
    }
}
