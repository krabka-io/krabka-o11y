use super::{
    Frame, PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION, PCOL_TRACE_ID, PCOL_VALUE,
    ProfileError, SampleSelector, Tree, merge_sql_to_tree,
};

pub(crate) async fn merge_scan_to_tree(
    scan: &crate::ProfileScan,
    tree: &mut Tree,
    prefix_frames: &[Frame],
    sample_selector: SampleSelector<'_>,
    call_sites: &[String],
) -> Result<(), ProfileError> {
    let span_where = match sample_selector {
        SampleSelector::Span(ids) => format!(
            " WHERE {span} IN ({ids})",
            span = PCOL_SPAN_ID,
            ids = ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
        ),
        SampleSelector::None | SampleSelector::Trace(_) => String::new(),
    };
    let sql = if matches!(sample_selector, SampleSelector::Trace(_)) {
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
    };
    merge_sql_to_tree(
        scan,
        &sql,
        tree,
        prefix_frames,
        call_sites,
        match sample_selector {
            SampleSelector::Trace(ids) => Some(ids),
            SampleSelector::None | SampleSelector::Span(_) => None,
        },
    )
    .await
}
