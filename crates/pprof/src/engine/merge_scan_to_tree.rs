use super::{
    Frame, PCOL_SPAN_ID, PCOL_STACKTRACE_ID, PCOL_STACKTRACE_PARTITION, PCOL_VALUE, ProfileError,
    Tree, merge_sql_to_tree,
};

pub(crate) async fn merge_scan_to_tree(
    scan: &crate::ProfileScan,
    tree: &mut Tree,
    prefix_frames: &[Frame],
    span_ids: Option<&[u64]>,
    call_sites: &[String],
) -> Result<(), ProfileError> {
    let span_where = span_ids.map_or_else(String::new, |ids| {
        format!(
            " WHERE {span} IN ({ids})",
            span = PCOL_SPAN_ID,
            ids = ids.iter().map(u64::to_string).collect::<Vec<_>>().join(",")
        )
    });
    let sql = format!(
        "SELECT {partition}, {stacktrace}, SUM({value}) AS v \
         FROM {table}{span_where} GROUP BY {partition}, {stacktrace} \
         ORDER BY {partition}, {stacktrace}",
        partition = PCOL_STACKTRACE_PARTITION,
        stacktrace = PCOL_STACKTRACE_ID,
        value = PCOL_VALUE,
        table = scan.samples_table,
        span_where = span_where,
    );
    merge_sql_to_tree(scan, &sql, tree, prefix_frames, call_sites).await
}
