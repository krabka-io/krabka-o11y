use super::{AsArray, Frame, Int64Type, ProfileError, Tree, UInt64Type, stack_matches_call_sites};

pub(crate) async fn merge_sql_to_tree(
    scan: &crate::ProfileScan,
    sql: &str,
    tree: &mut Tree,
    prefix_frames: &[Frame],
    call_sites: &[String],
) -> Result<(), ProfileError> {
    let batches = scan
        .ctx
        .sql(sql)
        .await
        .map_err(|err| ProfileError::Plan(err.to_string()))?
        .collect()
        .await
        .map_err(|err| ProfileError::Exec(err.to_string()))?;
    for batch in batches {
        let partitions = batch.column(0).as_primitive::<UInt64Type>();
        let stacktrace_ids = batch.column(1).as_primitive::<UInt64Type>();
        let values = batch.column(2).as_primitive::<Int64Type>();
        for row in 0..batch.num_rows() {
            let partition = partitions.value(row);
            let stacktrace_id = u32::try_from(stacktrace_ids.value(row)).map_err(|err| {
                ProfileError::Symbolize(format!("stacktrace id does not fit u32: {err}"))
            })?;
            let mut frames = scan.symbols.resolve(partition, stacktrace_id);
            if call_sites.is_empty() || stack_matches_call_sites(&frames, call_sites) {
                frames.extend_from_slice(prefix_frames);
                tree.add_stack(&frames, values.value(row));
            }
        }
    }
    Ok(())
}
