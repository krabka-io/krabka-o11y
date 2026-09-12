use super::{HttpQueryError, QuerierState, StreamPlan, read_planned_log_block};

pub(crate) async fn count_index_stats_entries(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<u64, HttpQueryError> {
    let mut entries = 0_u64;
    for block in &plan.blocks {
        // A block that is gone contributes no rows to the count. The count
        // is then short by that block, which is what the log line reports.
        let Some(rows) = read_planned_log_block(state, &block.key).await? else {
            continue;
        };
        let matching_entries = rows
            .into_iter()
            .filter(|row| {
                plan.fingerprints.contains(&row.series_fingerprint)
                    && plan.time_range.start_ns <= row.timestamp_ns
                    && row.timestamp_ns <= plan.time_range.end_ns
            })
            .count();
        entries = entries.saturating_add(u64::try_from(matching_entries).unwrap_or(u64::MAX));
    }
    Ok(entries)
}
