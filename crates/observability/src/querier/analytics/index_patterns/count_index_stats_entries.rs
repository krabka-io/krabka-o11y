use super::{
    HttpQueryError, QuerierState, StreamPlan, read_log_block, read_log_block_from_object_store,
};

pub(crate) async fn count_index_stats_entries(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<u64, HttpQueryError> {
    let mut entries = 0_u64;
    for block in &plan.blocks {
        let rows = if let Some(cold_store) = &state.cold_store {
            read_log_block_from_object_store(
                cold_store.store.as_ref(),
                &cold_store.prefix,
                &block.key,
            )
            .await?
        } else {
            read_log_block(&state.root, &block.key)?
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
