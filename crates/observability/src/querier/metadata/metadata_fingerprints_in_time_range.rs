use super::{
    BTreeSet, HttpQueryError, QuerierState, SeriesFingerprint, TimeRange, read_planned_log_block,
};

pub(crate) async fn metadata_fingerprints_in_time_range(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
) -> Result<BTreeSet<SeriesFingerprint>, HttpQueryError> {
    let mut fingerprints = BTreeSet::new();
    for block in state.block_index.match_blocks(tenant, time_range, &[]) {
        // The index still records which series the block held, so a block
        // that is gone degrades to a coarser answer rather than no answer.
        // The fallback cannot filter by time, because the rows it would
        // have read are what carried the timestamps.
        let Some(rows) = read_planned_log_block(state, &block.key).await? else {
            fingerprints.extend(block.fingerprints);
            continue;
        };
        fingerprints.extend(rows.into_iter().filter_map(|row| {
            (time_range.start_ns <= row.timestamp_ns && row.timestamp_ns <= time_range.end_ns)
                .then_some(row.series_fingerprint)
        }));
    }
    Ok(fingerprints)
}
