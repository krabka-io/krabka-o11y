use super::*;

pub(crate) async fn metadata_fingerprints_in_time_range(
    state: &QuerierState,
    tenant: &str,
    time_range: TimeRange,
) -> Result<BTreeSet<SeriesFingerprint>, HttpQueryError> {
    let mut fingerprints = BTreeSet::new();
    for block in state.block_index.match_blocks(tenant, time_range, &[]) {
        let rows = if let Some(cold_store) = &state.cold_store {
            read_log_block_from_object_store(
                cold_store.store.as_ref(),
                &cold_store.prefix,
                &block.key,
            )
            .await?
        } else {
            match read_log_block(&state.root, &block.key) {
                Ok(rows) => rows,
                Err(BlockStoreError::Io(source)) if source.kind() == ErrorKind::NotFound => {
                    fingerprints.extend(block.fingerprints);
                    continue;
                }
                Err(error) => return Err(error.into()),
            }
        };
        fingerprints.extend(rows.into_iter().filter_map(|row| {
            (time_range.start_ns <= row.timestamp_ns && row.timestamp_ns <= time_range.end_ns)
                .then_some(row.series_fingerprint)
        }));
    }
    Ok(fingerprints)
}
