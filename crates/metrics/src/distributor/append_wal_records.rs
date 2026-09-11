use super::{DistributorState, PushError, TenantId, WalRecord, partition_key};

/// Appends already-gated records to the WAL as one pipelined batch.
///
/// The records keep their argument order on the wire, so the samples of one
/// series stay ordered on the partition their key selects.
pub(crate) async fn append_wal_records(
    state: &DistributorState,
    tenant: &TenantId,
    records: Vec<WalRecord>,
) -> Result<(), PushError> {
    let keyed: Vec<_> = records
        .into_iter()
        .map(|record| {
            (
                partition_key(tenant.as_str(), record.series_fingerprint()),
                record,
            )
        })
        .collect();
    match state.sink.append_batch(keyed).await {
        Ok(()) => Ok(()),
        Err(error) => {
            // The actual WAL/produce error site — count it distinctly from
            // 4xx client/validation rejects so operators can alert on durable
            // append failures via rate(wal_append_failures_total). A batch that
            // appended some of its records is counted a second time, because a
            // retry of the whole body then duplicates what did land.
            if let Some(metrics) = &state.metrics {
                metrics.wal_append_failures.inc();
                metrics
                    .wal_produce
                    .record_batch_failure(error.appended(), error.total());
            }
            Err(PushError::ProduceBatch(error))
        }
    }
}
