use super::{DistributorState, WalRecord, PushError, partition_key};

/// Appends already-gated records to the WAL, one produce per record.
pub(crate) async fn append_wal_records(
    state: &DistributorState,
    tenant: &str,
    records: Vec<WalRecord>,
) -> Result<(), PushError> {
    for record in records {
        let key = partition_key(tenant, record.series_fingerprint());
        if let Err(error) = state.sink.append(key, record).await {
            // The actual WAL/produce error site — count it distinctly from
            // 4xx client/validation rejects so operators can alert on durable
            // append failures via rate(wal_append_failures_total).
            if let Some(metrics) = &state.metrics {
                metrics.wal_append_failures.inc();
            }
            return Err(error.into());
        }
    }
    Ok(())
}
