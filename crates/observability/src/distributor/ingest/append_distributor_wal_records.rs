use super::{DistributorError, DistributorState, TimeExt, WalLogRecord, check_ingest_quota};

pub(crate) async fn append_distributor_wal_records(
    state: &DistributorState,
    records: Vec<WalLogRecord>,
) -> Result<(), DistributorError> {
    // A quota/rate-limit reject is a 4xx client error, NOT a WAL-append
    // failure, so it must not bump the WAL failure counter.
    check_ingest_quota(state.ingest_limiter.as_ref(), &records).await?;
    let total = records.len();
    // One pipelined batch, not one produce per entry. The sink enqueues the
    // records in this order, so a stream's entries stay ordered on the
    // partition its key selects.
    let appended = state.sink.append_batch(records);
    let result = if let Some(timeout) = state.wal_append_timeout {
        if let Ok(inner) = tokio::time::timeout(timeout.to_std(), appended).await {
            inner.map_err(DistributorError::from)
        } else {
            // The batch was cancelled, so its outcome is unknown. The producer
            // may still deliver what it holds.
            state.metrics.wal_produce.record_batch_abandoned(total);
            Err(DistributorError::WalAppendTimeout)
        }
    } else {
        appended.await.map_err(DistributorError::from)
    };
    // Bump the WAL/produce append-failure counter only at the actual append
    // error site (timeout or sink error), never on a 4xx validation/quota
    // reject handled above or upstream.
    if let Err(error) = &result {
        state.metrics.record_wal_append_failure();
        if let DistributorError::WalBatch(batch) = error {
            // Logs have no query-time deduplication, so a retry of a push that
            // appended in part writes those entries a second time, and the
            // second copy is permanent.
            state
                .metrics
                .wal_produce
                .record_batch_failure(batch.appended(), batch.total());
        }
    }
    result
}
