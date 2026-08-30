use super::{
    DistributorError, DistributorState, TimeExt, WalLogRecord, append_wal_records,
    check_ingest_quota,
};

pub(crate) async fn append_distributor_wal_records(
    state: &DistributorState,
    records: Vec<WalLogRecord>,
) -> Result<(), DistributorError> {
    // A quota/rate-limit reject is a 4xx client error, NOT a WAL-append
    // failure, so it must not bump the WAL failure counter.
    check_ingest_quota(state.ingest_limiter.as_ref(), &records).await?;
    let result = if let Some(timeout) = state.wal_append_timeout {
        match tokio::time::timeout(
            timeout.to_std(),
            append_wal_records(state.sink.as_ref(), records),
        )
        .await
        {
            Ok(inner) => inner.map_err(DistributorError::from),
            Err(_) => Err(DistributorError::WalAppendTimeout),
        }
    } else {
        append_wal_records(state.sink.as_ref(), records)
            .await
            .map_err(DistributorError::from)
    };
    // Bump the WAL/produce append-failure counter only at the actual append
    // error site (timeout or sink error), never on a 4xx validation/quota
    // reject handled above or upstream.
    if result.is_err() {
        state.metrics.record_wal_append_failure();
    }
    result
}
