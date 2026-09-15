use super::{
    Arc, CancellationToken, Consumer, LiveStore, RwLock, ServiceMetrics, TracesError,
    ingest_wal_payloads,
};

/// Consume traces WAL records and rebuild the in-memory hot tier.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn run(
    mut consumer: Consumer,
    store: Arc<RwLock<LiveStore>>,
    metrics: ServiceMetrics,
    shutdown: CancellationToken,
) -> Result<(), TracesError> {
    while !shutdown.is_cancelled() {
        let records = consumer
            .poll(krabka_units::millis(500))
            .await
            .inspect_err(|_| metrics.wal_consumer.record_poll_failure())
            .map_err(|err| TracesError::Wal(err.to_string()))?;
        metrics.wal_consumer.record_poll(&records);
        if records.is_empty() {
            continue;
        }

        for record in &records {
            krabka_observability::persisted_format::validate_persisted_format(
                record
                    .headers
                    .iter()
                    .map(|header| (header.key.as_str(), header.value.as_deref())),
            )
            .map_err(|error| TracesError::Wal(error.to_string()))?;
        }

        {
            let payloads = records
                .iter()
                .filter_map(|record| record.value.as_deref())
                .collect::<Vec<_>>();
            let mut guard = store.write().await;
            ingest_wal_payloads(&mut guard, payloads)?;
        }

        if let Err(err) = consumer.commit_sync().await {
            tracing::warn!(error = %err, "live-store offset commit failed");
        }
    }
    Ok(())
}
