use super::*;

/// Consume traces WAL records and rebuild the in-memory hot tier.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub async fn run(
    mut consumer: Consumer,
    store: Arc<RwLock<LiveStore>>,
    shutdown: CancellationToken,
) -> Result<(), TracesError> {
    while !shutdown.is_cancelled() {
        let records = consumer
            .poll(krabka_units::millis(500))
            .await
            .map_err(|err| TracesError::Wal(err.to_string()))?;
        if records.is_empty() {
            continue;
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
