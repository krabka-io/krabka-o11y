use super::*;

/// Gates a clock batch and appends both the clock block records and the
/// projected float records.
pub(crate) async fn append_clock_readings(
    state: &DistributorState,
    tenant: &TenantId,
    readings: &[DecodedClockReading],
    ingest_unix_nanos: UnixNanos,
) -> Result<bool, PushError> {
    let uncertainty_ms = readings
        .iter()
        .map(|reading| reading.uncertainty_nanos.saturating_add(999_999) / 1_000_000)
        .max()
        .unwrap_or_default();
    let clock_uncertainty = Time::from_millis(uncertainty_ms);
    let mut series = clock_series(readings, ingest_unix_nanos);
    if !enforce_ingest_limits(state, tenant, &mut series, clock_uncertainty).await? {
        return Ok(false);
    }

    let mut records = clock_wal_records(tenant.as_str(), readings, ingest_unix_nanos);
    records.extend(wal_records_from_series(tenant.as_str(), &series));
    append_wal_records(state, tenant, records).await?;
    state.series_tracker.record_clock_uncertainty(
        tenant,
        clock_uncertainty,
        state.limits_for_tenant(tenant).active_series_idle_timeout,
        state.clock.now(),
    );
    Ok(true)
}

pub(crate) async fn append_decoded_series(
    state: &DistributorState,
    tenant: &TenantId,
    series: &mut [DecodedSeries],
) -> Result<bool, PushError> {
    if !enforce_ingest_limits(state, tenant, series, Time::ZERO).await? {
        return Ok(false);
    }
    append_wal_records(
        state,
        tenant,
        wal_records_from_series(tenant.as_str(), series),
    )
    .await?;
    Ok(true)
}

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
        .map(|record| WalBatchRecord {
            key: partition_key(tenant.as_str(), record.series_fingerprint()),
            record,
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
                metrics.ingest.wal_append_failures.inc();
                metrics
                    .wal_produce
                    .record_batch_failure(error.appended(), error.total());
            }
            Err(PushError::ProduceBatch(error))
        }
    }
}

/// Builds the clock block WAL records, one per reading.
///
/// The record rides the ordinary [`WalRecord`] envelope, with the node and
/// clock identity in its labels, so fingerprinting, partitioning, and tenancy
/// work on it exactly as they do on a float sample.
#[must_use]
pub fn clock_wal_records(
    tenant: &str,
    readings: &[DecodedClockReading],
    ingest_unix_nanos: UnixNanos,
) -> Vec<WalRecord> {
    readings
        .iter()
        .map(|reading| WalRecord {
            tenant: tenant.to_string(),
            labels: clock_identity_labels(reading)
                .into_iter()
                .map(|pair| crate::MetricLabelPair::new(pair.name, pair.value.into()))
                .collect(),
            payload: SamplePayload::ClockReading(Box::new(ClockReadingPayload {
                reading: reading.clone(),
                ingest_unix_nanos,
            })),
            exemplars: Vec::new(),
        })
        .collect()
}

pub(crate) fn label_pairs(
    series: &DecodedSeries,
) -> Vec<crate::MetricLabelPair<crate::MetricString>> {
    series
        .labels
        .iter()
        .map(|pair| crate::MetricLabelPair::new(pair.name, pair.value.into()))
        .collect()
}

/// Fans the decoded series into one WAL record per float sample or native-
/// histogram sample.
#[must_use]
pub fn wal_records_from_series(tenant: &str, series: &[DecodedSeries]) -> Vec<WalRecord> {
    let mut out = Vec::new();
    for series in series {
        let labels = label_pairs(series);
        let exemplars = series
            .exemplars
            .iter()
            .map(|exemplar| WalExemplar {
                labels: exemplar
                    .labels
                    .iter()
                    .map(|label| {
                        crate::MetricLabelPair::new(label.name.to_owned(), label.value.to_owned())
                    })
                    .collect(),
                value: exemplar.value,
                timestamp_ms: exemplar.timestamp_ms,
            })
            .collect::<Vec<_>>();

        out.extend(series.samples.iter().map(|sample| WalRecord {
            tenant: tenant.to_string(),
            labels: labels.clone(),
            payload: SamplePayload::Float {
                timestamp_ms: sample.timestamp_ms,
                value: sample.value,
                start_timestamp_ms: sample.start_timestamp_ms,
            },
            exemplars: Vec::new(),
        }));
        out.extend(series.histograms.iter().map(|sample| WalRecord {
            tenant: tenant.to_string(),
            labels: labels.clone(),
            payload: SamplePayload::Hist {
                timestamp_ms: sample.timestamp_ms,
                hist: sample.histogram.clone(),
            },
            exemplars: Vec::new(),
        }));
        if let Some(metadata) = &series.metadata {
            // Classic histogram and summary samples carry suffixed names;
            // metadata is indexed by the unsuffixed metric family.
            let mut metadata_labels = series.labels.clone();
            metadata_labels.insert("__name__", metadata.metric_family_name.as_str());
            out.push(WalRecord {
                tenant: tenant.to_string(),
                labels: metadata_labels
                    .iter()
                    .map(|pair| crate::MetricLabelPair::new(pair.name, pair.value.into()))
                    .collect(),
                payload: SamplePayload::Metadata {
                    metric_family_name: metadata.metric_family_name.clone(),
                    metric_type: metadata.metric_type.clone(),
                    help: metadata.help.clone(),
                    unit: metadata.unit.clone(),
                },
                exemplars: Vec::new(),
            });
        }
        if !exemplars.is_empty() {
            out.push(WalRecord {
                tenant: tenant.to_string(),
                labels,
                payload: SamplePayload::Exemplars,
                exemplars,
            });
        }
    }
    out
}
