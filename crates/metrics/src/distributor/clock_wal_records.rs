use super::{DecodedClockReading, UnixNanos, WalRecord, clock_identity_labels, SamplePayload, ClockReadingPayload};

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
            labels: clock_identity_labels(reading),
            payload: SamplePayload::ClockReading(Box::new(ClockReadingPayload {
                reading: reading.clone(),
                ingest_unix_nanos,
            })),
            exemplars: Vec::new(),
        })
        .collect()
}
