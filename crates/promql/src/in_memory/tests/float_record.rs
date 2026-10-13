use super::*;

pub(crate) fn float_record(
    tenant: &str,
    labels: &Labels,
    timestamp_ms: i64,
    value: f64,
) -> WalRecord {
    crate::wal_record::float_wal_record(
        tenant,
        labels
            .iter()
            .map(|pair| (pair.name.to_owned(), pair.value.to_owned().into())),
        timestamp_ms,
        value,
    )
}

pub(crate) fn float_sample_row(
    fingerprint: u64,
    timestamp_ms: i64,
    value: f64,
    start_timestamp_ms: Option<i64>,
) -> krabka_metrics::FloatSampleRow {
    krabka_metrics::FloatSampleRow {
        fingerprint,
        timestamp_ms,
        value,
        start_timestamp_ms,
    }
}
