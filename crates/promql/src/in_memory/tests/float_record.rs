use super::*;

pub(crate) fn float_record(
    tenant: &str,
    labels: &Labels,
    timestamp_ms: i64,
    value: f64,
) -> WalRecord {
    WalRecord {
        tenant: tenant.to_string(),
        labels: labels
            .iter()
            .map(|pair| krabka_metrics::MetricLabelPair::new(pair.name.clone(), pair.value.clone()))
            .collect(),
        payload: SamplePayload::Float {
            timestamp_ms,
            value,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    }
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
