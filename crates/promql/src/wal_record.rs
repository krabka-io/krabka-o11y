use krabka_metrics::{MetricLabelPair, MetricString, SamplePayload, WalRecord};

pub(crate) fn float_wal_record(
    tenant: &(impl ToString + ?Sized),
    labels: impl IntoIterator<Item = (String, MetricString)>,
    timestamp_ms: i64,
    value: f64,
) -> WalRecord {
    WalRecord {
        tenant: tenant.to_string(),
        labels: labels
            .into_iter()
            .map(|(name, value)| MetricLabelPair::new(name, value))
            .collect(),
        payload: SamplePayload::Float {
            timestamp_ms,
            value,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    }
}
