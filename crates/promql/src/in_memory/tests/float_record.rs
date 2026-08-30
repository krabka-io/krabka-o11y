use super::*;

pub(crate) fn float_record(tenant: &str, labels: &Labels, timestamp_ms: i64, value: f64) -> WalRecord {
    WalRecord {
        tenant: tenant.to_string(),
        labels: labels
            .iter()
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        payload: SamplePayload::Float {
            timestamp_ms,
            value,
            start_timestamp_ms: None,
        },
        exemplars: Vec::new(),
    }
}
