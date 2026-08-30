use super::WalExemplar;

/// One sorted exemplar sidecar row.
#[derive(Clone, Debug, PartialEq)]
pub struct ExemplarRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub value: f64,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub labels: Vec<(String, String)>,
}

pub(crate) fn exemplar_row(fingerprint: u64, exemplar: &WalExemplar) -> ExemplarRow {
    let mut trace_id = None;
    let mut span_id = None;
    let mut labels = Vec::new();

    for (name, value) in &exemplar.labels {
        match name.as_str() {
            "trace_id" => trace_id = Some(value.clone()),
            "span_id" => span_id = Some(value.clone()),
            _ => labels.push((name.clone(), value.clone())),
        }
    }

    ExemplarRow {
        fingerprint,
        timestamp_ms: exemplar.timestamp_ms,
        value: exemplar.value,
        trace_id,
        span_id,
        labels,
    }
}
