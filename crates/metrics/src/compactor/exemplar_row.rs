use super::WalExemplar;

/// One sorted exemplar sidecar row.
#[derive(Clone, Debug, PartialEq)]
pub struct ExemplarRow {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
    pub value: f64,
    pub trace_id: Option<String>,
    pub span_id: Option<String>,
    pub labels: Vec<krabka_blockstore::LabelPair>,
}

impl crate::numeric_sample_columns::NumericSampleRow for ExemplarRow {
    fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    fn timestamp_ms(&self) -> i64 {
        self.timestamp_ms
    }

    fn value(&self) -> f64 {
        self.value
    }
}

pub(crate) fn exemplar_row(fingerprint: u64, exemplar: &WalExemplar) -> ExemplarRow {
    let mut trace_id = None;
    let mut span_id = None;
    let mut labels = Vec::new();

    for label in &exemplar.labels {
        match label.name.as_str() {
            "trace_id" => trace_id = Some(label.value.clone()),
            "span_id" => span_id = Some(label.value.clone()),
            _ => labels.push(krabka_blockstore::LabelPair::new(
                label.name.clone(),
                label.value.clone(),
            )),
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
