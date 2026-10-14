use super::{Series, SeriesSample};

pub(crate) fn counter(
    name: &str,
    labels: &[krabka_traceql::MetricLabel],
    value: f64,
    timestamp_ms: i64,
) -> Series {
    Series {
        name: name.to_string(),
        labels: labels.to_vec(),
        sample: SeriesSample::Counter(value),
        exemplars: Vec::new(),
        timestamp_ms,
    }
}
