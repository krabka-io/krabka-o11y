use super::NativeHistogram;

/// Neutral sample shape emitted by metrics-generator processors.
#[derive(Clone, Debug, PartialEq)]
pub enum SeriesSample {
    Counter(f64),
    Gauge(f64),
    ClassicHistogram {
        buckets: Vec<(f64, f64)>,
        sum: f64,
        count: f64,
    },
    NativeHistogram(NativeHistogram),
}
