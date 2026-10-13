use super::NativeHistogram;

#[derive(Clone, Debug, PartialEq)]
pub struct HistogramBucket {
    pub upper_bound: f64,
    pub cumulative_count: f64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistogramSummary {
    pub buckets: Vec<HistogramBucket>,
    pub sum: f64,
    pub count: f64,
}

/// Neutral sample shape emitted by metrics-generator processors.
#[derive(Clone, Debug, PartialEq)]
pub enum SeriesSample {
    Counter(f64),
    Gauge(f64),
    ClassicHistogram {
        buckets: Vec<HistogramBucket>,
        sum: f64,
        count: f64,
    },
    NativeHistogram(NativeHistogram),
}
