use super::{ScanJob, SpanMatcher};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct ScanOptions {
    pub job: Option<ScanJob>,
    /// Preserve each attribute's original type for dynamic field expressions.
    pub include_raw_attributes: bool,
    pub projection_matchers: Vec<SpanMatcher>,
    /// Periodic metrics sampling fraction; selectors run after sampling.
    pub sample_fraction: Option<f64>,
    /// Sample complete traces when the metrics pipeline needs their spans.
    pub trace_sample_fraction: Option<f64>,
    /// Match pinned Tempo's public metrics frontend label decoding. Raw engine
    /// and per-block worker results retain typed array labels by default.
    pub tempo_frontend_labels: bool,
    /// Pool an inclusive Tempo instant-query window into one metrics bucket.
    /// The supplied step remains the rate denominator; ordinary range scans
    /// retain their existing bucket grid by default.
    pub tempo_instant_metrics: bool,
}
