use super::*;

/// One loaded sample slot.
#[derive(Clone, Debug, PartialEq)]
pub enum SampleSpec {
    /// A concrete float sample.
    Value(f64),
    /// A concrete native histogram sample.
    Histogram(NativeHistogram),
    /// A concrete string result.
    String(String),
    /// Missing sample, written `_`.
    Missing,
    /// Prometheus stale marker, written `stale`.
    Stale,
}
