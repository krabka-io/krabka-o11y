use super::{NativeHistogram, ResetHint, STALE_NAN_BITS};

/// The histogram that Prometheus reads for a stale marker in a histogram
/// chunk: only the sum is set, to the stale NaN.
pub fn stale_histogram(is_float: bool) -> NativeHistogram {
    NativeHistogram {
        schema: 0,
        is_float,
        reset_hint: ResetHint::Unknown,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: 0.0,
        sum: f64::from_bits(STALE_NAN_BITS),
        positive_spans: Vec::new(),
        positive_counts: Vec::new(),
        negative_spans: Vec::new(),
        negative_counts: Vec::new(),
        custom_values: None,
        start_timestamp_ms: None,
    }
}
