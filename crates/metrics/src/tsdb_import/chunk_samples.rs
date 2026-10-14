use super::NativeHistogram;

/// A float sample decoded from a TSDB chunk.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TimestampedFloatSample {
    pub timestamp_ms: i64,
    pub value: f64,
}

/// A histogram sample decoded from a TSDB chunk.
#[derive(Clone, Debug, PartialEq)]
pub struct TimestampedHistogramSample {
    pub timestamp_ms: i64,
    pub histogram: NativeHistogram,
}

/// The decoded samples of one chunk, in chunk order.
#[derive(Clone, Debug, PartialEq)]
pub enum ChunkSamples {
    Floats(Vec<TimestampedFloatSample>),
    Histograms(Vec<TimestampedHistogramSample>),
}
