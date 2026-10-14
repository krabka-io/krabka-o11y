use super::{DecodedExemplar, DecodedMetadata, DecodedSample, Labels, NativeHistogram};

/// One native histogram sample in a decoded series.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedHistogramSample {
    pub timestamp_ms: i64,
    pub histogram: NativeHistogram,
}

/// One decoded metric series from any ingest wire format.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedSeries {
    pub labels: Labels,
    pub samples: Vec<DecodedSample>,
    pub histograms: Vec<DecodedHistogramSample>,
    pub exemplars: Vec<DecodedExemplar>,
    pub metadata: Option<DecodedMetadata>,
}
