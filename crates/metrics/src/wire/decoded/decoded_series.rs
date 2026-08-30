use super::{Labels, DecodedSample, NativeHistogram, DecodedExemplar, DecodedMetadata};

/// One decoded metric series from any ingest wire format.
#[derive(Clone, Debug, PartialEq)]
pub struct DecodedSeries {
    pub labels: Labels,
    pub samples: Vec<DecodedSample>,
    pub histograms: Vec<(i64, NativeHistogram)>,
    pub exemplars: Vec<DecodedExemplar>,
    pub metadata: Option<DecodedMetadata>,
}
