use super::NativeHistogram;

/// The decoded samples of one chunk, in chunk order.
#[derive(Clone, Debug, PartialEq)]
pub enum ChunkSamples {
    Floats(Vec<(i64, f64)>),
    Histograms(Vec<(i64, NativeHistogram)>),
}
