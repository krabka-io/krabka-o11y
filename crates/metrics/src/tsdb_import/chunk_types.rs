use super::*;

/// The sample encodings that a Prometheus chunk can declare.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ChunkEncoding {
    Xor,
    Histogram,
    FloatHistogram,
}

impl ChunkEncoding {
    /// Maps the chunk's encoding byte. Prometheus reserves the other values,
    /// and the import refuses them rather than guessing a layout.
    pub fn from_byte(byte: u8) -> Option<Self> {
        match byte {
            1 => Some(Self::Xor),
            2 => Some(Self::Histogram),
            3 => Some(Self::FloatHistogram),
            _ => None,
        }
    }
}

/// Why one chunk's data did not decode.
///
/// The caller knows which series the chunk belongs to and turns this into a
/// [`TsdbImportError`](super::TsdbImportError) that names it.
#[derive(Debug, PartialEq)]
pub enum ChunkError {
    Truncated,
    Invalid(String),
    Schema(i64),
    TooManyBuckets(u64),
}

impl std::fmt::Display for ChunkError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => {
                formatter.write_str("chunk data ends early or holds an invalid bit sequence")
            }
            Self::Invalid(reason) => formatter.write_str(reason),
            Self::Schema(schema) => write!(formatter, "histogram schema {schema}"),
            Self::TooManyBuckets(count) => write!(formatter, "{count} histogram buckets"),
        }
    }
}

/// One chunk that the index lists for a series.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ChunkMeta {
    pub min_time: i64,
    pub max_time: i64,
    pub reference: u64,
}

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
