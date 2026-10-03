/// Errors that stop a Prometheus TSDB block import before it writes anything.
///
/// The decoder validates the whole block in memory, so each variant names the
/// first problem it found and no object is published for the block.
#[derive(Debug, thiserror::Error, PartialEq)]
#[non_exhaustive]
pub enum TsdbImportError {
    /// A section ends before the bytes that its header or layout promises.
    #[error("{section} is truncated")]
    Truncated { section: &'static str },

    #[error("{file} has magic {found:#010x}, expected {expected:#010x}")]
    BadMagic {
        file: &'static str,
        found: u32,
        expected: u32,
    },

    #[error("checksum mismatch in {section}")]
    Checksum { section: String },

    #[error(
        "index format version {0} is not supported; import accepts versions 2 and 3, rewrite \
         older blocks with a current Prometheus first"
    )]
    UnsupportedIndexVersion(u8),

    #[error("chunk segment {segment} has format version {version}, expected 1")]
    UnsupportedChunkFormat { segment: usize, version: u8 },

    #[error("tombstones format version {0} is not supported, expected 1")]
    UnsupportedTombstonesVersion(u8),

    #[error("chunk at reference {reference:#x} has unknown encoding {encoding}")]
    UnknownChunkEncoding { reference: u64, encoding: u8 },

    #[error("chunk reference {reference:#x} is outside the uploaded chunk segments")]
    ChunkReference { reference: u64 },

    #[error("native histogram schema {0} is not supported")]
    UnsupportedHistogramSchema(i64),

    #[error("invalid native histogram: {0}")]
    InvalidHistogram(String),

    #[error("invalid index: {0}")]
    InvalidIndex(String),

    #[error("invalid series labels: {0}")]
    InvalidLabels(String),

    #[error("series {labels} is out of order or duplicated in the index")]
    SeriesOrder { labels: String },

    #[error("series {labels} has an invalid chunk: {reason}")]
    InvalidChunk { labels: String, reason: String },

    #[error("series {labels} has sample {timestamp} at or before sample {previous}")]
    SampleOrder {
        labels: String,
        previous: i64,
        timestamp: i64,
    },

    #[error(
        "series {labels} has timestamp {timestamp} outside the block range [{min_time}, \
         {max_time})"
    )]
    OutOfBounds {
        labels: String,
        timestamp: i64,
        min_time: i64,
        max_time: i64,
    },

    #[error("block external label {label}={found:?} does not match tenant {expected:?}")]
    TenantMismatch {
        label: String,
        expected: String,
        found: String,
    },

    #[error("block exceeds the import limit {limit}: {value} > {max}")]
    LimitExceeded {
        limit: &'static str,
        value: u64,
        max: u64,
    },

    #[error("invalid block metadata: {0}")]
    InvalidMeta(String),

    #[error("block contains no samples")]
    Empty,
}
