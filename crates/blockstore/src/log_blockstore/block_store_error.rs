use super::*;

#[derive(Debug, Error)]
pub enum BlockStoreError {
    #[error(transparent)]
    Arrow(#[from] ArrowError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error("no log blocks were supplied for DataFusion scan")]
    EmptyBlockScan,
    #[error("invalid regex `{pattern}`: {source}")]
    InvalidRegex {
        pattern: String,
        source: regex::Error,
    },
    #[error("invalid block column `{column}`: expected {expected}")]
    InvalidBlockColumn {
        column: &'static str,
        expected: &'static str,
    },
    #[error("invalid time range: start {start_ns} is after end {end_ns}")]
    InvalidTimeRange { start_ns: i64, end_ns: i64 },
    #[error("invalid log index manifest version {actual}; expected {expected}")]
    InvalidManifestVersion { actual: u32, expected: u32 },
    #[error(transparent)]
    Io(#[from] io::Error),
    #[error(transparent)]
    Json(#[from] serde_json::Error),
    #[error("log index manifest fingerprint mismatch: expected {expected}, got {actual}")]
    ManifestFingerprintMismatch {
        expected: SeriesFingerprint,
        actual: SeriesFingerprint,
    },
    #[error("block path is not UTF-8: {path:?}")]
    NonUtf8BlockPath { path: PathBuf },
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    #[error(transparent)]
    Parquet(#[from] ParquetError),
    #[error("row timestamp {timestamp_ns} is outside block time range {start_ns}-{end_ns}")]
    RowOutsideBlockTimeRange {
        timestamp_ns: i64,
        start_ns: i64,
        end_ns: i64,
    },
}
