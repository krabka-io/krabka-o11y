use super::*;

#[derive(Debug, Error)]
pub enum WalRecordDecodeError {
    #[error("wal record deserialization failed: {0}")]
    Deserialize(#[from] serde_json::Error),
    #[error("native Kafka log record is missing header {name}")]
    MissingNativeHeader { name: String },
    #[error("native Kafka log record header {name} has no value")]
    MissingNativeHeaderValue { name: String },
    #[error("native Kafka log record header {name} is not UTF-8")]
    InvalidNativeHeaderUtf8 { name: String },
    #[error("native Kafka log record timestamp `{value}` is invalid: {source}")]
    InvalidNativeTimestamp {
        value: String,
        source: std::num::ParseIntError,
    },
    #[error("invalid native Kafka timestamp `{value}`")]
    InvalidNativeTimestampValue { value: String },
    #[error("native Kafka log record value is not UTF-8")]
    InvalidNativeLogLineUtf8,
    #[error("native Kafka log record did not include any krabka-log-label-* headers")]
    MissingNativeLabels,
    #[error("invalid native Kafka label name {name}")]
    InvalidNativeLabelName { name: String },
    #[error("invalid native Kafka metadata name {name}")]
    InvalidNativeMetadataName { name: String },
    #[error("duplicate native Kafka label name {name}")]
    DuplicateNativeLabelName { name: String },
    #[error("duplicate native Kafka metadata name {name}")]
    DuplicateNativeMetadataName { name: String },
}
