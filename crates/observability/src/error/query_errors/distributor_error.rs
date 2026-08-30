use super::*;

#[derive(Debug, Error)]
pub(crate) enum DistributorError {
    #[error("empty stream labels")]
    EmptyStreamLabels,
    #[error("invalid OTLP attribute")]
    InvalidOtlpAttribute,
    #[error("invalid OTLP payload")]
    InvalidOtlpPayload,
    #[error("ingest body {body_bytes} bytes exceeds configured limit {max_bytes}")]
    IngestBodyTooLarge { body_bytes: usize, max_bytes: usize },
    #[error(transparent)]
    IngestQuota(#[from] IngestLimitError),
    #[error("invalid Loki push value")]
    InvalidPushValue,
    #[error("invalid Loki push labels")]
    InvalidPushLabels,
    #[error("{0}")]
    InvalidPushLabelSyntax(String),
    #[error("{0}")]
    InvalidJsonPushValueSyntax(String),
    #[error("{0}")]
    InvalidJsonLineSyntax(String),
    #[error("{0}")]
    InvalidJsonTimestampSyntax(String),
    #[error("invalid Loki push payload")]
    InvalidPushPayload,
    #[error("error at least one valid stream is required for ingestion\n")]
    NoValidStreams,
    #[error("invalid structured metadata")]
    InvalidStructuredMetadata,
    #[error("{0}")]
    InvalidStructuredMetadataSyntax(String),
    #[error("invalid timestamp")]
    InvalidTimestamp,
    #[error(
        "entry for stream '{stream}' has timestamp too old: {timestamp}, oldest acceptable timestamp is: {oldest}\n",
        timestamp = rfc3339_seconds(*timestamp_ns),
        oldest = rfc3339_seconds(*oldest_acceptable_timestamp_ns),
    )]
    TimestampTooOld {
        stream: String,
        timestamp_ns: i64,
        oldest_acceptable_timestamp_ns: i64,
    },
    #[error(
        "entry for stream '{stream}' has timestamp too old: {timestamp}, oldest acceptable timestamp is: {oldest}\n",
        oldest = rfc3339_seconds(*oldest_acceptable_timestamp_ns),
    )]
    TimestampTooOldString {
        stream: String,
        timestamp: &'static str,
        oldest_acceptable_timestamp_ns: i64,
    },
    #[error(
        "entry for stream '{stream}' has timestamp too new: {timestamp}\n",
        timestamp = rfc3339_seconds(*timestamp_ns),
    )]
    TimestampTooNew { stream: String, timestamp_ns: i64 },
    #[error(transparent)]
    Http(#[from] HttpQueryError),
    #[error("invalid Loki protobuf payload: {0}")]
    LokiDecode(prost::DecodeError),
    #[error("invalid snappy-compressed Loki protobuf payload: {0}")]
    LokiSnappyDecode(snap::Error),
    #[error("invalid gzip-compressed Loki payload: {0}")]
    LokiGzipDecode(std::io::Error),
    #[error("invalid deflate-compressed Loki payload: {0}")]
    LokiDeflateDecode(std::io::Error),
    #[error("Content-Encoding {0:?} not supported")]
    UnsupportedLokiContentEncoding(String),
    #[error("invalid media type {0:?}")]
    InvalidLokiContentType(String),
    #[error("invalid OTLP protobuf payload: {0}")]
    OtlpDecode(prost::DecodeError),
    #[error("wal append timed out")]
    WalAppendTimeout,
    #[error(transparent)]
    WalSink(#[from] WalSinkError),
}

impl IntoResponse for DistributorError {
    fn into_response(self) -> Response {
        if let Self::LokiGzipDecode(source) = &self {
            return text_response(
                StatusCode::BAD_REQUEST,
                &loki_gzip_decode_error_text(source),
            );
        }
        if let Self::LokiDeflateDecode(_) = &self {
            return text_response(StatusCode::BAD_REQUEST, "EOF\n");
        }
        if let Self::UnsupportedLokiContentEncoding(_) = &self {
            return text_response(StatusCode::BAD_REQUEST, &format!("{self}\n"));
        }
        if let Self::LokiSnappyDecode(_) = &self {
            return text_response(StatusCode::BAD_REQUEST, "snappy: corrupt input\n");
        }
        if let Self::LokiDecode(_) = &self {
            return text_response(StatusCode::BAD_REQUEST, "unexpected EOF\n");
        }

        let status = match &self {
            Self::IngestBodyTooLarge { .. }
            | Self::IngestQuota(IngestLimitError::RateLimited { .. }) => {
                StatusCode::TOO_MANY_REQUESTS
            }
            Self::IngestQuota(IngestLimitError::Unauthorized { .. }) => StatusCode::FORBIDDEN,
            Self::IngestQuota(IngestLimitError::Unavailable { .. }) => {
                StatusCode::SERVICE_UNAVAILABLE
            }
            Self::NoValidStreams => StatusCode::UNPROCESSABLE_ENTITY,
            Self::WalAppendTimeout | Self::WalSink(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::EmptyStreamLabels
            | Self::InvalidOtlpAttribute
            | Self::InvalidOtlpPayload
            | Self::InvalidPushLabels
            | Self::InvalidJsonPushValueSyntax(_)
            | Self::InvalidJsonLineSyntax(_)
            | Self::InvalidJsonTimestampSyntax(_)
            | Self::InvalidPushLabelSyntax(_)
            | Self::InvalidPushPayload
            | Self::InvalidPushValue
            | Self::InvalidStructuredMetadata
            | Self::InvalidStructuredMetadataSyntax(_)
            | Self::InvalidTimestamp
            | Self::TimestampTooOld { .. }
            | Self::TimestampTooOldString { .. }
            | Self::TimestampTooNew { .. }
            | Self::Http(_)
            | Self::LokiDecode(_)
            | Self::LokiDeflateDecode(_)
            | Self::LokiGzipDecode(_)
            | Self::LokiSnappyDecode(_)
            | Self::InvalidLokiContentType(_)
            | Self::UnsupportedLokiContentEncoding(_)
            | Self::OtlpDecode(_) => StatusCode::BAD_REQUEST,
        };
        if matches!(
            &self,
            Self::InvalidPushLabelSyntax(_)
                | Self::InvalidJsonLineSyntax(_)
                | Self::InvalidJsonPushValueSyntax(_)
                | Self::InvalidJsonTimestampSyntax(_)
                | Self::InvalidStructuredMetadataSyntax(_)
                | Self::NoValidStreams
                | Self::TimestampTooOld { .. }
                | Self::TimestampTooOldString { .. }
                | Self::TimestampTooNew { .. }
        ) {
            return text_response(status, &self.to_string());
        }
        let error_type = match status {
            StatusCode::BAD_REQUEST => "bad_data",
            StatusCode::FORBIDDEN => "forbidden",
            StatusCode::TOO_MANY_REQUESTS => "rate_limited",
            _ => "server_error",
        };
        loki_error(status, error_type, &self.to_string())
    }
}
