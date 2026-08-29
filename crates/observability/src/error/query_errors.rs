use super::*;

pub(crate) fn loki_error(status: StatusCode, error_type: &'static str, error: &str) -> Response {
    let value = json!({
        "status": "error",
        "errorType": error_type,
        "error": error,
        "data": null,
    });
    json_response(status, &value)
}

pub(crate) fn loki_format_query_invalid_response(status: StatusCode, error: &str) -> Response {
    let error = serde_json::to_string(error).expect("string serialization cannot fail");
    (
        status,
        [("content-type", "application/json")],
        format!("{{\"status\":\"invalid-query\",\"error\":{error}}}\n"),
    )
        .into_response()
}

pub(crate) fn loki_parse_error(status: StatusCode, query: &str, source: &ParseError) -> Response {
    text_response(status, &loki_parse_error_text(query, source))
}

pub(crate) fn loki_parse_error_text(query: &str, source: &ParseError) -> String {
    match source {
        ParseError::Syntax { message, position } => {
            let unexpected = unexpected_logql_token(query, *position);
            let prefix = format!(
                "parse error at line {}, col {}: syntax error: unexpected {}",
                line_number(query, *position),
                column_number(query, *position),
                unexpected
            );
            if should_omit_expected_logql_token(message, &unexpected) {
                prefix
            } else {
                format!("{prefix}, expecting {}", expected_logql_token(message))
            }
        }
        ParseError::InvalidRegex { pattern, source } => {
            format!("parse error: invalid regex `{pattern}`: {source}")
        }
    }
}

pub(crate) fn line_number(query: &str, position: usize) -> usize {
    query[..position.min(query.len())]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1
}

pub(crate) fn column_number(query: &str, position: usize) -> usize {
    let prefix = &query[..position.min(query.len())];
    prefix
        .rsplit_once('\n')
        .map_or(prefix, |(_, line)| line)
        .chars()
        .count()
        + 1
}

pub(crate) fn unexpected_logql_token(query: &str, position: usize) -> String {
    let rest = &query[position.min(query.len())..];
    let Some(token) = rest.chars().next() else {
        return "$end".to_string();
    };
    if token == '_' || token.is_ascii_alphabetic() {
        return "IDENTIFIER".to_string();
    }
    token.to_string()
}

pub(crate) fn should_omit_expected_logql_token(message: &str, unexpected: &str) -> bool {
    message == "expected '{'" && unexpected == "IDENTIFIER"
}

pub(crate) fn expected_logql_token(message: &str) -> String {
    match message {
        "expected '\"'" | "expected closing quote" => "STRING".to_string(),
        "expected label matcher operator" => "ASSIGN, EQ, NEQ, RE, NRE".to_string(),
        "expected label name" => "IDENTIFIER".to_string(),
        "expected end of query" => "$end".to_string(),
        _ => message
            .strip_prefix("expected ")
            .unwrap_or(message)
            .to_string(),
    }
}

pub(crate) fn text_response(status: StatusCode, value: &str) -> Response {
    (
        status,
        [("content-type", "text/plain; charset=utf-8")],
        value.to_string(),
    )
        .into_response()
}

pub(crate) fn json_response(status: StatusCode, value: &Value) -> Response {
    (
        status,
        [("content-type", "application/json")],
        value.to_string(),
    )
        .into_response()
}

#[derive(Debug, Error)]
pub enum QueryError {
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error("invalid query column `{column}`: expected {expected}")]
    InvalidColumn {
        column: &'static str,
        expected: &'static str,
    },
    #[error("invalid metric query step {0}")]
    InvalidStep(i64),
    #[error("missing labels for tenant `{tenant}` series fingerprint {fingerprint}")]
    MissingSeriesLabels {
        tenant: String,
        fingerprint: SeriesFingerprint,
    },
    #[error("metric query contains pipeline error `{error}`")]
    MetricPipelineError {
        error: String,
        details: Option<String>,
    },
    #[error(transparent)]
    StructuredMetadata(#[from] serde_json::Error),
}

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

pub(crate) fn loki_gzip_decode_error_text(source: &std::io::Error) -> String {
    let source = source.to_string();
    let message = match source.as_str() {
        "unexpected end of file" => "unexpected EOF",
        other => other,
    };
    format!("{message}\n")
}

pub(crate) fn distributor_error_to_grpc_status(error: &DistributorError) -> tonic::Status {
    let message = error.to_string();
    match error {
        DistributorError::IngestBodyTooLarge { .. }
        | DistributorError::IngestQuota(IngestLimitError::RateLimited { .. }) => {
            tonic::Status::resource_exhausted(message)
        }
        DistributorError::IngestQuota(IngestLimitError::Unauthorized { .. }) => {
            tonic::Status::permission_denied(message)
        }
        DistributorError::IngestQuota(IngestLimitError::Unavailable { .. })
        | DistributorError::WalAppendTimeout
        | DistributorError::WalSink(_) => tonic::Status::unavailable(message),
        DistributorError::EmptyStreamLabels
        | DistributorError::InvalidOtlpAttribute
        | DistributorError::InvalidOtlpPayload
        | DistributorError::InvalidPushLabels
        | DistributorError::InvalidJsonLineSyntax(_)
        | DistributorError::InvalidJsonTimestampSyntax(_)
        | DistributorError::InvalidPushLabelSyntax(_)
        | DistributorError::InvalidPushPayload
        | DistributorError::InvalidPushValue
        | DistributorError::NoValidStreams
        | DistributorError::InvalidJsonPushValueSyntax(_)
        | DistributorError::InvalidStructuredMetadata
        | DistributorError::InvalidStructuredMetadataSyntax(_)
        | DistributorError::InvalidTimestamp
        | DistributorError::TimestampTooOld { .. }
        | DistributorError::TimestampTooOldString { .. }
        | DistributorError::TimestampTooNew { .. }
        | DistributorError::Http(_)
        | DistributorError::LokiDecode(_)
        | DistributorError::LokiDeflateDecode(_)
        | DistributorError::LokiGzipDecode(_)
        | DistributorError::LokiSnappyDecode(_)
        | DistributorError::InvalidLokiContentType(_)
        | DistributorError::UnsupportedLokiContentEncoding(_)
        | DistributorError::OtlpDecode(_) => tonic::Status::invalid_argument(message),
    }
}

#[derive(Debug, Error)]
pub(crate) enum HttpQueryError {
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    #[error(transparent)]
    BlockStore(#[from] BlockStoreError),
    #[error("invalid percent-encoded query parameter")]
    InvalidPercentEncoding,
    #[error("invalid direction '{0}'")]
    InvalidDirection(String),
    #[error("strconv.Atoi: parsing \"{0}\": invalid syntax")]
    InvalidLimit(String),
    #[error("limit must be a positive value")]
    LimitNotPositive,
    #[error(
        "zero or negative query resolution step widths are not accepted. Try a positive integer"
    )]
    InvalidStep,
    #[error("invalid query parameter `{name}` value `{value}`")]
    InvalidQueryParameter { name: &'static str, value: String },
    #[error("delay_for can't be greater than 5")]
    TailDelayForTooLarge,
    #[error("cannot parse \"{value}\" to a valid duration")]
    InvalidDurationQueryParameter { value: String },
    #[error("interval must be >= 0")]
    InvalidInterval,
    #[error("invalid aggregation option")]
    InvalidVolumeAggregation,
    #[error("could not parse 'since' parameter: not a valid duration string: \"{value}\"")]
    InvalidSinceQueryParameter { value: String },
    #[error(
        "could not parse '{name}' parameter: strconv.ParseInt: parsing \"{value}\": invalid syntax"
    )]
    InvalidTimestampQueryParameter { name: &'static str, value: String },
    #[error("invalid tenant header")]
    InvalidTenant,
    #[error("missing query parameter `{0}`")]
    MissingQueryParameter(&'static str),
    #[error("missing X-Scope-OrgID header")]
    MissingTenant,
    #[error("query range {range_ns}ns exceeds configured limit {max_range_ns}ns")]
    QueryRangeTooLarge { range_ns: i64, max_range_ns: i64 },
    #[error("the query time range exceeds the limit (query length: {query_length}, limit: 30d1h)")]
    LokiQueryRangeTooLarge { query_length: String },
    #[error(
        "exceeded maximum resolution of 11,000 points per time series. Try increasing the value of the step parameter"
    )]
    QueryResolutionTooHigh,
    #[error("query planned {planned_bytes} bytes, exceeding configured limit {max_bytes}")]
    QueryBytesTooLarge { planned_bytes: u64, max_bytes: u64 },
    #[error("query length {query_length} bytes exceeds configured limit {max_query_length}")]
    QueryLengthTooLarge {
        query_length: usize,
        max_query_length: usize,
    },
    #[error("query matched {series} series, exceeding configured limit {max_series}")]
    QuerySeriesTooLarge { series: usize, max_series: usize },
    #[error("approx_topk is not enabled. See -limits.shard_aggregations")]
    ApproxTopKDisabled,
    #[error("parse error at line 1, col 1: syntax error: unexpected IDENTIFIER")]
    CountValuesQuery,
    #[error("{0}")]
    LokiPlainParse(String),
    #[error("{0}")]
    LokiFormatPlainParse(String),
    #[error(transparent)]
    QueryAuthorization(#[from] QueryAuthorizationError),
    #[error("{source}")]
    LokiParse { query: String, source: ParseError },
    #[error("{source}")]
    LokiFormatParse { query: String, source: ParseError },
    #[error("missing query parameter `query`")]
    LokiFormatMissingQuery,
    #[error("cannot encode Loki query result as parquet: {0}")]
    LokiParquet(&'static str),
    #[error(transparent)]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error(transparent)]
    Parse(#[from] ParseError),
    #[error(transparent)]
    Plan(#[from] PlanError),
    #[error(transparent)]
    Query(#[from] QueryError),
    #[error(transparent)]
    DeleteRequests(#[from] LogDeleteRequestStoreError),
    #[error(transparent)]
    Rules(#[from] LokiRuleStoreError),
    #[error(transparent)]
    DeleteFilter(#[from] ActiveLogDeleteFilterError),
}
