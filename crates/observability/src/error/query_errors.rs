use crate::{
    ActiveLogDeleteFilterError, BlockStoreError, DataFusionError, Error, IngestLimitError,
    IntoResponse, LogDeleteRequestStoreError, LokiRuleStoreError, ParseError, PlanError,
    QueryAuthorizationError, Response, SeriesFingerprint, StatusCode, TenantErrorSurface,
    TenantRequestError, Value, WalSinkError, json, rfc3339_seconds, server_security::TenantDenied,
    wal_produce::WalBatchError,
};

pub(crate) fn column_number(query: &str, position: usize) -> usize {
    let prefix = &query[..position.min(query.len())];
    prefix
        .rfind('\n')
        .map_or(prefix, |index| &prefix[index + 1..])
        .chars()
        .count()
        + 1
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

pub(crate) fn json_response(status: StatusCode, value: &Value) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(value.to_string().into())
        .expect("JSON response headers are valid")
}

pub(crate) fn line_number(query: &str, position: usize) -> usize {
    query[..position.min(query.len())]
        .bytes()
        .filter(|byte| *byte == b'\n')
        .count()
        + 1
}

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
    Response::builder()
        .status(status)
        .header("content-type", "application/json")
        .body(format!("{{\"status\":\"invalid-query\",\"error\":{error}}}\n").into())
        .expect("JSON response headers are valid")
}

pub(crate) fn loki_gzip_decode_error_text(source: &std::io::Error) -> String {
    let source = source.to_string();
    let message = match source.as_str() {
        "unexpected end of file" => "unexpected EOF",
        other => other,
    };
    format!("{message}\n")
}

pub(crate) fn loki_parse_error(status: StatusCode, query: &str, source: &ParseError) -> Response {
    text_response(status, &loki_parse_error_text(query, source))
}

pub(crate) fn should_omit_expected_logql_token(message: &str, unexpected: &str) -> bool {
    message == "expected '{'" && unexpected == "IDENTIFIER"
}

pub(crate) fn text_response(status: StatusCode, value: &str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", "text/plain; charset=utf-8")
        .body(value.to_string().into())
        .expect("text response headers are valid")
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
    /// `Loki`'s `LineTooLongErrorMsg`, raised by `max_line_size`.
    #[error(
        "Max entry size '{limit_bytes}' bytes exceeded for stream '{stream}' while adding an entry with length '{line_bytes}' bytes\n"
    )]
    LineTooLong {
        stream: String,
        limit_bytes: usize,
        line_bytes: usize,
    },
    /// `Loki`'s `MaxLabelNamesPerSeriesErrorMsg`.
    #[error("entry for stream '{stream}' has {observed} label names; limit {limit}\n")]
    TooManyLabelNames {
        stream: String,
        observed: u64,
        limit: u64,
    },
    /// `Loki`'s `LabelNameTooLongErrorMsg`.
    #[error("stream '{stream}' has label name too long: '{name}'\n")]
    LabelNameTooLong { stream: String, name: String },
    /// `Loki`'s `LabelValueTooLongErrorMsg`.
    #[error("stream '{stream}' has label value too long: '{value}'\n")]
    LabelValueTooLong { stream: String, value: String },
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
    #[error("label name is empty\n")]
    EmptyStructuredMetadataLabelName,
    #[error(
        "stream '{stream}' has structured metadata too large: '{observed}' bytes, limit: '{limit}' bytes\n"
    )]
    StructuredMetadataTooLarge {
        stream: String,
        observed: usize,
        limit: usize,
    },
    #[error(
        "stream '{stream}' has too many structured metadata labels: '{observed}', limit: '{limit}'\n"
    )]
    TooManyStructuredMetadataLabels {
        stream: String,
        observed: usize,
        limit: u64,
    },
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
    /// A normalized record names a tenant other than the one the request
    /// resolved. The distributor never builds such a batch, so this is a
    /// defect, and the push is refused rather than checked as one tenant.
    #[error("ingest batch for tenant `{tenant}` holds a record for tenant `{record_tenant}`")]
    RecordTenantMismatch {
        tenant: String,
        record_tenant: String,
    },
    #[error("wal append timed out")]
    WalAppendTimeout,
    /// A push whose WAL batch appended in part or not at all.
    ///
    /// The error carries how much of the push reached the broker. The status
    /// is 503, so Promtail and Alloy retry the whole push, which is the only
    /// thing they know how to do.
    #[error(transparent)]
    WalBatch(#[from] WalBatchError<WalSinkError>),
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
            Self::WalAppendTimeout | Self::WalBatch(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::RecordTenantMismatch { .. } | Self::EmptyStructuredMetadataLabelName => {
                StatusCode::INTERNAL_SERVER_ERROR
            }
            Self::EmptyStreamLabels
            | Self::LineTooLong { .. }
            | Self::TooManyLabelNames { .. }
            | Self::LabelNameTooLong { .. }
            | Self::LabelValueTooLong { .. }
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
            | Self::StructuredMetadataTooLarge { .. }
            | Self::TooManyStructuredMetadataLabels { .. }
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
                | Self::LineTooLong { .. }
                | Self::TooManyLabelNames { .. }
                | Self::LabelNameTooLong { .. }
                | Self::LabelValueTooLong { .. }
                | Self::StructuredMetadataTooLarge { .. }
                | Self::TooManyStructuredMetadataLabels { .. }
                | Self::NoValidStreams
                | Self::EmptyStructuredMetadataLabelName
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
        | DistributorError::WalBatch(_) => tonic::Status::unavailable(message),
        DistributorError::RecordTenantMismatch { .. }
        | DistributorError::EmptyStructuredMetadataLabelName => tonic::Status::internal(message),
        DistributorError::EmptyStreamLabels
        | DistributorError::LineTooLong { .. }
        | DistributorError::TooManyLabelNames { .. }
        | DistributorError::LabelNameTooLong { .. }
        | DistributorError::LabelValueTooLong { .. }
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
        | DistributorError::StructuredMetadataTooLarge { .. }
        | DistributorError::TooManyStructuredMetadataLabels { .. }
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
    /// The request's `X-Scope-OrgID` does not name the tenant the handler
    /// needs. `surface` picks which of Loki's answers the response copies.
    #[error("{source}")]
    Tenant {
        source: TenantRequestError,
        surface: TenantErrorSurface,
    },
    #[error("missing query parameter `{0}`")]
    MissingQueryParameter(&'static str),
    #[error("query range {range_ns}ns exceeds configured limit {max_range_ns}ns")]
    QueryRangeTooLarge { range_ns: i64, max_range_ns: i64 },
    /// `Loki`'s `ErrQueryTooLong`, raised by the tenant's `max_query_length`.
    ///
    /// The two sides are rendered by two different formatters, because `Loki`
    /// renders them with two different Go types. See
    /// [`format_loki_model_duration`](crate::format_loki_model_duration).
    #[error(
        "the query time range exceeds the limit (query length: {query_length}, limit: {limit})"
    )]
    LokiQueryRangeTooLarge { query_length: String, limit: String },
    #[error(
        "exceeded maximum resolution of 11,000 points per time series. Try increasing the value of the step parameter"
    )]
    QueryResolutionTooHigh,
    #[error("query planned {planned_bytes} bytes, exceeding configured limit {max_bytes}")]
    QueryBytesTooLarge { planned_bytes: u64, max_bytes: u64 },
    #[error("query length {query_bytes} bytes exceeds configured limit {max_bytes}")]
    QueryStringTooLong {
        query_bytes: usize,
        max_bytes: usize,
    },
    /// `Loki`'s `max_entries_limit_per_query`, with `Loki`'s own message.
    #[error(
        "max entries limit per query exceeded, limit > max_entries_limit_per_query ({limit} > {max})"
    )]
    MaxEntriesLimitPerQuery { limit: u64, max: u64 },
    #[error("query matched {series} series, exceeding configured limit {max_series}")]
    QuerySeriesTooLarge { series: usize, max_series: usize },
    #[error("query frontend overloaded; retry after {retry_after_seconds}s")]
    QueryOverloaded { retry_after_seconds: u64 },
    #[error("multi variant queries are disabled for this instance")]
    VariantsDisabled,
    #[error("{0}")]
    VariantUnsupported(String),
    #[error("approx_topk is not enabled. See -limits.shard_aggregations")]
    ApproxTopKDisabled,
    #[error("count min sketches are only supported on instant queries")]
    ApproxTopKRangeQuery,
    #[error("parse error at line 1, col 1: syntax error: unexpected IDENTIFIER")]
    CountValuesQuery,
    #[error("{0}")]
    LokiPlainParse(String),
    #[error("{0}")]
    LokiFormatPlainParse(String),
    #[error(transparent)]
    QueryAuthorization(#[from] QueryAuthorizationError),
    /// The request's principal may not use the tenant it names. The response
    /// is the 403 that [`TenantDenied`] gives.
    #[error(transparent)]
    TenantDenied(#[from] TenantDenied),
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
