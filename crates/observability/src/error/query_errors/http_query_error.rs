use super::*;

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
