use super::{
    ActiveLogDeleteFilterError, BlockStoreError, Error, LogDeleteRequestStoreError,
    LokiRuleStoreError, ParseError, PlanError, QueryAuthorizationError, QueryError, TenantDenied,
    TenantErrorSurface, TenantRequestError,
};

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
