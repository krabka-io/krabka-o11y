/// A zero-sized type the router names to pick one variant of `E` for a
/// handler that several routes share.
pub(crate) trait RouteVariant<E> {
    const VARIANT: E;
}

/// The Tempo API version a route answers.
pub(crate) enum ApiVersion {
    V1,
    V2,
}

/// How `/api/search` delivers its result.
pub(crate) enum SearchDelivery {
    /// One JSON response once every shard completes.
    Whole,
    /// An NDJSON stream of cumulative responses as shards complete.
    Streamed,
}

/// Which `TraceQL` metrics query a route answers.
pub(crate) enum MetricsQueryKind {
    /// `/api/metrics/query_range`.
    Range,
    /// The single-sample `/api/metrics/query`.
    Instant,
}

/// Routes the Tempo v1 API.
pub(crate) struct V1Api;

impl RouteVariant<ApiVersion> for V1Api {
    const VARIANT: ApiVersion = ApiVersion::V1;
}

/// Routes the Tempo v2 API.
pub(crate) struct V2Api;

impl RouteVariant<ApiVersion> for V2Api {
    const VARIANT: ApiVersion = ApiVersion::V2;
}

/// Routes `/api/search`.
pub(crate) struct WholeSearch;

impl RouteVariant<SearchDelivery> for WholeSearch {
    const VARIANT: SearchDelivery = SearchDelivery::Whole;
}

/// Routes `/api/search/stream`.
pub(crate) struct StreamedSearch;

impl RouteVariant<SearchDelivery> for StreamedSearch {
    const VARIANT: SearchDelivery = SearchDelivery::Streamed;
}

/// Routes `/api/metrics/query_range`.
pub(crate) struct RangeMetrics;

impl RouteVariant<MetricsQueryKind> for RangeMetrics {
    const VARIANT: MetricsQueryKind = MetricsQueryKind::Range;
}

/// Routes `/api/metrics/query`.
pub(crate) struct InstantMetrics;

impl RouteVariant<MetricsQueryKind> for InstantMetrics {
    const VARIANT: MetricsQueryKind = MetricsQueryKind::Instant;
}
