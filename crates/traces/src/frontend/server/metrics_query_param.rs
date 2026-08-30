use super::{Uri, query_param};

/// The `TraceQL` metrics query string.
///
/// Tempo accepts both `q` and `query` on the metrics endpoints. The Explore
/// `TraceQL` editor and the HTTP API send `q`. The Grafana Tempo datasource
/// that powers the Traces Drilldown app sends `query`. This accepts either, and
/// prefers `q`.
pub(crate) fn metrics_query_param(uri: &Uri) -> Option<String> {
    query_param(uri, "q").or_else(|| query_param(uri, "query"))
}
