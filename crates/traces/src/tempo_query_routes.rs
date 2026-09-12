macro_rules! tempo_query_routes {
    (
        $router:expr;
        echo = $echo:expr,
        buildinfo = $buildinfo:expr,
        overrides = $overrides:expr,
        search = $search:expr,
        search_stream = $search_stream:expr,
        trace_v1 = $trace_v1:expr,
        trace_v2 = $trace_v2:expr,
        tags_v1 = $tags_v1:expr,
        tags_v2 = $tags_v2:expr,
        tag_values_v1 = $tag_values_v1:expr,
        tag_values_v2 = $tag_values_v2:expr,
        metrics_range = $metrics_range:expr,
        metrics_instant = $metrics_instant:expr $(,)?
    ) => {
        $router
            .route("/api/echo", $echo)
            .route("/api/status/buildinfo", $buildinfo)
            .route("/api/overrides", $overrides)
            .route("/api/search", $search)
            .route("/api/search/stream", $search_stream)
            .route("/api/traces/{trace_id}", $trace_v1)
            .route("/api/v2/traces/{trace_id}", $trace_v2)
            .route("/api/search/tags", $tags_v1)
            .route("/api/v2/search/tags", $tags_v2)
            .route("/api/search/tag/{tag}/values", $tag_values_v1)
            .route("/api/v2/search/tag/{tag}/values", $tag_values_v2)
            .route("/api/metrics/query_range", $metrics_range)
            .route("/api/metrics/query", $metrics_instant)
    };
}

pub(crate) use tempo_query_routes;
