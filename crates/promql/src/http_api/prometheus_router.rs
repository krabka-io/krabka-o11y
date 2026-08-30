use super::{
    Arc, ByteSizeExt, DefaultBodyLimit, MetricStore, PrometheusApiState, Router, alertmanagers,
    alerts, build_info, cardinality_active_series, cardinality_active_series_post,
    cardinality_label_names, cardinality_label_names_post, cardinality_label_values,
    cardinality_label_values_post, delete_ruler_config_group, delete_ruler_config_namespace,
    format_query, format_query_post, get, label_values, label_values_post, labels, labels_post,
    metadata, parse_query, parse_query_post, post, query, query_exemplars, query_exemplars_post,
    query_post, query_range, query_range_post, remote_read, ruler_config_group,
    ruler_config_namespace, ruler_config_rules, rules, runtime_info, scrape_pools, series,
    series_post, set_ruler_config_group, status_config, status_flags, target_metadata, targets,
    tsdb_blocks, tsdb_status, wal_replay_status,
};

/// Builds the routes for the Prometheus API and the `/prometheus` prefix of Mimir.
pub fn prometheus_router<S: MetricStore + 'static>(state: Arc<PrometheusApiState<S>>) -> Router {
    let remote_read_max_body = state.remote_read_max_body.bytes_usize();
    Router::new()
        .route("/api/v1/query", get(query::<S>).post(query_post::<S>))
        .route(
            "/api/v1/query_range",
            get(query_range::<S>).post(query_range_post::<S>),
        )
        .route(
            "/api/v1/query_exemplars",
            get(query_exemplars::<S>).post(query_exemplars_post::<S>),
        )
        .route(
            "/api/v1/read",
            post(remote_read::<S>).layer(DefaultBodyLimit::max(remote_read_max_body)),
        )
        .route(
            "/api/v1/cardinality/label_names",
            get(cardinality_label_names::<S>).post(cardinality_label_names_post::<S>),
        )
        .route(
            "/api/v1/cardinality/label_values",
            get(cardinality_label_values::<S>).post(cardinality_label_values_post::<S>),
        )
        .route(
            "/api/v1/cardinality/active_series",
            get(cardinality_active_series::<S>).post(cardinality_active_series_post::<S>),
        )
        .route("/api/v1/series", get(series::<S>).post(series_post::<S>))
        .route("/api/v1/labels", get(labels::<S>).post(labels_post::<S>))
        .route(
            "/api/v1/label/{name}/values",
            get(label_values::<S>).post(label_values_post::<S>),
        )
        .route("/api/v1/metadata", get(metadata::<S>))
        .route("/api/v1/rules", get(rules::<S>))
        .route("/api/v1/alerts", get(alerts::<S>))
        .route("/api/v1/alertmanagers", get(alertmanagers))
        .route("/api/v1/targets", get(targets))
        .route("/api/v1/targets/metadata", get(target_metadata::<S>))
        .route("/api/v1/scrape_pools", get(scrape_pools))
        .route(
            "/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route(
            "/api/v1/parse_query",
            get(parse_query).post(parse_query_post),
        )
        .route("/api/v1/status/buildinfo", get(build_info))
        .route("/api/v1/status/config", get(status_config))
        .route("/api/v1/status/flags", get(status_flags::<S>))
        .route("/api/v1/status/runtimeinfo", get(runtime_info::<S>))
        .route("/api/v1/status/tsdb", get(tsdb_status::<S>))
        .route("/api/v1/status/tsdb/blocks", get(tsdb_blocks::<S>))
        .route("/api/v1/status/walreplay", get(wal_replay_status))
        .route(
            "/prometheus/api/v1/query",
            get(query::<S>).post(query_post::<S>),
        )
        .route(
            "/prometheus/api/v1/query_range",
            get(query_range::<S>).post(query_range_post::<S>),
        )
        .route(
            "/prometheus/api/v1/query_exemplars",
            get(query_exemplars::<S>).post(query_exemplars_post::<S>),
        )
        .route(
            "/prometheus/api/v1/read",
            post(remote_read::<S>).layer(DefaultBodyLimit::max(remote_read_max_body)),
        )
        .route(
            "/prometheus/api/v1/cardinality/label_names",
            get(cardinality_label_names::<S>).post(cardinality_label_names_post::<S>),
        )
        .route(
            "/prometheus/api/v1/cardinality/label_values",
            get(cardinality_label_values::<S>).post(cardinality_label_values_post::<S>),
        )
        .route(
            "/prometheus/api/v1/cardinality/active_series",
            get(cardinality_active_series::<S>).post(cardinality_active_series_post::<S>),
        )
        .route(
            "/prometheus/api/v1/series",
            get(series::<S>).post(series_post::<S>),
        )
        .route(
            "/prometheus/api/v1/labels",
            get(labels::<S>).post(labels_post::<S>),
        )
        .route(
            "/prometheus/api/v1/label/{name}/values",
            get(label_values::<S>).post(label_values_post::<S>),
        )
        .route("/prometheus/api/v1/metadata", get(metadata::<S>))
        .route("/prometheus/api/v1/rules", get(rules::<S>))
        .route("/prometheus/api/v1/alerts", get(alerts::<S>))
        .route("/prometheus/api/v1/alertmanagers", get(alertmanagers))
        .route("/prometheus/api/v1/targets", get(targets))
        .route(
            "/prometheus/api/v1/targets/metadata",
            get(target_metadata::<S>),
        )
        .route("/prometheus/api/v1/scrape_pools", get(scrape_pools))
        .route("/prometheus/config/v1/rules", get(ruler_config_rules::<S>))
        .route(
            "/prometheus/config/v1/rules/{namespace}",
            get(ruler_config_namespace::<S>)
                .post(set_ruler_config_group::<S>)
                .delete(delete_ruler_config_namespace::<S>),
        )
        .route(
            "/prometheus/config/v1/rules/{namespace}/{group_name}",
            get(ruler_config_group::<S>).delete(delete_ruler_config_group::<S>),
        )
        .route(
            "/prometheus/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route(
            "/prometheus/api/v1/parse_query",
            get(parse_query).post(parse_query_post),
        )
        .route("/prometheus/api/v1/status/buildinfo", get(build_info))
        .route("/prometheus/api/v1/status/config", get(status_config))
        .route("/prometheus/api/v1/status/flags", get(status_flags::<S>))
        .route(
            "/prometheus/api/v1/status/runtimeinfo",
            get(runtime_info::<S>),
        )
        .route("/prometheus/api/v1/status/tsdb", get(tsdb_status::<S>))
        .route(
            "/prometheus/api/v1/status/tsdb/blocks",
            get(tsdb_blocks::<S>),
        )
        .route(
            "/prometheus/api/v1/status/walreplay",
            get(wal_replay_status),
        )
        .with_state(state)
}
