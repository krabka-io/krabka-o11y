use super::{
    QUERIER_OPS, QuerierState, Router, ServiceReadiness, api_prom_label_names,
    api_prom_label_names_post, api_prom_label_values, api_prom_label_values_post, api_prom_query,
    api_prom_query_post, api_prom_query_range, api_prom_query_range_post, api_prom_series,
    api_prom_series_post, create_loki_rule_group, delete_loki_rule_group,
    delete_loki_rule_namespace, detected_field_values, detected_field_values_post, detected_fields,
    detected_fields_post, detected_labels, detected_labels_post, format_query, format_query_post,
    get, index_stats, index_stats_post, index_volume, index_volume_post, index_volume_range,
    index_volume_range_post, label_names, label_names_post, label_values, label_values_post,
    loki_page_not_found, loki_rule_group, loki_rule_namespace, loki_rules, patterns, patterns_post,
    prometheus_alerts, prometheus_rules, query, query_post, query_range, query_range_post,
    ruler_ring, scheduler_ring, series, series_post, tail, with_role_ops_routes,
};

pub(crate) fn loki_router_with_readiness(
    state: QuerierState,
    readiness: ServiceReadiness,
) -> Router {
    with_role_ops_routes(Router::new(), QUERIER_OPS, readiness)
        .route("/loki/api/v1/rules", get(loki_rules))
        .route(
            "/loki/api/v1/rules/{namespace}",
            get(loki_rule_namespace)
                .post(create_loki_rule_group)
                .delete(delete_loki_rule_namespace),
        )
        .route(
            "/loki/api/v1/rules/{namespace}/{group_name}",
            get(loki_rule_group).delete(delete_loki_rule_group),
        )
        .route("/prometheus/api/v1/rules", get(prometheus_rules))
        .route("/prometheus/api/v1/alerts", get(prometheus_alerts))
        .route("/ruler/ring", get(ruler_ring))
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route("/loki/api/v1/patterns", get(patterns).post(patterns_post))
        .route(
            "/loki/api/v1/detected_fields",
            get(detected_fields).post(detected_fields_post),
        )
        .route(
            "/loki/api/v1/detected_labels",
            get(detected_labels).post(detected_labels_post),
        )
        .route(
            "/loki/api/v1/detected_field/{name}/values",
            get(detected_field_values).post(detected_field_values_post),
        )
        .route("/loki/api/v1/query", get(query).post(query_post))
        .route(
            "/loki/api/v1/query_range",
            get(query_range).post(query_range_post),
        )
        .route(
            "/loki/api/v1/labels",
            get(label_names).post(label_names_post),
        )
        .route(
            "/loki/api/v1/label",
            get(label_names).post(label_names_post),
        )
        .route(
            "/loki/api/v1/label/{name}/values",
            get(label_values).post(label_values_post),
        )
        .route("/loki/api/v1/series", get(series).post(series_post))
        .route(
            "/api/prom/query",
            get(api_prom_query).post(api_prom_query_post),
        )
        .route(
            "/api/prom/query_range",
            get(api_prom_query_range).post(api_prom_query_range_post),
        )
        .route("/api/prom/rules", get(loki_rules))
        .route("/api/prom/alerts", get(loki_page_not_found))
        .route("/scheduler/ring", get(scheduler_ring))
        .route(
            "/api/prom/rules/{namespace}",
            get(loki_rule_namespace)
                .post(create_loki_rule_group)
                .delete(delete_loki_rule_namespace),
        )
        .route(
            "/api/prom/rules/{namespace}/{group_name}",
            get(loki_rule_group).delete(delete_loki_rule_group),
        )
        .route("/api/prom/tail", get(tail))
        .route(
            "/api/prom/label",
            get(api_prom_label_names).post(api_prom_label_names_post),
        )
        .route(
            "/api/prom/label/{name}/values",
            get(api_prom_label_values).post(api_prom_label_values_post),
        )
        .route(
            "/api/prom/series",
            get(api_prom_series).post(api_prom_series_post),
        )
        .route(
            "/loki/api/v1/index/stats",
            get(index_stats).post(index_stats_post),
        )
        .route(
            "/loki/api/v1/index/volume",
            get(index_volume).post(index_volume_post),
        )
        .route(
            "/loki/api/v1/index/volume_range",
            get(index_volume_range).post(index_volume_range_post),
        )
        .route("/loki/api/v1/tail", get(tail))
        .with_state(state)
}
