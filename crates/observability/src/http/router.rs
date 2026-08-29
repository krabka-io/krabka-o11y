#[allow(clippy::wildcard_imports)]
use super::*;
use crate::ruler::{
    create_loki_rule_group, delete_loki_rule_group, delete_loki_rule_namespace,
    loki_page_not_found, loki_rule_group, loki_rule_namespace, loki_rules, prometheus_alerts,
    prometheus_rules, ring_status_page, ruler_status_page,
};

pub fn loki_router(state: QuerierState) -> Router {
    loki_router_with_readiness(state, ServiceReadiness::ready())
}

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

pub(crate) fn compactor_router_with_delete_requests(
    delete_requests: SharedLogDeleteRequests,
) -> Router {
    let delete_state = CompactorDeleteState { delete_requests };
    with_role_ops_routes(Router::new(), COMPACTOR_OPS, ServiceReadiness::ready())
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route(
            "/loki/api/v1/delete",
            get(list_delete_requests)
                .post(create_delete_request)
                .put(create_delete_request)
                .delete(cancel_delete_request),
        )
        .with_state(delete_state)
}

pub(crate) async fn ready(Extension(readiness): Extension<ServiceReadiness>) -> Response {
    if readiness.is_ready() {
        (StatusCode::OK, "ready\n").into_response()
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "not ready\n").into_response()
    }
}

pub(crate) async fn flush_ingester_chunks() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

pub(crate) async fn get_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    let status = if state.prepare_shutdown.load(AtomicOrdering::SeqCst) {
        "set"
    } else {
        "unset"
    };
    text_response(StatusCode::OK, status)
}

pub(crate) async fn set_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    state.prepare_shutdown.store(true, AtomicOrdering::SeqCst);
    StatusCode::NO_CONTENT.into_response()
}

pub(crate) async fn unset_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    state.prepare_shutdown.store(false, AtomicOrdering::SeqCst);
    StatusCode::NO_CONTENT.into_response()
}

pub(crate) async fn shutdown_ingester() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

pub(crate) async fn log_level() -> Response {
    json_response(
        StatusCode::OK,
        &json!({ "message": "Current log level is info" }),
    )
}

pub(crate) async fn log_level_post(RawQuery(raw_query): RawQuery, body: Bytes) -> Response {
    let body_query = match form_body_query(&body) {
        Ok(body_query) => body_query,
        Err(error) => return error.into_response(),
    };
    // Both `!raw_query.is_empty()` guards are permanent mutation survivors
    // against `true`, and only against `true`. An empty query string with an
    // empty body falls through to the same empty string either way; with a
    // non-empty body it would merely append a trailing `&`, which the
    // parameter parser skips. Dropping them the other way, to `false`, does
    // change the answer: a level named only in the query string is lost.
    let raw_params = match (raw_query.as_deref(), body_query.is_empty()) {
        (Some(raw_query), true) if !raw_query.is_empty() => raw_query.to_owned(),
        (Some(raw_query), false) if !raw_query.is_empty() => format!("{body_query}&{raw_query}"),
        _ => body_query,
    };
    match parse_log_level_param(Some(&raw_params)) {
        Ok(level) => json_response(
            StatusCode::OK,
            &json!({
                "status": "success",
                "message": format!("Log level set to {level}"),
            }),
        ),
        Err(HttpQueryError::InvalidQueryParameter {
            name: "log_level",
            value,
        }) => log_level_failed_response(&format!("unrecognized log level \"{value}\"")),
        Err(HttpQueryError::MissingQueryParameter("log_level")) => {
            log_level_failed_response("unrecognized log level \"\"")
        }
        Err(error) => error.into_response(),
    }
}

pub(crate) fn log_level_failed_response(message: &str) -> Response {
    json_response(
        StatusCode::BAD_REQUEST,
        &json!({
            "status": "failed",
            "message": message,
        }),
    )
}

pub(crate) fn parse_log_level_param(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("log_level"));
    };
    for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_form_component(key)? == "log_level" {
            let level = decode_form_component(value)?;
            return match level.as_str() {
                "debug" | "info" | "warn" | "error" => Ok(level),
                _ => Err(HttpQueryError::InvalidQueryParameter {
                    name: "log_level",
                    value: level,
                }),
            };
        }
    }
    Err(HttpQueryError::MissingQueryParameter("log_level"))
}

/// The `target` that `/config` reports, for every role.
///
/// `Loki` reports the components its process runs. Krabka serves the full `Loki`
/// surface from each role, so its ops endpoints answer as single-binary `Loki`
/// does: [`status_services`] lists every component whichever role serves it,
/// and `/config` reports the target that goes with that list.
/// `real_loki_and_krabka_return_same_stable_config_status_lines` compares this
/// against a real `Loki` container, which reports `all`.
///
/// The per-role name stays in [`RoleOps::target`] for `/metrics`, where `Loki`
/// does report the running component.
pub(crate) const LOKI_CONFIG_TARGET: &str = "all";

pub(crate) async fn role_config(RawQuery(raw_query): RawQuery) -> Response {
    status_config(raw_query.as_deref())
}

pub(crate) fn status_config(raw_query: Option<&str>) -> Response {
    match query_param_value(raw_query, "mode").as_deref() {
        Some("diff") => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("content-type", "text/plain; charset=utf-8")],
                "unsupported type <nil>\n",
            )
                .into_response();
        }
        Some("defaults") => {
            return (
                StatusCode::OK,
                [("content-type", "application/yaml; charset=utf-8")],
                format!("target: {LOKI_CONFIG_TARGET}\nauth_enabled: true\n"),
            )
                .into_response();
        }
        _ => {}
    }

    (
        StatusCode::OK,
        [("content-type", "application/yaml; charset=utf-8")],
        format!("target: {LOKI_CONFIG_TARGET}\n"),
    )
        .into_response()
}

pub(crate) fn query_param_value(raw_query: Option<&str>, name: &str) -> Option<String> {
    let raw_query = raw_query?;
    for pair in raw_query.split('&') {
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        if decode_form_component(key).ok()? == name {
            return decode_form_component(value).ok();
        }
    }
    None
}

pub(crate) async fn role_services(Extension(ops): Extension<RoleOps>) -> Response {
    status_services(ops.target)
}

pub(crate) fn status_services(_name: &'static str) -> Response {
    text_response(
        StatusCode::OK,
        "query-scheduler => Running\n\
         ingester-querier => Running\n\
         query-frontend => Running\n\
         server => Running\n\
         querier => Running\n\
         rule-evaluator => Running\n\
         memberlist-kv => Running\n\
         query-frontend-tripperware => Running\n\
         analytics => Running\n\
         ruler => Running\n\
         cache-generation-loader => Running\n\
         store => Running\n\
         ring => Running\n\
         ingester => Running\n\
         compactor => Running\n\
         distributor => Running\n\
         query-scheduler-ring => Running\n",
    )
}

pub(crate) async fn memberlist_status() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/plain")],
        "This instance doesn't use memberlist.",
    )
        .into_response()
}

pub(crate) async fn role_metrics(Extension(ops): Extension<RoleOps>) -> Response {
    status_metrics(ops.target)
}

pub(crate) async fn scheduler_ring() -> Response {
    ring_status_page("krabka-scheduler")
}

pub(crate) async fn ruler_ring() -> Response {
    ruler_status_page()
}

pub(crate) async fn role_ring(Extension(ops): Extension<RoleOps>) -> Response {
    ring_status_page(ops.ring_component)
}
