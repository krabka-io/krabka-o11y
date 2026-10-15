use axum::response::IntoResponse;

use crate::{
    Arc, BLOCK_BUILDER_OPS, Bytes, CompactorDeleteState, DRAINING_GATE, DistributorState,
    Extension, HttpQueryError, LogLevelControl, LogLevelError, LogQueryAuthorizer, QUERIER_OPS,
    QuerierState, RawQuery, RequestSecurity, Response, RoleOps, RoleReadiness, Router,
    SharedLogDeleteRequests, State, StatusCode, api_prom_label_names, api_prom_label_names_post,
    api_prom_label_values, api_prom_label_values_post, api_prom_query, api_prom_query_post,
    api_prom_query_range, api_prom_query_range_post, api_prom_series, api_prom_series_post,
    audit::{
        AuditOutcome, OPERATION_INGESTER_FLUSH, OPERATION_INGESTER_PREPARE_SHUTDOWN_SET,
        OPERATION_INGESTER_PREPARE_SHUTDOWN_UNSET, OPERATION_INGESTER_SHUTDOWN,
        OPERATION_LOG_LEVEL_SET, RESOURCE_LOG_LEVEL, resource,
    },
    cancel_delete_request, create_delete_request, decode_query_pair, detected_field_values,
    detected_field_values_post, detected_fields, detected_fields_post, detected_labels,
    detected_labels_post, form_body_query, format_query, format_query_post, get, index_shards,
    index_stats, index_stats_post, index_volume, index_volume_post, index_volume_range,
    index_volume_range_post, json, json_response, label_names, label_names_post, label_values,
    label_values_post, list_delete_requests, patterns, patterns_post, query, query_post,
    query_range, query_range_post, require_org_id,
    ruler::{
        create_loki_rule_group, delete_loki_rule_group, delete_loki_rule_namespace,
        loki_page_not_found, loki_rule_group, loki_rule_namespace, loki_rules, prometheus_alerts,
        prometheus_rules, ring_status_page, ruler_status_page,
    },
    series, series_post, status_metrics, tail, text_response, with_role_ops_routes,
};

fn prepare_ingester_shutdown(
    state: &DistributorState,
    security: &RequestSecurity,
    operation: &'static str,
) -> Response {
    if let Err(denied) = security.authorize_admin() {
        return denied.into_response();
    }
    state.prepare_shutdown.mark_unready();
    security.admin_operation(operation, vec![security.ingester()], AuditOutcome::Success);
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /ingester/prepare_shutdown`: whether a drain is in force.
pub(crate) async fn get_prepare_shutdown(State(state): State<DistributorState>) -> Response {
    let status = if state.prepare_shutdown.is_ready() {
        "unset"
    } else {
        "set"
    };
    text_response(StatusCode::OK, status)
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

pub fn loki_router(state: QuerierState) -> Router {
    loki_router_with_readiness(state, RoleReadiness::new())
}

pub(crate) fn loki_router_with_readiness(state: QuerierState, readiness: RoleReadiness) -> Router {
    with_role_ops_routes(Router::new(), QUERIER_OPS, readiness).merge(loki_query_routes(state))
}

pub(crate) fn query_param_value(raw_query: Option<&str>, name: &str) -> Option<String> {
    let raw_query = raw_query?;
    for pair in raw_query.split('&') {
        let pair = decode_query_pair(pair).ok()?;
        if pair.key == name {
            return Some(pair.value);
        }
    }
    None
}

pub(crate) async fn role_config(RawQuery(raw_query): RawQuery) -> Response {
    status_config(raw_query.as_deref())
}

pub(crate) async fn role_metrics(Extension(ops): Extension<RoleOps>) -> Response {
    status_metrics(ops.target)
}

pub(crate) async fn role_services(Extension(readiness): Extension<RoleReadiness>) -> Response {
    status_services(&readiness)
}

pub(crate) async fn ruler_ring() -> Response {
    ruler_status_page()
}

pub(crate) async fn scheduler_ring() -> Response {
    // No readiness gate stands between this process and scheduling: the
    // querier role schedules its own work in-process, so the scheduler is
    // active from the moment the listener answers.
    ring_status_page("krabka-scheduler", "ACTIVE")
}

pub(crate) fn compactor_router_with_delete_requests(
    delete_requests: SharedLogDeleteRequests,
    query_authorizer: Arc<dyn LogQueryAuthorizer>,
    readiness: RoleReadiness,
) -> Router {
    with_role_ops_routes(Router::new(), BLOCK_BUILDER_OPS, readiness)
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .merge(delete_request_routes(delete_requests, query_authorizer))
}

/// `Loki`'s delete-request API, with no ops routes on it.
///
/// The block builder owns these because it is the role that materialises a
/// delete while it writes, which is what `Loki`'s compactor does too. Every
/// route checks its tenant against `query_authorizer`.
pub(crate) fn delete_request_routes(
    delete_requests: SharedLogDeleteRequests,
    query_authorizer: Arc<dyn LogQueryAuthorizer>,
) -> Router {
    Router::new()
        .route(
            "/loki/api/v1/delete",
            get(list_delete_requests)
                .post(create_delete_request)
                .put(create_delete_request)
                .delete(cancel_delete_request),
        )
        .route_layer(axum::middleware::from_fn(require_org_id))
        .with_state(CompactorDeleteState {
            delete_requests,
            query_authorizer,
        })
}

/// `POST /flush`: force buffered writes out to storage.
///
/// Krabka's distributor holds no chunk buffer. `POST /loki/api/v1/push` awaits
/// the WAL append and answers 204 only once the WAL has taken the records, so
/// at the moment this handler runs everything acknowledged is already durable
/// and there is nothing left to force. The empty 204 is `Loki`'s answer to a
/// flush with nothing to do, and here it is the answer every time.
///
/// An authenticated principal needs `admin: true`, and any other authenticated
/// principal gets 403. The audit trail records each flush.
pub(crate) async fn flush_ingester_chunks(security: RequestSecurity) -> Response {
    if let Err(denied) = security.authorize_admin() {
        return denied.into_response();
    }
    security.admin_operation(
        OPERATION_INGESTER_FLUSH,
        vec![security.ingester()],
        AuditOutcome::Success,
    );
    StatusCode::NO_CONTENT.into_response()
}

/// `GET /log_level`, reporting the level this process is really filtering at.
///
/// It used to answer `Current log level is info` whatever the process was
/// doing, so the one page that could have told an operator their `RUST_LOG`
/// had not taken agreed with them that it had.
pub(crate) async fn log_level() -> Response {
    json_response(
        StatusCode::OK,
        &json!({
            "message": format!("Current log level is {}", LogLevelControl::process().level()),
        }),
    )
}

/// The memberlist page.
///
/// Kept as a constant deliberately, and it is not a stub: Krabka joins no
/// gossip cluster, and this is the sentence real `Loki` serves here when its
/// memberlist KV is not configured. Anything else -- a peer table, a "healthy"
/// claim -- would be the fabrication this endpoint is supposed to avoid.
pub(crate) async fn memberlist_status() -> Response {
    Response::builder()
        .status(crate::StatusCode::OK)
        .header("content-type", "text/plain")
        .body("This instance doesn't use memberlist.".into())
        .expect("memberlist response headers are valid")
}

pub(crate) fn parse_log_level_param(raw_query: Option<&str>) -> Result<String, HttpQueryError> {
    let Some(raw_query) = raw_query else {
        return Err(HttpQueryError::MissingQueryParameter("log_level"));
    };
    for pair in raw_query.split('&').filter(|pair| !pair.is_empty()) {
        let pair = decode_query_pair(pair)?;
        if pair.key == "log_level" {
            let level = pair.value;
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

pub(crate) async fn role_ring(
    Extension(ops): Extension<RoleOps>,
    Extension(readiness): Extension<RoleReadiness>,
) -> Response {
    // The same readiness `/ready` and `/services` report. A process that is
    // still loading its index is joining, not serving.
    let state = if readiness.is_ready() {
        "ACTIVE"
    } else {
        "JOINING"
    };
    ring_status_page(ops.ring_component, state)
}

/// `DELETE /ingester/prepare_shutdown`: cancel a drain, as `Loki` does.
///
/// An authenticated principal needs `admin: true`, and any other authenticated
/// principal gets 403 with the readiness unchanged. The audit trail records
/// each request that restores the gate.
pub(crate) async fn unset_prepare_shutdown(
    State(state): State<DistributorState>,
    security: RequestSecurity,
) -> Response {
    if let Err(denied) = security.authorize_admin() {
        return denied.into_response();
    }
    state.prepare_shutdown.mark_ready();
    security.admin_operation(
        OPERATION_INGESTER_PREPARE_SHUTDOWN_UNSET,
        vec![security.ingester()],
        AuditOutcome::Success,
    );
    StatusCode::NO_CONTENT.into_response()
}

/// `POST /log_level`, moving this process's filter.
///
/// The reply follows what happened. A success means the filter moved and the
/// next line at that level is emitted; a process whose logging was installed
/// without a reload handle answers 501 and names the variable that does set
/// its level, because an operator who is told `success` and then sees no new
/// output goes looking for the bug somewhere else entirely.
///
/// An authenticated principal needs `admin: true`. Any other authenticated
/// principal gets 403 before the request is read. The audit trail records each
/// attempt to set a valid level, with its outcome.
pub(crate) async fn log_level_post(
    security: RequestSecurity,
    RawQuery(raw_query): RawQuery,
    body: Bytes,
) -> Response {
    if let Err(denied) = security.authorize_admin() {
        return denied.into_response();
    }
    match requested_log_level(raw_query.as_deref(), &body) {
        Ok(level) => {
            let result = LogLevelControl::process().set_level(&level);
            security.admin_operation(
                OPERATION_LOG_LEVEL_SET,
                vec![resource(RESOURCE_LOG_LEVEL, level.as_str())],
                if result.is_ok() {
                    AuditOutcome::Success
                } else {
                    AuditOutcome::Failure
                },
            );
            match result {
                Ok(()) => json_response(
                    StatusCode::OK,
                    &json!({
                        "status": "success",
                        "message": format!("Log level set to {level}"),
                    }),
                ),
                Err(error @ LogLevelError::Fixed) => json_response(
                    StatusCode::NOT_IMPLEMENTED,
                    &json!({
                        "status": "failed",
                        "message": error.to_string(),
                    }),
                ),
                Err(error) => json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &json!({
                        "status": "failed",
                        "message": error.to_string(),
                    }),
                ),
            }
        }
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

/// Every `Loki` read route, with no ops routes on it.
///
/// The ops routes -- `/ready`, `/metrics`, `/config`, `/services` and the ring
/// pages -- say which role the process is, and a process is one role even when
/// it runs several. Keeping them out of here is what lets an all-in-one merge
/// this surface with the distributor's push routes and still answer `/ready`
/// once, for the whole process, rather than twice with two different answers.
pub(crate) fn loki_query_routes(state: QuerierState) -> Router {
    // `dskit`'s auth middleware guards every route that reads a tenant, so a
    // request without `X-Scope-OrgID` gets 401 before its parameters are read.
    let tenant_routes = Router::new()
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
        .route("/loki/api/v1/index/shards", get(index_shards))
        .route(
            "/loki/api/v1/index/volume",
            get(index_volume).post(index_volume_post),
        )
        .route(
            "/loki/api/v1/index/volume_range",
            get(index_volume_range).post(index_volume_range_post),
        )
        .route("/loki/api/v1/tail", get(tail))
        .route_layer(axum::middleware::from_fn(require_org_id));
    Router::new()
        .route("/ruler/ring", get(ruler_ring))
        .route(
            "/loki/api/v1/format_query",
            get(format_query).post(format_query_post),
        )
        .route("/api/prom/alerts", get(loki_page_not_found))
        .route("/scheduler/ring", get(scheduler_ring))
        .merge(tenant_routes)
        .with_state(state)
}

/// The modules `/services` reports, in the order single-binary `Loki` lists
/// them.
///
/// The order is part of the page a `Loki` runbook reads, so it is pinned here
/// rather than sorted.
pub(crate) const LOKI_SERVICE_MODULES: &[&str] = &[
    "query-scheduler",
    "ingester-querier",
    "query-frontend",
    "server",
    "querier",
    "rule-evaluator",
    "memberlist-kv",
    "query-frontend-tripperware",
    "pattern-ingester",
    "pattern-ingester-tee",
    "pattern-ring-client",
    "ruler",
    "cache-generation-loader",
    "store",
    "ring",
    "ingester",
    "compactor",
    "distributor",
    "query-scheduler-ring",
];

/// The level a `POST /log_level` is asking for.
///
/// `Loki` accepts the parameter in the query string, in a form body, or in
/// both. When both carry one the BODY wins, because the merged string puts it
/// first and the parser returns on the first match -- an ordering that only
/// shows when the two disagree.
///
/// # Errors
/// Returns [`HttpQueryError::MissingQueryParameter`] when neither source names
/// a level and [`HttpQueryError::InvalidQueryParameter`] when the level named
/// is not one this process filters at.
pub(crate) fn requested_log_level(
    raw_query: Option<&str>,
    body: &Bytes,
) -> Result<String, HttpQueryError> {
    let body_query = form_body_query(body)?;
    // Both `!raw_query.is_empty()` guards are permanent mutation survivors
    // against `true`, and only against `true`. An empty query string with an
    // empty body falls through to the same empty string either way; with a
    // non-empty body it would merely append a trailing `&`, which the
    // parameter parser skips. Dropping them the other way, to `false`, does
    // change the answer: a level named only in the query string is lost.
    let raw_params = if let Some(raw_query) = raw_query.filter(|query| !query.is_empty()) {
        if body_query.is_empty() {
            raw_query.to_owned()
        } else {
            format!("{body_query}&{raw_query}")
        }
    } else {
        body_query
    };
    parse_log_level_param(Some(&raw_params))
}

/// `POST /ingester/prepare_shutdown`: take this process out of rotation.
///
/// `Loki` unregisters from the ring here so the distributors stop choosing the
/// instance. Krabka has no ring, so the equivalent -- and the thing an
/// operator's `preStop` hook actually depends on -- is to fail the readiness
/// probe. The request that reaches here therefore drops the
/// [`DRAINING_GATE`](crate::DRAINING_GATE), `/ready` answers 503 from the next
/// probe, and the orchestrator takes the endpoint out of the service. Nothing
/// is buffered to lose: a push is only acknowledged once the WAL append it
/// made has been.
///
/// An authenticated principal needs `admin: true`, and any other authenticated
/// principal gets 403 with the readiness unchanged. The audit trail records
/// each request that drops the gate.
pub(crate) async fn set_prepare_shutdown(
    State(state): State<DistributorState>,
    security: RequestSecurity,
) -> Response {
    prepare_ingester_shutdown(&state, &security, OPERATION_INGESTER_PREPARE_SHUTDOWN_SET)
}

/// `/ingester/shutdown`: leave rotation for good.
///
/// `Loki` flushes, unregisters and terminates. Krabka has nothing to flush --
/// a push is acknowledged only after its WAL append is -- and no ring to leave,
/// so what is left of the operation is to stop being ready, which this does.
/// The process does not exit itself: an orchestrator that sees the probe fail
/// stops it, and a process that killed itself out from under an in-flight
/// query would be the opposite of a drain.
///
/// An authenticated principal needs `admin: true`, and any other authenticated
/// principal gets 403 with the readiness unchanged. The audit trail records
/// each shutdown.
pub(crate) async fn shutdown_ingester(
    State(state): State<DistributorState>,
    security: RequestSecurity,
) -> Response {
    prepare_ingester_shutdown(&state, &security, OPERATION_INGESTER_SHUTDOWN)
}

fn yaml_response(status: StatusCode, body: String, content_type: &'static str) -> Response {
    Response::builder()
        .status(status)
        .header("content-type", content_type)
        .body(axum::body::Body::from(body))
        .expect("a static config response is valid")
}

pub(crate) fn status_config(raw_query: Option<&str>) -> Response {
    match query_param_value(raw_query, "mode").as_deref() {
        Some("diff") => {
            return yaml_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "unsupported type <nil>\n".to_string(),
                "text/plain; charset=utf-8",
            );
        }
        Some("defaults") => {
            return yaml_response(
                StatusCode::OK,
                format!("target: {LOKI_CONFIG_TARGET}\nauth_enabled: true\n"),
                "application/yaml; charset=utf-8",
            );
        }
        _ => {}
    }

    // `auth_enabled: true` is what this process does: every read and every
    // push without `X-Scope-OrgID` is refused with `no org id`. Loki's
    // effective config names the setting, so a reader of this page can tell
    // a multi-tenant deployment from a single-tenant one.
    yaml_response(
        StatusCode::OK,
        format!("target: {LOKI_CONFIG_TARGET}\nauth_enabled: true\n"),
        "application/yaml; charset=utf-8",
    )
}

/// The `/services` page, reporting each module's state.
///
/// `Loki` lists the modules its process runs and the state each is in. Krabka
/// serves the whole `Loki` surface from every role, so the list is the
/// single-binary one -- see [`LOKI_CONFIG_TARGET`](LOKI_CONFIG_TARGET)
/// -- but the states are this process's own. A module reads `Starting` while
/// any readiness gate is unmet, which is exactly when `/ready` answers 503, so
/// the two pages cannot disagree. Before this took its state from
/// [`RoleReadiness`] it said `Running` for everything from the first bound
/// port onwards, and a rollout watching `/services` saw a healthy process at
/// the same moment `/ready` was refusing traffic.
///
/// `server` is the exception: the HTTP listener answered the request that got
/// here, so it is running whatever else is still coming up.
pub(crate) fn status_services(readiness: &RoleReadiness) -> Response {
    let pending = readiness.pending();
    let state = if pending.is_empty() {
        "Running"
    } else if readiness.is_pending(DRAINING_GATE) {
        // An operator asked this process to leave rotation. `Loki` calls that
        // `Stopping`, and calling it `Starting` would send a runbook the wrong
        // way at the one moment it is being read.
        "Stopping"
    } else {
        "Starting"
    };
    let mut page = String::new();
    for module in LOKI_SERVICE_MODULES {
        let module_state = if *module == "server" {
            "Running"
        } else {
            state
        };
        page.push_str(module);
        page.push_str(" => ");
        page.push_str(module_state);
        page.push('\n');
    }
    text_response(StatusCode::OK, &page)
}
