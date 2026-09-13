use std::{
    fmt::Write as _,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{Path, RawQuery, State},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use krabka_observability::server_security::Principal;
use serde::Deserialize;
use serde_json::{Value, json};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use xxhash_rust::xxh64::xxh64;

use super::{MetricStore, PrometheusApiState, authorized_tenant_from_headers};

static NEXT_SILENCE_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Deserialize)]
struct AlertmanagerConfig {
    #[serde(default)]
    template_files: std::collections::BTreeMap<String, String>,
    alertmanager_config: String,
}

/// Builds Mimir's multitenant Alertmanager configuration and Grafana v2 API routes.
pub fn mimir_alertmanager_router<S: MetricStore + 'static>(
    state: Arc<PrometheusApiState<S>>,
) -> Router {
    Router::new()
        .route(
            "/api/v1/alerts",
            get(get_config::<S>)
                .post(set_config::<S>)
                .delete(delete_config::<S>),
        )
        .route("/alertmanager/api/v2/status", get(status::<S>))
        .route("/alertmanager/api/v2/receivers", get(receivers::<S>))
        .route(
            "/alertmanager/api/v2/alerts",
            get(alerts::<S>).post(set_alerts::<S>),
        )
        .route("/alertmanager/api/v2/alerts/groups", get(alert_groups::<S>))
        .route(
            "/alertmanager/api/v2/silences",
            get(silences::<S>).post(set_silence::<S>),
        )
        .route(
            "/alertmanager/api/v2/silence/{id}",
            get(silence::<S>).delete(delete_silence::<S>),
        )
        .with_state(state)
}

fn tenant(
    headers: &HeaderMap,
    principal: &Principal,
) -> Result<krabka_blockstore::TenantId, Box<Response>> {
    authorized_tenant_from_headers(headers, principal)
        .map_err(|error| Box::new(error.into_response()))
}

async fn get_config<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    match state.alertmanager_configs.read() {
        Ok(configs) => match configs.get(&tenant) {
            Some(config) => (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/yaml")],
                config.clone(),
            )
                .into_response(),
            None => (
                StatusCode::NOT_FOUND,
                "alertmanager storage object not found\n",
            )
                .into_response(),
        },
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn set_config<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let body = match String::from_utf8(body.to_vec()) {
        Ok(body) => body,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let config: AlertmanagerConfig = match serde_yaml::from_str(&body) {
        Ok(config) => config,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    let parsed = match serde_yaml::from_str::<serde_yaml::Value>(&config.alertmanager_config) {
        Ok(parsed) => parsed,
        Err(error) => return (StatusCode::BAD_REQUEST, error.to_string()).into_response(),
    };
    if let Err(error) = validate_alertmanager_config(&parsed, &config.template_files) {
        return (StatusCode::BAD_REQUEST, error).into_response();
    }
    let stored = match state.alertmanager_configs.write() {
        Ok(mut configs) => {
            configs.insert(tenant.clone(), body);
            true
        }
        Err(_) => false,
    };
    if !stored {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    match state.persist_alertmanager_config(&tenant).await {
        Ok(()) => StatusCode::CREATED.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

async fn delete_config<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let deleted = match state.alertmanager_configs.write() {
        Ok(mut configs) => {
            configs.remove(&tenant);
            true
        }
        Err(_) => false,
    };
    if !deleted {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    match state.persist_alertmanager_config(&tenant).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

async fn status<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let config = state
        .alertmanager_configs
        .read()
        .ok()
        .and_then(|configs| configs.get(&tenant).cloned())
        .unwrap_or_default();
    no_store(Json(json!({"cluster":{"name":"krabka","peers":[],"status":"ready"},"config":{"original":config},"uptime":"0001-01-01T00:00:00Z","versionInfo":{"branch":"","buildDate":"","buildUser":"","goVersion":"","revision":"","version":env!("CARGO_PKG_VERSION")}})).into_response())
}

async fn receivers<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let names = state
        .alertmanager_configs
        .read()
        .ok()
        .and_then(|configs| configs.get(&tenant).cloned())
        .and_then(|body| serde_yaml::from_str::<AlertmanagerConfig>(&body).ok())
        .and_then(|wrapper| {
            serde_yaml::from_str::<serde_yaml::Value>(&wrapper.alertmanager_config).ok()
        })
        .and_then(|config| {
            config
                .get("receivers")
                .and_then(serde_yaml::Value::as_sequence)
                .cloned()
        })
        .unwrap_or_default()
        .into_iter()
        .filter_map(|receiver| {
            receiver
                .get("name")
                .and_then(serde_yaml::Value::as_str)
                .map(|name| json!({"name":name}))
        })
        .collect::<Vec<_>>();
    no_store(Json(names).into_response())
}

async fn alerts<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let alerts = state
        .alertmanager_alerts
        .read()
        .ok()
        .and_then(|alerts| alerts.get(&tenant).cloned())
        .unwrap_or_default()
        .into_iter()
        .filter(|alert| matches_filters(alert, query.as_deref()))
        .collect::<Vec<_>>();
    no_store(Json(alerts).into_response())
}

async fn set_alerts<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Json(mut new_alerts): Json<Vec<Value>>,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let receiver = default_receiver(&state, &tenant);
    for alert in &mut new_alerts {
        enrich_alert(alert, receiver.as_deref());
    }
    let stored = match state.alertmanager_alerts.write() {
        Ok(mut alerts) => {
            alerts.entry(tenant.clone()).or_default().extend(new_alerts);
            true
        }
        Err(_) => false,
    };
    if !stored {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    match state.persist_alertmanager_alerts(&tenant).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

async fn alert_groups<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let alerts = state
        .alertmanager_alerts
        .read()
        .ok()
        .and_then(|all| all.get(&tenant).cloned())
        .unwrap_or_default();
    let mut groups = std::collections::BTreeMap::<String, Vec<Value>>::new();
    for alert in alerts
        .into_iter()
        .filter(|alert| matches_filters(alert, query.as_deref()))
    {
        let receiver = alert
            .pointer("/receivers/0/name")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_owned();
        groups.entry(receiver).or_default().push(alert);
    }
    no_store(Json(groups.into_iter().map(|(receiver, alerts)| json!({"labels":{},"receiver":{"name":receiver},"alerts":alerts})).collect::<Vec<_>>()).into_response())
}

async fn silences<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    RawQuery(query): RawQuery,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let silences = state
        .alertmanager_silences
        .read()
        .ok()
        .and_then(|silences| {
            silences
                .get(&tenant)
                .map(|items| items.values().cloned().collect::<Vec<_>>())
        })
        .unwrap_or_default()
        .into_iter()
        .map(with_silence_state)
        .filter(|silence| matches_filters(silence, query.as_deref()))
        .collect::<Vec<_>>();
    no_store(Json(silences).into_response())
}

async fn set_silence<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Json(mut silence): Json<Value>,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let id = silence
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .map_or_else(new_silence_id, str::to_owned);
    if let Some(object) = silence.as_object_mut() {
        object.insert("id".into(), Value::String(id.clone()));
    }
    silence = with_silence_state(silence);
    let stored = match state.alertmanager_silences.write() {
        Ok(mut silences) => {
            silences
                .entry(tenant.clone())
                .or_default()
                .insert(id.clone(), silence);
            true
        }
        Err(_) => false,
    };
    if !stored {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    match state.persist_alertmanager_silences(&tenant).await {
        Ok(()) => Json(json!({"silenceID":id})).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

async fn silence<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    match state
        .alertmanager_silences
        .read()
        .ok()
        .and_then(|silences| {
            silences
                .get(&tenant)
                .and_then(|items| items.get(&id))
                .cloned()
        }) {
        Some(silence) => no_store(Json(with_silence_state(silence)).into_response()),
        None => StatusCode::NOT_FOUND.into_response(),
    }
}

async fn delete_silence<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(id): Path<String>,
) -> Response {
    let tenant = match tenant(&headers, &principal) {
        Ok(tenant) => tenant,
        Err(response) => return *response,
    };
    let deleted = match state.alertmanager_silences.write() {
        Ok(mut silences) => {
            if let Some(items) = silences.get_mut(&tenant) {
                items.remove(&id);
            }
            true
        }
        Err(_) => false,
    };
    if !deleted {
        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
    }
    match state.persist_alertmanager_silences(&tenant).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

fn validate_alertmanager_config(
    config: &serde_yaml::Value,
    templates: &std::collections::BTreeMap<String, String>,
) -> Result<(), String> {
    if templates.keys().any(|name| name.trim().is_empty()) {
        return Err("template file names must not be empty".into());
    }
    let route_receiver = config
        .get("route")
        .and_then(|route| route.get("receiver"))
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
        .ok_or_else(|| "alertmanager config route.receiver is required".to_string())?;
    let receivers = config
        .get("receivers")
        .and_then(serde_yaml::Value::as_sequence)
        .ok_or_else(|| "alertmanager config receivers are required".to_string())?;
    let mut names = std::collections::BTreeSet::new();
    for receiver in receivers {
        let name = receiver
            .get("name")
            .and_then(serde_yaml::Value::as_str)
            .filter(|name| !name.is_empty())
            .ok_or_else(|| "receiver name is required".to_string())?;
        if !names.insert(name) {
            return Err(format!("duplicate receiver name {name:?}"));
        }
    }
    if !names.contains(route_receiver) {
        return Err(format!(
            "route references undefined receiver {route_receiver:?}"
        ));
    }
    Ok(())
}

fn default_receiver<S: MetricStore>(
    state: &PrometheusApiState<S>,
    tenant: &krabka_blockstore::TenantId,
) -> Option<String> {
    state
        .alertmanager_configs
        .read()
        .ok()
        .and_then(|configs| configs.get(tenant).cloned())
        .and_then(|body| serde_yaml::from_str::<AlertmanagerConfig>(&body).ok())
        .and_then(|wrapper| {
            serde_yaml::from_str::<serde_yaml::Value>(&wrapper.alertmanager_config).ok()
        })
        .and_then(|config| {
            config
                .get("route")?
                .get("receiver")?
                .as_str()
                .map(str::to_owned)
        })
}

fn enrich_alert(alert: &mut Value, receiver: Option<&str>) {
    let Some(object) = alert.as_object_mut() else {
        return;
    };
    let labels = object
        .get("labels")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let canonical = labels
        .iter()
        .fold(String::new(), |mut output, (name, value)| {
            writeln!(output, "{name}={}", value.as_str().unwrap_or_default())
                .expect("writing to a String cannot fail");
            output
        });
    object.insert(
        "fingerprint".into(),
        Value::String(format!("{:016x}", xxh64(canonical.as_bytes(), 0))),
    );
    object
        .entry("status")
        .or_insert_with(|| json!({"inhibitedBy":[],"silencedBy":[],"state":"active"}));
    object
        .entry("receivers")
        .or_insert_with(|| receiver.map_or_else(|| json!([]), |name| json!([{"name":name}])));
    object
        .entry("updatedAt")
        .or_insert_with(|| Value::String(now_rfc3339()));
}

fn matches_filters(value: &Value, query: Option<&str>) -> bool {
    let Some(query) = query else {
        return true;
    };
    url::form_urlencoded::parse(query.as_bytes())
        .filter(|(name, _)| name == "filter" || name == "receiver")
        .all(|(kind, filter)| {
            if kind == "receiver" {
                return value
                    .get("receivers")
                    .and_then(Value::as_array)
                    .is_some_and(|receivers| {
                        receivers.iter().any(|receiver| {
                            receiver.get("name").and_then(Value::as_str) == Some(filter.as_ref())
                        })
                    });
            }
            let filter = filter.trim_matches(|character| character == '{' || character == '}');
            let Some((name, expected)) = filter.split_once('=') else {
                return true;
            };
            value
                .get("labels")
                .and_then(|labels| labels.get(name.trim()))
                .and_then(Value::as_str)
                == Some(expected.trim().trim_matches('"'))
                || value
                    .get("matchers")
                    .and_then(Value::as_array)
                    .is_some_and(|matchers| {
                        matchers.iter().any(|matcher| {
                            matcher.get("name").and_then(Value::as_str) == Some(name.trim())
                                && matcher.get("value").and_then(Value::as_str)
                                    == Some(expected.trim().trim_matches('"'))
                        })
                    })
        })
}

fn with_silence_state(mut silence: Value) -> Value {
    let now = OffsetDateTime::now_utc();
    let starts = silence
        .get("startsAt")
        .and_then(Value::as_str)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok());
    let ends = silence
        .get("endsAt")
        .and_then(Value::as_str)
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok());
    let state = if ends.is_some_and(|end| end <= now) {
        "expired"
    } else if starts.is_some_and(|start| start > now) {
        "pending"
    } else {
        "active"
    };
    if let Some(object) = silence.as_object_mut() {
        object.insert("status".into(), json!({"state":state}));
    }
    silence
}

fn now_rfc3339() -> String {
    OffsetDateTime::now_utc()
        .format(&Rfc3339)
        .unwrap_or_default()
}

fn no_store(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        axum::http::HeaderValue::from_static("no-store"),
    );
    response
}

fn new_silence_id() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let sequence = NEXT_SILENCE_ID.fetch_add(1, Ordering::Relaxed);
    format!("{nanos:032x}-{sequence:016x}")
}
