use crate::json;
use crate::{
    BTreeMap, BTreeSet, Bytes, HeaderMap, HttpQueryError, LokiRuleNamespaces, Path, QuerierState,
    RawQuery, Response, Serialize, State, StatusCode, StreamQuery, current_unix_time_ns,
    json_response, text_response,
};
use axum::response::IntoResponse;

pub(crate) fn ring_status_page(instance: &'static str) -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        format!(
            "<!doctype html><html><head><title>Ring Status</title></head>\
         <body><h1>Ring Status</h1>\
         <table><thead><tr><th>Instance</th><th>State</th></tr></thead>\
         <tbody><tr><td>{instance}</td><td>ACTIVE</td></tr></tbody>\
         </table></body></html>"
        ),
    )
        .into_response()
}

pub(crate) fn ruler_status_page() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        "<!doctype html><html><head><title>Cortex Ruler Status</title></head>\
         <body><h1>Cortex Ruler Status</h1></body></html>",
    )
        .into_response()
}

pub(crate) async fn loki_rules(State(state): State<QuerierState>, headers: HeaderMap) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let Some(namespaces) = rules.get(&tenant).map(loki_rule_namespace_response) else {
        return missing_loki_rule_directory_response(&tenant);
    };
    loki_yaml_response(StatusCode::OK, &namespaces)
}

pub(crate) async fn loki_page_not_found() -> Response {
    text_response(StatusCode::NOT_FOUND, "404 page not found\n")
}

pub(crate) fn missing_loki_rule_directory_response(tenant: &str) -> Response {
    text_response(
        StatusCode::BAD_REQUEST,
        &format!(
            "unable to read rule dir /loki/rules/{tenant}: open /loki/rules/{tenant}: no such file or directory\n"
        ),
    )
}

pub(crate) async fn loki_rule_namespace(
    State(state): State<QuerierState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    if !rules.contains_key(&tenant) {
        return missing_loki_rule_namespace_response(&tenant, &namespace);
    }
    let Some(groups) = rules
        .get(&tenant)
        .and_then(|namespaces| namespaces.get(&namespace))
    else {
        return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
    };
    loki_yaml_response(
        StatusCode::OK,
        &groups.values().cloned().collect::<Vec<_>>(),
    )
}

pub(crate) fn missing_loki_rule_namespace_response(tenant: &str, namespace: &str) -> Response {
    text_response(
        StatusCode::BAD_REQUEST,
        &format!(
            "error parsing /loki/rules/{tenant}/{namespace}: /loki/rules/{tenant}/{namespace}: open /loki/rules/{tenant}/{namespace}: no such file or directory\n"
        ),
    )
}

pub(crate) async fn create_loki_rule_group(
    State(state): State<QuerierState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let Ok(rule_group) = parse_loki_rule_group(&body) else {
        return text_response(StatusCode::BAD_REQUEST, "unable to decoded rule group\n");
    };
    let name = match loki_rule_group_name(&rule_group) {
        Some(name) => name.to_string(),
        None => return text_response(StatusCode::BAD_REQUEST, "unable to decoded rule group\n"),
    };
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        rules
            .entry(tenant.clone())
            .or_default()
            .entry(namespace)
            .or_default()
            .insert(name, rule_group);
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        return HttpQueryError::from(error).into_response();
    }
    state.alert_states.clear_tenant(&tenant);
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}

pub(crate) async fn delete_loki_rule_namespace(
    State(state): State<QuerierState>,
    Path(namespace): Path<String>,
    headers: HeaderMap,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        let Some(namespaces) = rules.get_mut(&tenant) else {
            return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
        };
        if namespaces.remove(&namespace).is_none() {
            return text_response(StatusCode::NOT_FOUND, "no rule groups found\n");
        }
        if namespaces.is_empty() {
            rules.remove(&tenant);
        }
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        return HttpQueryError::from(error).into_response();
    }
    state.alert_states.clear_tenant(&tenant);
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}

pub(crate) async fn loki_rule_group(
    State(state): State<QuerierState>,
    Path((namespace, group_name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let Some(groups) = rules
        .get(&tenant)
        .and_then(|namespaces| namespaces.get(&namespace))
    else {
        return text_response(
            StatusCode::BAD_REQUEST,
            "GetRuleGroup unsupported in rule local store\n",
        );
    };
    let Some(group) = groups.get(&group_name) else {
        return text_response(StatusCode::NOT_FOUND, "group does not exist\n");
    };
    loki_yaml_response(StatusCode::OK, group)
}

pub(crate) async fn delete_loki_rule_group(
    State(state): State<QuerierState>,
    Path((namespace, group_name)): Path<(String, String)>,
    headers: HeaderMap,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        let Some(namespaces) = rules.get_mut(&tenant) else {
            return text_response(StatusCode::NOT_FOUND, "group does not exist\n");
        };
        let Some(groups) = namespaces.get_mut(&namespace) else {
            return text_response(StatusCode::NOT_FOUND, "group does not exist\n");
        };
        if groups.remove(&group_name).is_none() {
            return text_response(StatusCode::NOT_FOUND, "group does not exist\n");
        }
        if groups.is_empty() {
            namespaces.remove(&namespace);
        }
        if namespaces.is_empty() {
            rules.remove(&tenant);
        }
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        return HttpQueryError::from(error).into_response();
    }
    state.alert_states.clear_tenant(&tenant);
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}

pub(crate) fn loki_ruler_tenant(headers: &HeaderMap) -> Result<String, HttpQueryError> {
    match headers.get("X-Scope-OrgID") {
        Some(value) => {
            let tenant = value.to_str().map_err(|_| HttpQueryError::InvalidTenant)?;
            if tenant.is_empty() {
                Err(HttpQueryError::InvalidTenant)
            } else {
                Ok(tenant.to_string())
            }
        }
        None => Ok("fake".to_string()),
    }
}

pub(crate) fn loki_rule_namespace_response(
    namespaces: &LokiRuleNamespaces,
) -> BTreeMap<String, Vec<serde_yaml::Value>> {
    namespaces
        .iter()
        .map(|(namespace, groups)| (namespace.clone(), groups.values().cloned().collect()))
        .collect()
}

pub(crate) fn parse_loki_rule_group(body: &[u8]) -> Result<serde_yaml::Value, ()> {
    let rule_group = serde_yaml::from_slice(body).map_err(|_| ())?;
    validate_loki_rule_group(&rule_group)?;
    Ok(rule_group)
}

pub(crate) fn loki_rule_group_name(rule_group: &serde_yaml::Value) -> Option<&str> {
    let serde_yaml::Value::Mapping(fields) = rule_group else {
        return None;
    };
    fields
        .get(serde_yaml::Value::String("name".to_string()))
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
}

pub(crate) fn validate_loki_rule_group(rule_group: &serde_yaml::Value) -> Result<(), ()> {
    let fields = loki_yaml_mapping(rule_group).ok_or(())?;
    if loki_rule_group_name(rule_group).is_none() {
        return Err(());
    }
    let rules = fields
        .get(serde_yaml_key("rules"))
        .and_then(serde_yaml::Value::as_sequence)
        .ok_or(())?;
    for rule in rules {
        validate_loki_rule(rule)?;
    }
    Ok(())
}

pub(crate) fn validate_loki_rule(rule: &serde_yaml::Value) -> Result<(), ()> {
    let fields = loki_yaml_mapping(rule).ok_or(())?;
    yaml_string_field(fields, "expr")
        .filter(|expr| !expr.is_empty())
        .ok_or(())?;
    let is_alert = yaml_string_field(fields, "alert").is_some_and(|name| !name.is_empty());
    let is_record = yaml_string_field(fields, "record").is_some_and(|name| !name.is_empty());
    if is_alert == is_record {
        return Err(());
    }
    Ok(())
}

pub(crate) fn loki_yaml_response(status: StatusCode, value: &impl Serialize) -> Response {
    match serde_yaml::to_string(value) {
        Ok(body) => (
            status,
            [("content-type", "application/yaml; charset=utf-8")],
            body,
        )
            .into_response(),
        Err(source) => text_response(StatusCode::INTERNAL_SERVER_ERROR, &source.to_string()),
    }
}

pub(crate) async fn prometheus_rules(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let filters = match PrometheusRulesFilters::parse(raw_query.as_deref()) {
        Ok(filters) => filters,
        Err(error) => return error.into_response(),
    };
    let evaluation_time = filters.evaluation_time.unwrap_or_else(current_unix_time_ns);
    let namespaces = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned")
        .get(&tenant)
        .cloned();
    let page = match namespaces {
        Some(namespaces) => {
            match prometheus_rule_groups_response(
                &state,
                &tenant,
                &namespaces,
                &filters,
                evaluation_time,
            )
            .await
            {
                Ok(page) => page,
                Err(error) => return error.into_response(),
            }
        }
        None => match filters.page_groups(Vec::new()) {
            Ok(page) => page,
            Err(error) => return error.into_response(),
        },
    };
    let mut data = json!({
        "groups": page.groups
    });
    if let Some(token) = page.next_token {
        data["groupNextToken"] = json!(token);
    }
    json_response(
        StatusCode::OK,
        &json!({
            "status": "success",
            "data": data,
            "errorType": "",
            "error": "",
        }),
    )
}

pub(crate) async fn prometheus_alerts(
    State(state): State<QuerierState>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let tenant = match loki_ruler_tenant(&headers) {
        Ok(tenant) => tenant,
        Err(error) => return error.into_response(),
    };
    let filters = match PrometheusRulesFilters::parse(raw_query.as_deref()) {
        Ok(filters) => filters,
        Err(error) => return error.into_response(),
    };
    let evaluation_time = filters.evaluation_time.unwrap_or_else(current_unix_time_ns);
    let namespaces = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned")
        .get(&tenant)
        .cloned();
    let alerts = match namespaces {
        Some(namespaces) => {
            match prometheus_alerts_response(&state, &tenant, &namespaces, evaluation_time).await {
                Ok(alerts) => alerts,
                Err(error) => return error.into_response(),
            }
        }
        None => Vec::new(),
    };
    json_response(
        StatusCode::OK,
        &json!({
            "status": "success",
            "data": {
                "alerts": alerts
            },
            "errorType": "",
            "error": "",
        }),
    )
}

#[derive(Debug, Default, PartialEq)]
pub(crate) struct PrometheusRulesFilters {
    pub(crate) rule_kind: Option<&'static str>,
    pub(crate) rule_names: BTreeSet<String>,
    pub(crate) rule_groups: BTreeSet<String>,
    pub(crate) files: BTreeSet<String>,
    pub(crate) label_selectors: Vec<StreamQuery>,
    pub(crate) group_limit: Option<usize>,
    pub(crate) group_next_token: Option<String>,
    pub(crate) exclude_alerts: bool,
    pub(crate) evaluation_time: Option<i64>,
}
use super::{
    loki_yaml_mapping, prometheus_alerts_response, prometheus_rule_groups_response, serde_yaml_key,
    yaml_string_field,
};
