use axum::{extract::FromRequestParts, http::request::Parts};
use axum::response::IntoResponse;

use super::{
    loki_yaml_mapping, prometheus_alerts_response, prometheus_rule_groups_response, serde_yaml_key,
    yaml_string_field,
};
use crate::{
    BTreeMap, BTreeSet, Bytes, HeaderMap, HttpQueryError, LokiRuleNamespaces, LokiRuleTenants,
    Path, QuerierState, RawQuery, RequestSecurity, Response, Serialize, State, StatusCode,
    StreamQuery, TenantErrorSurface, TenantId, Value,
    audit::{
        AuditOutcome, AuditResource, OPERATION_RULE_GROUP_DELETE, OPERATION_RULE_GROUP_SET,
        OPERATION_RULE_NAMESPACE_DELETE, RESOURCE_RULE_GROUP, RESOURCE_RULE_NAMESPACE,
        RESOURCE_TENANT, resource,
    },
    authorized_tenant, current_unix_time_ns, json, json_response, text_response,
};


fn prometheus_ruler_success(data: &Value) -> Response {
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

fn status_html_page(title: &str, body: &str) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/html; charset=utf-8")
        .body(
            format!(
                "<!doctype html><html><head><title>{title}</title></head>\
                 <body><h1>{title}</h1>{body}</body></html>"
            )
            .into(),
        )
        .expect("status page headers are valid")
}


fn deleted_rule_response(
    state: &QuerierState,
    #[allow(clippy::needless_pass_by_value)] security: RequestSecurity,
    operation: &'static str,
    resources: Vec<AuditResource>,
    tenant: &str,
    snapshot: &LokiRuleTenants,
) -> Result<Response, HttpQueryError> {
    if let Err(error) = state.rules.persist_snapshot(snapshot) {
        security.admin_operation(operation, resources, AuditOutcome::Failure);
        return Err(HttpQueryError::from(error));
    }
    security.admin_operation(operation, resources, AuditOutcome::Success);
    state.alert_states.clear_tenant(tenant);
    Ok(json_response(
        StatusCode::ACCEPTED,
        &json!({ "status": "success" }),
    ))
}

fn snapshot_after_rule_removal(
    state: &QuerierState,
    tenant: &str,
    remove: impl FnOnce(&mut LokiRuleNamespaces) -> bool,
) -> Option<LokiRuleTenants> {
    let mut rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let namespaces = rules.get_mut(tenant)?;
    if !remove(namespaces) {
        return None;
    }
    if namespaces.is_empty() {
        rules.remove(tenant);
    }
    Some(rules.clone())
}

#[derive(serde::Deserialize)]
pub(crate) struct LokiRuleGroupPath {
    pub(crate) namespace: String,
    pub(crate) group_name: String,
}

pub(crate) async fn loki_page_not_found() -> Response {
    text_response(StatusCode::NOT_FOUND, "404 page not found\n")
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

pub(crate) fn loki_rule_namespace_response(
    namespaces: &LokiRuleNamespaces,
) -> BTreeMap<String, Vec<serde_yaml::Value>> {
    let mut response = BTreeMap::new();
    for namespace in namespaces {
        response.insert(namespace.0.clone(), namespace.1.values().cloned().collect());
    }
    response
}

pub(crate) fn loki_yaml_response(status: StatusCode, value: &impl Serialize) -> Response {
    match serde_yaml::to_string(value) {
        Ok(body) => Response::builder()
            .status(status)
            .header("content-type", "application/yaml; charset=utf-8")
            .body(body.into())
            .expect("YAML response headers are valid"),
        Err(source) => text_response(StatusCode::INTERNAL_SERVER_ERROR, &source.to_string()),
    }
}

pub(crate) fn missing_loki_rule_directory_response(tenant: &str) -> Response {
    text_response(
        StatusCode::BAD_REQUEST,
        &format!(
            "unable to read rule dir /loki/rules/{tenant}: open /loki/rules/{tenant}: no such file or directory\n"
        ),
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

pub(crate) fn parse_loki_rule_group(body: &[u8]) -> Result<serde_yaml::Value, ()> {
    let rule_group = serde_yaml::from_slice(body).map_err(|_| ())?;
    validate_loki_rule_group(&rule_group)?;
    Ok(rule_group)
}

pub(crate) fn ruler_status_page() -> Response {
    status_html_page("Cortex Ruler Status", "")
}

/// The ring status page, describing the one process that serves this role.
///
/// Krabka runs no hash ring: there is no gossip, no token ownership and no
/// second instance to hand a range to. The page stays because `Loki`'s is at
/// this path and an operator's runbook sends them here, but it says what is
/// true rather than rendering a one-member ring as though membership had been
/// checked. `state` comes from the caller's readiness, so a process that is
/// still starting reads `JOINING` here and 503 at `/ready`.
pub(crate) fn ring_status_page(instance: &str, state: &str) -> Response {
    status_html_page(
        "Ring Status",
        &format!(
            "\
         <p>Krabka runs no hash ring. This process serves the role by itself, \
         so the row below is the process answering this request and not a \
         membership that was gossiped, tokenised or health-checked. There are \
         no peers to list, and none to have been forgotten.</p>\
         <table><thead><tr><th>Instance</th><th>State</th></tr></thead>\
         <tbody><tr><td>{instance}</td><td>{state}</td></tr></tbody>\
         </table>"
        ),
    )
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

/// The one tenant a ruler request names, after the principal's grant and the
/// query authorizer allow it.
///
/// Loki's ruler reads the tenant with `dskit`'s `tenant.TenantID`, so a header
/// with two tenants is refused and `a|a` is tenant `a`. With `auth_enabled:
/// true`, which is the `loki_differential` configuration, a request without a
/// tenant gets 401. It is never served as tenant `fake`.
///
/// Every rule route asks the authorizer, the routes that change rule groups
/// too. An unauthenticated caller that could name any tenant could otherwise
/// read, create and delete that tenant's rules. A tenant outside the
/// principal's grant gets a 403 before the authorizer runs.
pub(crate) async fn authorized_ruler_tenant(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    surface: TenantErrorSurface,
) -> Result<TenantId, HttpQueryError> {
    authorized_tenant(state, security, headers, surface).await
}

/// The tenant of a Loki ruler request, authorized as
/// [`authorized_ruler_tenant`] does with the ruler's tenant-error surface.
pub(crate) struct RulerTenant(pub(crate) TenantId);

impl FromRequestParts<QuerierState> for RulerTenant {
    type Rejection = Response;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &QuerierState,
    ) -> Result<Self, Self::Rejection> {
        let security = RequestSecurity::from_request_parts(parts, state)
            .await
            .map_err(IntoResponse::into_response)?;
        authorized_ruler_tenant(state, &security, &parts.headers, TenantErrorSurface::Ruler)
            .await
            .map(Self)
            .map_err(IntoResponse::into_response)
    }
}

/// `POST /loki/api/v1/rules/{namespace}`: create or replace one rule group.
///
/// The audit trail records each attempt to store a group that parsed, with
/// the outcome of the write.
pub(crate) async fn create_loki_rule_group(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    Path(namespace): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let tenant =
        match authorized_ruler_tenant(&state, &security, &headers, TenantErrorSurface::Ruler).await
        {
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
    let resources = vec![
        resource(RESOURCE_TENANT, tenant.as_str()),
        resource(RESOURCE_RULE_GROUP, format!("{namespace}/{name}")),
    ];
    let snapshot = {
        let mut rules = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned");
        rules
            .entry(tenant.as_str().to_owned())
            .or_default()
            .entry(namespace)
            .or_default()
            .insert(name, rule_group);
        rules.clone()
    };
    if let Err(error) = state.rules.persist_snapshot(&snapshot) {
        security.admin_operation(OPERATION_RULE_GROUP_SET, resources, AuditOutcome::Failure);
        return HttpQueryError::from(error).into_response();
    }
    security.admin_operation(OPERATION_RULE_GROUP_SET, resources, AuditOutcome::Success);
    state.alert_states.clear_tenant(tenant.as_str());
    json_response(StatusCode::ACCEPTED, &json!({ "status": "success" }))
}

/// `DELETE /loki/api/v1/rules/{namespace}/{group_name}`: delete one rule group.
///
/// The audit trail records each deletion of a group that exists, with the
/// outcome of the write. A group that does not exist gets 404 and changes
/// nothing, so it records nothing.
pub(crate) async fn delete_loki_rule_group(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    Path(path): Path<LokiRuleGroupPath>,
    RulerTenant(tenant): RulerTenant,
) -> Result<Response, HttpQueryError> {
    let resources = vec![
        resource(RESOURCE_TENANT, tenant.as_str()),
        resource(
            RESOURCE_RULE_GROUP,
            format!("{}/{}", path.namespace, path.group_name),
        ),
    ];
    let Some(snapshot) =
        snapshot_after_rule_removal(&state, tenant.as_str(), |namespaces| {
            let Some(groups) = namespaces.get_mut(&path.namespace) else {
                return false;
            };
            if groups.remove(&path.group_name).is_none() {
                return false;
            }
            if groups.is_empty() {
                namespaces.remove(&path.namespace);
            }
            true
        })
    else {
        return Ok(text_response(
            StatusCode::NOT_FOUND,
            "group does not exist\n",
        ));
    };
    deleted_rule_response(
        &state,
        security,
        OPERATION_RULE_GROUP_DELETE,
        resources,
        tenant.as_str(),
        &snapshot,
    )
}

/// `DELETE /loki/api/v1/rules/{namespace}`: delete every rule group in a
/// namespace.
///
/// The audit trail records each deletion of a namespace that exists, with the
/// outcome of the write. A namespace that does not exist gets 404 and changes
/// nothing, so it records nothing.
pub(crate) async fn delete_loki_rule_namespace(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    Path(namespace): Path<String>,
    RulerTenant(tenant): RulerTenant,
) -> Result<Response, HttpQueryError> {
    let resources = vec![
        resource(RESOURCE_TENANT, tenant.as_str()),
        resource(RESOURCE_RULE_NAMESPACE, namespace.as_str()),
    ];
    let Some(snapshot) =
        snapshot_after_rule_removal(&state, tenant.as_str(), |namespaces| {
            namespaces.remove(&namespace).is_some()
        })
    else {
        return Ok(text_response(
            StatusCode::NOT_FOUND,
            "no rule groups found\n",
        ));
    };
    deleted_rule_response(
        &state,
        security,
        OPERATION_RULE_NAMESPACE_DELETE,
        resources,
        tenant.as_str(),
        &snapshot,
    )
}

pub(crate) async fn loki_rule_group(
    State(state): State<QuerierState>,
    // Extracted ahead of the path, so a request without a principal is
    // refused first; `RulerTenant` reads the principal again.
    _security: RequestSecurity,
    Path(path): Path<LokiRuleGroupPath>,
    RulerTenant(tenant): RulerTenant,
) -> Result<Response, HttpQueryError> {
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let Some(groups) = rules
        .get(tenant.as_str())
        .and_then(|namespaces| namespaces.get(&path.namespace))
    else {
        return Ok(text_response(
            StatusCode::BAD_REQUEST,
            "GetRuleGroup unsupported in rule local store\n",
        ));
    };
    let Some(group) = groups.get(&path.group_name) else {
        return Ok(text_response(
            StatusCode::NOT_FOUND,
            "group does not exist\n",
        ));
    };
    Ok(loki_yaml_response(StatusCode::OK, group))
}

pub(crate) async fn loki_rule_namespace(
    State(state): State<QuerierState>,
    // Extracted ahead of the path, so a request without a principal is
    // refused first; `RulerTenant` reads the principal again.
    _security: RequestSecurity,
    Path(namespace): Path<String>,
    RulerTenant(tenant): RulerTenant,
) -> Result<Response, HttpQueryError> {
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    if !rules.contains_key(tenant.as_str()) {
        return Ok(missing_loki_rule_namespace_response(
            tenant.as_str(),
            &namespace,
        ));
    }
    let Some(groups) = rules
        .get(tenant.as_str())
        .and_then(|namespaces| namespaces.get(&namespace))
    else {
        return Ok(text_response(
            StatusCode::NOT_FOUND,
            "no rule groups found\n",
        ));
    };
    Ok(loki_yaml_response(
        StatusCode::OK,
        &groups.values().cloned().collect::<Vec<_>>(),
    ))
}

pub(crate) async fn loki_rules(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
) -> Response {
    let tenant =
        match authorized_ruler_tenant(&state, &security, &headers, TenantErrorSurface::Ruler).await
        {
            Ok(tenant) => tenant,
            Err(error) => return error.into_response(),
        };
    let rules = state
        .rules
        .tenants
        .lock()
        .expect("Loki rule store lock poisoned");
    let Some(namespaces) = rules.get(tenant.as_str()).map(loki_rule_namespace_response) else {
        return missing_loki_rule_directory_response(tenant.as_str());
    };
    loki_yaml_response(StatusCode::OK, &namespaces)
}

pub(crate) async fn prometheus_alerts(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let PrometheusRulerRequest {
        tenant,
        filters: _,
        evaluation_time,
        namespaces,
    } = match prometheus_ruler_request(&state, &security, &headers, raw_query.as_deref())
        .await
    {
        Ok(request) => request,
        Err(response) => return *response,
    };
    let alerts = match namespaces {
        Some(namespaces) => {
            match prometheus_alerts_response(&state, &tenant, &namespaces, evaluation_time).await {
                Ok(alerts) => alerts,
                Err(error) => return error.into_response(),
            }
        }
        None => Vec::new(),
    };
    prometheus_ruler_success(&json!({ "alerts": alerts }))
}

/// What the Prometheus-compatible rule and alert endpoints read from a
/// request: the tenant, the filters, the evaluation time, and the tenant's
/// rule namespaces, if it has any.
pub(crate) struct PrometheusRulerRequest {
    pub(crate) tenant: TenantId,
    pub(crate) filters: PrometheusRulesFilters,
    pub(crate) evaluation_time: i64,
    pub(crate) namespaces: Option<LokiRuleNamespaces>,
}

/// The parts of a Prometheus-compatible rule or alert request that
/// [`PrometheusRulerRequest`] is resolved from.
pub(crate) struct PrometheusRulerInputs<'a> {
    pub(crate) state: &'a QuerierState,
    pub(crate) security: &'a RequestSecurity,
    pub(crate) headers: &'a HeaderMap,
    pub(crate) raw_query: Option<&'a str>,
}

pub(crate) async fn prometheus_ruler_request(
    state: &QuerierState,
    security: &RequestSecurity,
    headers: &HeaderMap,
    raw_query: Option<&str>,
) -> Result<PrometheusRulerRequest, Box<Response>> {
    PrometheusRulerInputs {
        state,
        security,
        headers,
        raw_query,
    }
    .resolve()
    .await
}

impl PrometheusRulerInputs<'_> {
    /// Authorizes the request's tenant and parses its filters. The error is
    /// the response to send instead.
    pub(crate) async fn resolve(self) -> Result<PrometheusRulerRequest, Box<Response>> {
        let Self {
            state,
            security,
            headers,
            raw_query,
        } = self;
        let tenant = authorized_ruler_tenant(
            state,
            security,
            headers,
            TenantErrorSurface::PrometheusRuler,
        )
        .await
        .map_err(|error| Box::new(error.into_response()))?;
        let filters = PrometheusRulesFilters::parse(raw_query)
            .map_err(|error| Box::new(error.into_response()))?;
        let evaluation_time = filters.evaluation_time.unwrap_or_else(current_unix_time_ns);
        let namespaces = state
            .rules
            .tenants
            .lock()
            .expect("Loki rule store lock poisoned")
            .get(tenant.as_str())
            .cloned();
        Ok(PrometheusRulerRequest {
            tenant,
            filters,
            evaluation_time,
            namespaces,
        })
    }
}

pub(crate) async fn prometheus_rules(
    State(state): State<QuerierState>,
    security: RequestSecurity,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Response {
    let PrometheusRulerRequest {
        tenant,
        filters,
        evaluation_time,
        namespaces,
    } = match prometheus_ruler_request(&state, &security, &headers, raw_query.as_deref())
        .await
    {
        Ok(request) => request,
        Err(response) => return *response,
    };
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
    prometheus_ruler_success(&data)
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
