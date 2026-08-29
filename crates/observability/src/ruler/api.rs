fn ring_status_page(instance: &'static str) -> Response {
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

fn ruler_status_page() -> Response {
    (
        StatusCode::OK,
        [("content-type", "text/html; charset=utf-8")],
        "<!doctype html><html><head><title>Cortex Ruler Status</title></head>\
         <body><h1>Cortex Ruler Status</h1></body></html>",
    )
        .into_response()
}

async fn loki_rules(State(state): State<QuerierState>, headers: HeaderMap) -> Response {
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

async fn loki_page_not_found() -> Response {
    text_response(StatusCode::NOT_FOUND, "404 page not found\n")
}

fn missing_loki_rule_directory_response(tenant: &str) -> Response {
    text_response(
        StatusCode::BAD_REQUEST,
        &format!(
            "unable to read rule dir /loki/rules/{tenant}: open /loki/rules/{tenant}: no such file or directory\n"
        ),
    )
}

async fn loki_rule_namespace(
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

fn missing_loki_rule_namespace_response(tenant: &str, namespace: &str) -> Response {
    text_response(
        StatusCode::BAD_REQUEST,
        &format!(
            "error parsing /loki/rules/{tenant}/{namespace}: /loki/rules/{tenant}/{namespace}: open /loki/rules/{tenant}/{namespace}: no such file or directory\n"
        ),
    )
}

async fn create_loki_rule_group(
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

async fn delete_loki_rule_namespace(
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

async fn loki_rule_group(
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

async fn delete_loki_rule_group(
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

fn loki_ruler_tenant(headers: &HeaderMap) -> Result<String, HttpQueryError> {
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

fn loki_rule_namespace_response(
    namespaces: &LokiRuleNamespaces,
) -> BTreeMap<String, Vec<serde_yaml::Value>> {
    namespaces
        .iter()
        .map(|(namespace, groups)| (namespace.clone(), groups.values().cloned().collect()))
        .collect()
}

fn parse_loki_rule_group(body: &[u8]) -> Result<serde_yaml::Value, ()> {
    let rule_group = serde_yaml::from_slice(body).map_err(|_| ())?;
    validate_loki_rule_group(&rule_group)?;
    Ok(rule_group)
}

fn loki_rule_group_name(rule_group: &serde_yaml::Value) -> Option<&str> {
    let serde_yaml::Value::Mapping(fields) = rule_group else {
        return None;
    };
    fields
        .get(serde_yaml::Value::String("name".to_string()))
        .and_then(serde_yaml::Value::as_str)
        .filter(|name| !name.is_empty())
}

fn validate_loki_rule_group(rule_group: &serde_yaml::Value) -> Result<(), ()> {
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

fn validate_loki_rule(rule: &serde_yaml::Value) -> Result<(), ()> {
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

fn loki_yaml_response(status: StatusCode, value: &impl Serialize) -> Response {
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

async fn prometheus_rules(
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

async fn prometheus_alerts(
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
struct PrometheusRulesFilters {
    rule_kind: Option<&'static str>,
    rule_names: BTreeSet<String>,
    rule_groups: BTreeSet<String>,
    files: BTreeSet<String>,
    label_selectors: Vec<StreamQuery>,
    group_limit: Option<usize>,
    group_next_token: Option<String>,
    exclude_alerts: bool,
    evaluation_time: Option<i64>,
}

impl PrometheusRulesFilters {
    fn parse(raw_query: Option<&str>) -> Result<Self, HttpQueryError> {
        let mut filters = Self::default();
        let Some(raw_query) = raw_query else {
            return Ok(filters);
        };
        for (key, value) in url::form_urlencoded::parse(raw_query.as_bytes()) {
            match key.as_ref() {
                "type" if value == "alert" => filters.rule_kind = Some("alerting"),
                "type" if value == "record" => filters.rule_kind = Some("recording"),
                "exclude_alerts" if value == "true" => filters.exclude_alerts = true,
                "time" if !value.is_empty() => {
                    filters.evaluation_time =
                        Some(parse_loki_timestamp_query_param("time", &value)?);
                }
                "rule_name" | "rule_name[]" if !value.is_empty() => {
                    filters.rule_names.insert(value.into_owned());
                }
                "rule_group" | "rule_group[]" if !value.is_empty() => {
                    filters.rule_groups.insert(value.into_owned());
                }
                "file" | "file[]" if !value.is_empty() => {
                    filters.files.insert(value.into_owned());
                }
                "group_limit" if !value.is_empty() => {
                    filters.group_limit = Some(parse_usize_query_param("group_limit", &value)?);
                }
                "group_next_token" if !value.is_empty() => {
                    filters.group_next_token = Some(value.into_owned());
                }
                "match" | "match[]" if !value.is_empty() => {
                    let selector = value.into_owned();
                    filters
                        .label_selectors
                        .push(parse_query(&selector).map_err(|source| {
                            HttpQueryError::LokiParse {
                                query: selector.clone(),
                                source,
                            }
                        })?);
                }
                _ => {}
            }
        }
        if filters.group_next_token.is_some() && filters.group_limit.is_none() {
            return Err(HttpQueryError::MissingQueryParameter("group_limit"));
        }
        Ok(filters)
    }

    fn has_rule_filter(&self) -> bool {
        self.rule_kind.is_some() || !self.rule_names.is_empty() || !self.label_selectors.is_empty()
    }

    fn matches_rule(&self, rule: &Value, source_rule: &serde_yaml::Value) -> bool {
        if self
            .rule_kind
            .is_some_and(|kind| rule.get("type").and_then(Value::as_str) != Some(kind))
        {
            return false;
        }
        if !self.rule_names.is_empty()
            && !rule
                .get("name")
                .and_then(Value::as_str)
                .is_some_and(|name| self.rule_names.contains(name))
        {
            return false;
        }
        self.matches_rule_labels(source_rule)
    }

    fn matches_rule_labels(&self, source_rule: &serde_yaml::Value) -> bool {
        if self.label_selectors.is_empty() {
            return true;
        }
        let labels = loki_yaml_mapping(source_rule)
            .map(|fields| yaml_string_labels_field(fields, "labels"))
            .unwrap_or_default();
        self.label_selectors.iter().any(|selector| {
            selector
                .matchers
                .iter()
                .all(|matcher| matcher.matches(&labels))
        })
    }

    fn page_groups(
        &self,
        groups: Vec<PrometheusRuleGroupResponse>,
    ) -> Result<PrometheusRulesPage, HttpQueryError> {
        let start_index = match &self.group_next_token {
            Some(token) => groups
                .iter()
                .position(|group| group.token == *token)
                .map(|index| index + 1)
                .ok_or_else(|| HttpQueryError::InvalidQueryParameter {
                    name: "group_next_token",
                    value: token.clone(),
                })?,
            None => 0,
        };
        let Some(limit) = self.group_limit else {
            return Ok(PrometheusRulesPage {
                groups: groups
                    .into_iter()
                    .skip(start_index)
                    .map(|group| group.value)
                    .collect(),
                next_token: None,
            });
        };
        let next_token = (groups.len() > start_index.saturating_add(limit) && limit > 0)
            .then(|| groups[start_index + limit - 1].token.clone());
        Ok(PrometheusRulesPage {
            groups: groups
                .into_iter()
                .skip(start_index)
                .take(limit)
                .map(|group| group.value)
                .collect(),
            next_token,
        })
    }
}

#[derive(Default)]
struct PrometheusRulesPage {
    groups: Vec<Value>,
    next_token: Option<String>,
}

struct PrometheusRuleGroupResponse {
    token: String,
    value: Value,
}

async fn prometheus_rule_groups_response(
    state: &QuerierState,
    tenant: &str,
    namespaces: &LokiRuleNamespaces,
    filters: &PrometheusRulesFilters,
    evaluation_time: i64,
) -> Result<PrometheusRulesPage, HttpQueryError> {
    let mut response_groups = Vec::new();
    for (namespace, groups) in namespaces {
        if !filters.files.is_empty() && !filters.files.contains(namespace) {
            continue;
        }
        for group in groups.values() {
            let Some(name) = loki_rule_group_name(group) else {
                continue;
            };
            if !filters.rule_groups.is_empty() && !filters.rule_groups.contains(name) {
                continue;
            }
            let rules =
                prometheus_rules_for_group(state, tenant, group, filters, evaluation_time).await?;
            if filters.has_rule_filter() && rules.is_empty() {
                continue;
            }
            response_groups.push(PrometheusRuleGroupResponse {
                token: prometheus_rule_group_page_token(namespace, name),
                value: json!({
                    "name": name,
                    "file": namespace,
                    "interval": prometheus_rule_group_interval_seconds(group),
                    "limit": 0,
                    "rules": rules,
                }),
            });
        }
    }
    filters.page_groups(response_groups)
}

fn prometheus_rule_group_page_token(namespace: &str, group_name: &str) -> String {
    URL_SAFE_NO_PAD.encode(format!("{namespace}\n{group_name}"))
}

async fn prometheus_rules_for_group(
    state: &QuerierState,
    tenant: &str,
    group: &serde_yaml::Value,
    filters: &PrometheusRulesFilters,
    evaluation_time: i64,
) -> Result<Vec<Value>, HttpQueryError> {
    let mut response_rules = Vec::new();
    let Some(rules) = loki_yaml_mapping(group)
        .and_then(|fields| fields.get(serde_yaml_key("rules")))
        .and_then(serde_yaml::Value::as_sequence)
    else {
        return Ok(response_rules);
    };
    for source_rule in rules {
        let Some(mut rule) = prometheus_rule_response(source_rule) else {
            continue;
        };
        if !filters.matches_rule(&rule, source_rule) {
            continue;
        }
        if !filters.exclude_alerts && rule.get("type").and_then(Value::as_str) == Some("alerting") {
            let alerts =
                prometheus_alerts_for_rule(state, tenant, source_rule, evaluation_time).await?;
            rule["alerts"] = json!(alerts);
        }
        response_rules.push(rule);
    }
    Ok(response_rules)
}

fn prometheus_rule_response(rule: &serde_yaml::Value) -> Option<Value> {
    let fields = loki_yaml_mapping(rule)?;
    let query = yaml_string_field(fields, "expr")?;
    if let Some(name) = yaml_string_field(fields, "alert") {
        let mut rule = json!({
            "type": "alerting",
            "name": name,
            "query": query,
            "duration": yaml_duration_seconds_field(fields, "for").unwrap_or(0),
            "labels": yaml_string_map_field(fields, "labels"),
            "annotations": yaml_string_map_field(fields, "annotations"),
            "alerts": [],
            "health": "ok",
        });
        remove_empty_object_field(&mut rule, "labels");
        remove_empty_object_field(&mut rule, "annotations");
        return Some(rule);
    }
    yaml_string_field(fields, "record").map(|name| {
        let mut rule = json!({
            "type": "recording",
            "name": name,
            "query": query,
            "labels": yaml_string_map_field(fields, "labels"),
            "health": "ok",
        });
        remove_empty_object_field(&mut rule, "labels");
        rule
    })
}

fn prometheus_rule_group_interval_seconds(group: &serde_yaml::Value) -> i64 {
    loki_yaml_mapping(group)
        .and_then(|fields| yaml_duration_seconds_field(fields, "interval"))
        .unwrap_or(0)
}

fn yaml_duration_seconds_field(fields: &serde_yaml::Mapping, name: &'static str) -> Option<i64> {
    yaml_duration_ns_field(fields, name)
        .and_then(|duration_ns| duration_ns.checked_div(1_000_000_000))
}

fn yaml_duration_ns_field(fields: &serde_yaml::Mapping, name: &'static str) -> Option<i64> {
    let duration = yaml_string_field(fields, name)?;
    parse_prometheus_duration(duration)
}

fn yaml_string_field<'a>(fields: &'a serde_yaml::Mapping, name: &'static str) -> Option<&'a str> {
    fields
        .get(serde_yaml_key(name))
        .and_then(serde_yaml::Value::as_str)
}

fn yaml_string_map_field(fields: &serde_yaml::Mapping, name: &'static str) -> Value {
    let values = fields
        .get(serde_yaml_key(name))
        .and_then(loki_yaml_mapping)
        .map(|values| {
            values
                .iter()
                .filter_map(|(key, value)| {
                    Some((key.as_str()?.to_string(), json!(value.as_str()?)))
                })
                .collect::<serde_json::Map<_, _>>()
        })
        .unwrap_or_default();
    Value::Object(values)
}

fn yaml_string_template_map_field(fields: &serde_yaml::Mapping, name: &'static str) -> Labels {
    fields
        .get(serde_yaml_key(name))
        .and_then(loki_yaml_mapping)
        .map(|values| {
            values
                .iter()
                .filter_map(|(key, value)| {
                    Some((key.as_str()?.to_string(), value.as_str()?.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn yaml_string_labels_field(fields: &serde_yaml::Mapping, name: &'static str) -> Labels {
    fields
        .get(serde_yaml_key(name))
        .and_then(loki_yaml_mapping)
        .map(|values| {
            values
                .iter()
                .filter_map(|(key, value)| {
                    Some((key.as_str()?.to_string(), value.as_str()?.to_string()))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn expand_prometheus_alert_template(template: &str, labels: &Labels, value: &str) -> String {
    let mut expanded = String::with_capacity(template.len());
    let mut remaining = template;
    while let Some(start) = remaining.find("{{") {
        expanded.push_str(&remaining[..start]);
        let action_start = start + "{{".len();
        let action = &remaining[action_start..];
        let Some(end) = action.find("}}") else {
            expanded.push_str(&remaining[start..]);
            return expanded;
        };
        let expression = action[..end].trim();
        if expression == "$value" {
            expanded.push_str(value);
        } else if let Some(name) = expression.strip_prefix("$labels.") {
            if let Some(label_value) = labels.get(name) {
                expanded.push_str(label_value);
            } else {
                expanded.push_str("{{");
                expanded.push_str(&action[..end]);
                expanded.push_str("}}");
            }
        } else {
            expanded.push_str("{{");
            expanded.push_str(&action[..end]);
            expanded.push_str("}}");
        }
        remaining = &action[end + "}}".len()..];
    }
    expanded.push_str(remaining);
    expanded
}

fn prometheus_alert_template_map(templates: &Labels, labels: &Labels, value: &str) -> Value {
    Value::Object(
        templates
            .iter()
            .map(|(key, template)| {
                (
                    key.clone(),
                    json!(expand_prometheus_alert_template(template, labels, value)),
                )
            })
            .collect(),
    )
}

fn loki_yaml_mapping(value: &serde_yaml::Value) -> Option<&serde_yaml::Mapping> {
    match value {
        serde_yaml::Value::Mapping(fields) => Some(fields),
        _ => None,
    }
}

fn serde_yaml_key(value: &'static str) -> serde_yaml::Value {
    serde_yaml::Value::String(value.to_string())
}

fn remove_empty_object_field(value: &mut Value, field: &'static str) {
    let Some(fields) = value.as_object_mut() else {
        return;
    };
    if fields
        .get(field)
        .and_then(Value::as_object)
        .is_some_and(serde_json::Map::is_empty)
    {
        fields.remove(field);
    }
}

async fn prometheus_alerts_response(
    state: &QuerierState,
    tenant: &str,
    namespaces: &LokiRuleNamespaces,
    evaluation_time: i64,
) -> Result<Vec<Value>, HttpQueryError> {
    let mut alerts = Vec::new();
    for groups in namespaces.values() {
        for group in groups.values() {
            let Some(rules) = loki_yaml_mapping(group)
                .and_then(|fields| fields.get(serde_yaml_key("rules")))
                .and_then(serde_yaml::Value::as_sequence)
            else {
                continue;
            };
            for rule in rules {
                alerts.extend(
                    prometheus_alerts_for_rule(state, tenant, rule, evaluation_time).await?,
                );
            }
        }
    }
    Ok(alerts)
}

async fn prometheus_alerts_for_rule(
    state: &QuerierState,
    tenant: &str,
    rule: &serde_yaml::Value,
    evaluation_time: i64,
) -> Result<Vec<Value>, HttpQueryError> {
    let Some(fields) = loki_yaml_mapping(rule) else {
        return Ok(Vec::new());
    };
    let Some(alert_name) = yaml_string_field(fields, "alert") else {
        return Ok(Vec::new());
    };
    let Some(query) = yaml_string_field(fields, "expr") else {
        return Ok(Vec::new());
    };
    let params = QueryParams {
        query: query.to_string(),
        time: Some(evaluation_time),
        start: None,
        end: None,
        since: None,
        step: None,
        interval: None,
        limit: None,
        direction: None,
        delay_for: None,
    };
    let result = execute_http_query_for_tenant(state, tenant, &params, QueryKind::Instant).await?;
    Ok(prometheus_alerts_from_query_result(
        &state.alert_states,
        tenant,
        alert_name,
        fields,
        query,
        evaluation_time,
        &result,
    ))
}

fn prometheus_alerts_from_query_result(
    alert_states: &SharedPrometheusAlertStates,
    tenant: &str,
    alert_name: &str,
    fields: &serde_yaml::Mapping,
    query: &str,
    evaluation_time: i64,
    result: &Value,
) -> Vec<Value> {
    let hold_duration_ns = yaml_duration_ns_field(fields, "for").unwrap_or(0);
    let keep_firing_for_ns = yaml_duration_ns_field(fields, "keep_firing_for").unwrap_or(0);
    let annotation_templates = yaml_string_template_map_field(fields, "annotations");
    let rule_label_templates = yaml_string_template_map_field(fields, "labels");
    let samples = result
        .pointer("/data/result")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|sample| {
            let value = sample
                .get("value")
                .and_then(Value::as_array)
                .and_then(|value| value.get(1))
                .and_then(Value::as_str)?;
            let mut labels = BTreeMap::new();
            if let Some(metric) = sample.get("metric").and_then(Value::as_object) {
                for (key, value) in metric {
                    if let Some(value) = value.as_str() {
                        labels.insert(key.clone(), value.to_string());
                    }
                }
            }
            for (key, template) in &rule_label_templates {
                labels.insert(
                    key.clone(),
                    expand_prometheus_alert_template(template, &labels, value),
                );
            }
            labels.insert("alertname".to_string(), alert_name.to_string());
            Some((labels, value.to_string()))
        })
        .collect::<Vec<_>>();

    let mut states = alert_states
        .alerts
        .lock()
        .expect("Prometheus alert state lock poisoned");
    let mut active_keys = BTreeSet::new();
    let mut alerts = samples
        .into_iter()
        .map(|(labels, value)| {
            let key = PrometheusAlertKey {
                tenant: tenant.to_string(),
                alert_name: alert_name.to_string(),
                query: query.to_string(),
                labels: labels.clone(),
            };
            let alert = states
                .entry(key.clone())
                .or_insert_with(|| PrometheusAlertRuntimeState {
                    active_at: evaluation_time,
                    last_active_at: evaluation_time,
                    value: value.clone(),
            });
            alert.last_active_at = evaluation_time;
            alert.value.clone_from(&value);
            let state = if evaluation_time.saturating_sub(alert.active_at) >= hold_duration_ns {
                "firing"
            } else {
                "pending"
            };
            active_keys.insert(key);
            json!({
                "activeAt": prometheus_active_at(alert.active_at),
                "annotations": prometheus_alert_template_map(&annotation_templates, &labels, &value),
                "labels": labels,
                "state": state,
                "value": value,
            })
        })
        .collect::<Vec<_>>();

    let (retained_alerts, retained_keys) = retained_prometheus_alerts(
        &states,
        &PrometheusRetainedAlertParams {
            tenant,
            alert_name,
            query,
            evaluation_time,
            hold_duration_ns,
            keep_firing_for_ns,
            active_keys: &active_keys,
            annotation_templates: &annotation_templates,
        },
    );
    alerts.extend(retained_alerts);

    states.retain(|key, _| {
        key.tenant != tenant
            || key.alert_name != alert_name
            || key.query != query
            || active_keys.contains(key)
            || retained_keys.contains(key)
    });
    alerts
}

struct PrometheusRetainedAlertParams<'a> {
    tenant: &'a str,
    alert_name: &'a str,
    query: &'a str,
    evaluation_time: i64,
    hold_duration_ns: i64,
    keep_firing_for_ns: i64,
    active_keys: &'a BTreeSet<PrometheusAlertKey>,
    annotation_templates: &'a Labels,
}

fn retained_prometheus_alerts(
    states: &BTreeMap<PrometheusAlertKey, PrometheusAlertRuntimeState>,
    params: &PrometheusRetainedAlertParams<'_>,
) -> (Vec<Value>, BTreeSet<PrometheusAlertKey>) {
    let mut retained_alerts = Vec::new();
    let mut retained_keys = BTreeSet::new();
    for (key, alert) in states {
        if !prometheus_alert_key_matches_rule(key, params) {
            continue;
        }
        let was_firing =
            alert.last_active_at.saturating_sub(alert.active_at) >= params.hold_duration_ns;
        let within_keep_firing = params.evaluation_time.saturating_sub(alert.last_active_at)
            <= params.keep_firing_for_ns;
        if was_firing && within_keep_firing {
            retained_keys.insert(key.clone());
            retained_alerts.push(json!({
                "activeAt": prometheus_active_at(alert.active_at),
                "annotations": prometheus_alert_template_map(
                    params.annotation_templates,
                    &key.labels,
                    &alert.value,
                ),
                "labels": key.labels,
                "state": "firing",
                "value": alert.value,
            }));
        }
    }
    (retained_alerts, retained_keys)
}

fn prometheus_alert_key_matches_rule(
    key: &PrometheusAlertKey,
    params: &PrometheusRetainedAlertParams<'_>,
) -> bool {
    key.tenant == params.tenant
        && key.alert_name == params.alert_name
        && key.query == params.query
        && !params.active_keys.contains(key)
}

fn prometheus_active_at(timestamp_ns: i64) -> String {
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(timestamp_ns))
        .ok()
        .and_then(|timestamp| timestamp.format(&Rfc3339).ok())
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

