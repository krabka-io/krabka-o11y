use super::*;

impl PrometheusRulesFilters {
    pub(crate) fn parse(raw_query: Option<&str>) -> Result<Self, HttpQueryError> {
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

    pub(crate) fn has_rule_filter(&self) -> bool {
        self.rule_kind.is_some() || !self.rule_names.is_empty() || !self.label_selectors.is_empty()
    }

    pub(crate) fn matches_rule(&self, rule: &Value, source_rule: &serde_yaml::Value) -> bool {
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

    pub(crate) fn matches_rule_labels(&self, source_rule: &serde_yaml::Value) -> bool {
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

    pub(crate) fn page_groups(
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
pub(crate) struct PrometheusRulesPage {
    pub(crate) groups: Vec<Value>,
    pub(crate) next_token: Option<String>,
}

pub(crate) struct PrometheusRuleGroupResponse {
    pub(crate) token: String,
    pub(crate) value: Value,
}

pub(crate) async fn prometheus_rule_groups_response(
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

pub(crate) fn prometheus_rule_group_page_token(namespace: &str, group_name: &str) -> String {
    URL_SAFE_NO_PAD.encode(format!("{namespace}\n{group_name}"))
}

pub(crate) async fn prometheus_rules_for_group(
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

pub(crate) fn prometheus_rule_response(rule: &serde_yaml::Value) -> Option<Value> {
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

pub(crate) fn prometheus_rule_group_interval_seconds(group: &serde_yaml::Value) -> i64 {
    loki_yaml_mapping(group)
        .and_then(|fields| yaml_duration_seconds_field(fields, "interval"))
        .unwrap_or(0)
}

pub(crate) fn yaml_duration_seconds_field(
    fields: &serde_yaml::Mapping,
    name: &'static str,
) -> Option<i64> {
    yaml_duration_ns_field(fields, name)
        .and_then(|duration_ns| duration_ns.checked_div(1_000_000_000))
}

pub(crate) fn yaml_duration_ns_field(
    fields: &serde_yaml::Mapping,
    name: &'static str,
) -> Option<i64> {
    let duration = yaml_string_field(fields, name)?;
    parse_prometheus_duration(duration)
}

pub(crate) fn yaml_string_field<'a>(
    fields: &'a serde_yaml::Mapping,
    name: &'static str,
) -> Option<&'a str> {
    fields
        .get(serde_yaml_key(name))
        .and_then(serde_yaml::Value::as_str)
}

pub(crate) fn yaml_string_map_field(fields: &serde_yaml::Mapping, name: &'static str) -> Value {
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

pub(crate) fn yaml_string_template_map_field(
    fields: &serde_yaml::Mapping,
    name: &'static str,
) -> Labels {
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

pub(crate) fn yaml_string_labels_field(fields: &serde_yaml::Mapping, name: &'static str) -> Labels {
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

pub(crate) fn expand_prometheus_alert_template(
    template: &str,
    labels: &Labels,
    value: &str,
) -> String {
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

pub(crate) fn prometheus_alert_template_map(
    templates: &Labels,
    labels: &Labels,
    value: &str,
) -> Value {
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

pub(crate) fn loki_yaml_mapping(value: &serde_yaml::Value) -> Option<&serde_yaml::Mapping> {
    match value {
        serde_yaml::Value::Mapping(fields) => Some(fields),
        _ => None,
    }
}

pub(crate) fn serde_yaml_key(value: &'static str) -> serde_yaml::Value {
    serde_yaml::Value::String(value.to_string())
}

pub(crate) fn remove_empty_object_field(value: &mut Value, field: &'static str) {
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

pub(crate) async fn prometheus_alerts_response(
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

pub(crate) async fn prometheus_alerts_for_rule(
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
