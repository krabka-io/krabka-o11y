use super::{
    HttpQueryError, PrometheusRuleGroupResponse, PrometheusRulesFilters, PrometheusRulesPage,
    Value, loki_yaml_mapping, parse_loki_timestamp_query_param, parse_query,
    parse_usize_query_param, yaml_string_labels_field,
};

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
