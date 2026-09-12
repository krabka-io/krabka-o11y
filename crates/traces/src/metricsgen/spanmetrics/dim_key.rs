use super::{SpanMetricsConfig, SpanRecord, sorted_labels, span_kind_dim, status_dim};
use crate::metricsgen::config::{FilterPolicy, MatchType};

pub(crate) type DimKey = Vec<(String, String)>;

pub(crate) fn dim_key(
    span: &SpanRecord,
    include_status_message: bool,
    config: &SpanMetricsConfig,
) -> DimKey {
    let mut labels = vec![
        ("service".to_string(), span.service_name.clone()),
        ("span_name".to_string(), span.name.clone()),
        (
            "span_kind".to_string(),
            span_kind_dim(span.kind).to_string(),
        ),
        (
            "status_code".to_string(),
            status_dim(span.status).to_string(),
        ),
    ];
    if include_status_message {
        labels.push(("status_message".to_string(), span.status_message.clone()));
    }
    labels.extend(config.dimensions.iter().filter_map(|name| {
        span_attr(span, name).map(|value| (prometheus_label_name(name), value.to_string()))
    }));
    labels.extend(config.dimension_mappings.iter().filter_map(|mapping| {
        let values = mapping
            .source_labels
            .iter()
            .filter_map(|name| span_attr(span, name))
            .collect::<Vec<_>>();
        (!values.is_empty()).then(|| (mapping.name.clone(), values.join(&mapping.join)))
    }));
    sorted_labels(labels)
}

pub(crate) fn span_allowed(span: &SpanRecord, config: &SpanMetricsConfig) -> bool {
    span_allowed_by_policies(span, &config.include, &config.include_any, &config.exclude)
}

pub(crate) fn span_allowed_by_policies(
    span: &SpanRecord,
    include: &[FilterPolicy],
    include_any: &[FilterPolicy],
    exclude: &[FilterPolicy],
) -> bool {
    !exclude.iter().any(|policy| policy_matches(span, policy))
        && (include.is_empty() || include.iter().all(|policy| policy_matches(span, policy)))
        && (include_any.is_empty()
            || include_any
                .iter()
                .any(|policy| policy_matches(span, policy)))
}

pub(crate) fn span_multiplier(span: &SpanRecord, key: Option<&str>) -> f64 {
    key.and_then(|key| span_attr(span, key))
        .and_then(|value| value.parse::<f64>().ok())
        .filter(|value| value.is_finite() && *value >= 0.0)
        .unwrap_or(1.0)
}

pub(crate) fn span_attr<'a>(span: &'a SpanRecord, key: &str) -> Option<&'a str> {
    match key {
        "service.name" | "service" => Some(&span.service_name),
        "span.name" | "span_name" => Some(&span.name),
        _ => span
            .attributes
            .iter()
            .chain(&span.resource_attributes)
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.as_str()),
    }
}

fn policy_matches(span: &SpanRecord, policy: &FilterPolicy) -> bool {
    policy.attributes.iter().all(|attribute| {
        let Some(actual) = span_attr(span, &attribute.key) else {
            return false;
        };
        match policy.match_type {
            MatchType::Strict => actual == attribute.value,
            MatchType::Regex => {
                regex::Regex::new(&attribute.value).is_ok_and(|pattern| pattern.is_match(actual))
            }
        }
    })
}

pub(crate) fn prometheus_label_name(name: &str) -> String {
    let mut label: String = name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect();
    if label.starts_with(|ch: char| ch.is_ascii_digit()) {
        label.insert(0, '_');
    }
    label
}
