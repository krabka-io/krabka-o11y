use super::{SpanMetricsConfig, SpanRecord, red_dimension_pairs};
use crate::metricsgen::config::{FilterPolicy, MatchType};

pub(crate) type DimKey = Vec<krabka_traceql::MetricLabel>;

pub(crate) fn dim_key(
    span: &SpanRecord,
    include_status_message: bool,
    config: &SpanMetricsConfig,
) -> DimKey {
    let mut labels = red_dimension_pairs(span).into_iter().collect::<Vec<_>>();
    if include_status_message {
        labels.push(krabka_traceql::MetricLabel::new(
            "status_message",
            span.status_message.clone(),
        ));
    }
    for name in &config.dimensions {
        let label = prometheus_label_name(name);
        if !label.is_empty()
            && let Some(value) = span_attr(span, name)
            && !labels.iter().any(|existing| existing.key == label)
        {
            labels.push(krabka_traceql::MetricLabel::new(label, value));
        }
    }
    for mapping in &config.dimension_mappings {
        let values = mapping
            .source_labels
            .iter()
            .filter_map(|name| span_attr(span, name))
            .collect::<Vec<_>>();
        let name = prometheus_label_name(&mapping.name);
        if !name.is_empty()
            && !values.is_empty()
            && !labels.iter().any(|existing| existing.key == name)
        {
            labels.push(krabka_traceql::MetricLabel::new(
                name,
                values.join(&mapping.join),
            ));
        }
    }
    labels.sort();
    labels
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
            .find(|attribute| attribute.key == key)
            .map(|attribute| attribute.value.as_str()),
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
