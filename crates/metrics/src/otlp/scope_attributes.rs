use super::{KeyValue, ScopeMetrics, instrumentation_scope_attributes, string_attribute};

pub(crate) fn scope_attributes(scope_metrics: &ScopeMetrics) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    if let Some(scope) = &scope_metrics.scope {
        attributes.extend(instrumentation_scope_attributes(scope));
    }
    if !scope_metrics.schema_url.is_empty() {
        attributes.push(string_attribute(
            "otel_scope_schema_url",
            &scope_metrics.schema_url,
        ));
    }
    attributes
}
