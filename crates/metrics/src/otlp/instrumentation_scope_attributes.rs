use super::{
    InstrumentationScope, KeyValue, TranslationStrategy, normalize_name, string_attribute,
};

pub(crate) fn instrumentation_scope_attributes(scope: &InstrumentationScope) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    if !scope.name.is_empty() {
        attributes.push(string_attribute("otel_scope_name", &scope.name));
    }
    if !scope.version.is_empty() {
        attributes.push(string_attribute("otel_scope_version", &scope.version));
    }
    for attribute in &scope.attributes {
        let key = format!("otel_scope_{}", attribute.key);
        let normalized = normalize_name(&key, TranslationStrategy::default());
        if matches!(
            normalized.as_str(),
            "otel_scope_name" | "otel_scope_version" | "otel_scope_schema_url"
        ) {
            continue;
        }
        let mut attribute = attribute.clone();
        attribute.key = key;
        attributes.push(attribute);
    }
    attributes
}
