use super::*;

pub(crate) fn attribute_value(value: Option<&AnyValue>) -> Option<String> {
    match value?.value.as_ref()? {
        any_value::Value::StringValue(value) => Some(value.clone()),
        any_value::Value::BoolValue(value) => Some(value.to_string()),
        any_value::Value::IntValue(value) => Some(value.to_string()),
        any_value::Value::DoubleValue(value) => Some(value.to_string()),
        any_value::Value::BytesValue(value) => Some(format!("{value:x?}")),
        any_value::Value::ArrayValue(_)
        | any_value::Value::KvlistValue(_)
        | any_value::Value::StringValueStrindex(_) => None,
    }
}

pub(crate) fn insert_attributes(
    labels: &mut Labels,
    attributes: &[KeyValue],
    strategy: TranslationStrategy,
) {
    for attribute in attributes {
        if let Some(value) = attribute_value(attribute.value.as_ref()) {
            labels.insert(normalize_name(&attribute.key, strategy), value);
        }
    }
}

pub(crate) fn instrumentation_scope_attributes(
    scope: &InstrumentationScope,
    strategy: TranslationStrategy,
) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    if !scope.name.is_empty() {
        attributes.push(string_attribute("otel_scope_name", &scope.name));
    }
    if !scope.version.is_empty() {
        attributes.push(string_attribute("otel_scope_version", &scope.version));
    }
    for attribute in &scope.attributes {
        let key = format!("otel_scope_{}", attribute.key);
        let normalized = normalize_name(&key, strategy);
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

pub(crate) fn labels(
    name: &str,
    resource_attributes: &[KeyValue],
    point_attributes: &[KeyValue],
    extra: Option<ExtraLabel<'_>>,
    strategy: TranslationStrategy,
) -> Labels {
    let mut labels = Labels::new();
    labels.insert("__name__", name);
    insert_attributes(&mut labels, resource_attributes, strategy);
    insert_attributes(&mut labels, point_attributes, strategy);
    if let Some(extra) = extra {
        labels.insert(extra.label_name, extra.label_value);
    }
    labels
}

pub(crate) fn scope_attributes(
    scope_metrics: &ScopeMetrics,
    strategy: TranslationStrategy,
) -> Vec<KeyValue> {
    let mut attributes = Vec::new();
    if let Some(scope) = &scope_metrics.scope {
        attributes.extend(instrumentation_scope_attributes(scope, strategy));
    }
    if !scope_metrics.schema_url.is_empty() {
        attributes.push(string_attribute(
            "otel_scope_schema_url",
            &scope_metrics.schema_url,
        ));
    }
    attributes
}

pub(crate) fn promoted_resource_attributes(
    resource_attributes: &[KeyValue],
    additional_attributes: &[String],
) -> Vec<KeyValue> {
    let value = |name: &str| {
        resource_attributes
            .iter()
            .find(|attribute| attribute.key == name)
            .and_then(|attribute| attribute_value(attribute.value.as_ref()))
    };

    let mut promoted = Vec::new();
    if let Some(service_name) = value("service.name") {
        let job = value("service.namespace").map_or_else(
            || service_name.clone(),
            |namespace| format!("{namespace}/{service_name}"),
        );
        promoted.push(string_attribute("job", &job));
    }
    if let Some(instance) = value("service.instance.id") {
        promoted.push(string_attribute("instance", &instance));
    }
    promoted.extend(
        additional_attributes
            .iter()
            .filter_map(|name| {
                resource_attributes
                    .iter()
                    .find(|attribute| attribute.key == *name)
            })
            .cloned(),
    );
    promoted
}
