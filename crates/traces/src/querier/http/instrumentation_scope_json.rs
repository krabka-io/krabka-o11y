use super::{json, AttrValue, Value, Map, attrs_json};

pub(crate) fn instrumentation_scope_json(
    name: &str,
    version: &str,
    attributes: &[(String, AttrValue)],
) -> Value {
    let mut scope = Map::new();
    if !name.is_empty() {
        scope.insert("name".into(), json!(name));
    }
    if !version.is_empty() {
        scope.insert("version".into(), json!(version));
    }
    if !attributes.is_empty() {
        scope.insert("attributes".into(), attrs_json(attributes));
    }
    Value::Object(scope)
}
