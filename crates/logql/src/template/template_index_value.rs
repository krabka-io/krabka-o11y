use super::*;

pub(crate) fn template_index_value(value: &TemplateRuntimeValue, index: &str) -> Option<TemplateRuntimeValue> {
    match value {
        TemplateRuntimeValue::Json(serde_json::Value::Object(object)) => {
            object.get(index).cloned().map(TemplateRuntimeValue::Json)
        }
        TemplateRuntimeValue::Json(serde_json::Value::Array(values)) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| values.get(index).cloned())
            .map(TemplateRuntimeValue::Json),
        TemplateRuntimeValue::String(value) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| value.as_bytes().get(index).copied())
            .map(|byte| TemplateRuntimeValue::Integer(i64::from(byte))),
        TemplateRuntimeValue::Json(serde_json::Value::String(value)) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| value.as_bytes().get(index).copied())
            .map(|byte| TemplateRuntimeValue::Integer(i64::from(byte))),
        _ => None,
    }
}
