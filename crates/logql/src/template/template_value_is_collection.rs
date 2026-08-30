use super::*;

pub(crate) fn template_value_is_collection(value: &TemplateRuntimeValue) -> bool {
    matches!(
        value,
        TemplateRuntimeValue::String(_)
            | TemplateRuntimeValue::Json(
                serde_json::Value::String(_)
                    | serde_json::Value::Array(_)
                    | serde_json::Value::Object(_)
            )
    )
}
