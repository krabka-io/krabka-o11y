use super::{Labels, TemplateRuntimeValue, template_variable_path_value};

pub(crate) fn template_root_field_value(fields: &Labels, path: &[String]) -> TemplateRuntimeValue {
    let Some((first, rest)) = path.split_first() else {
        let object = fields
            .iter()
            .map(|(key, value)| (key.clone(), serde_json::Value::String(value.clone())))
            .collect();
        return TemplateRuntimeValue::Json(serde_json::Value::Object(object));
    };

    let Some(value) = fields.get(first) else {
        return TemplateRuntimeValue::String(String::new());
    };
    if rest.is_empty() {
        return TemplateRuntimeValue::String(value.clone());
    }

    serde_json::from_str::<serde_json::Value>(value)
        .ok()
        .and_then(|json| template_variable_path_value(&TemplateRuntimeValue::Json(json), rest))
        .unwrap_or_else(|| TemplateRuntimeValue::String(String::new()))
}
