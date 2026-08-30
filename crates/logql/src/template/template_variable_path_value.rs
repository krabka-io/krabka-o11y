use super::*;

pub(crate) fn template_variable_path_value(
    value: &TemplateRuntimeValue,
    path: &[String],
) -> Option<TemplateRuntimeValue> {
    if path.is_empty() {
        return Some(value.clone());
    }
    let TemplateRuntimeValue::Json(mut current) = value.clone() else {
        return None;
    };
    for part in path {
        match current {
            serde_json::Value::Object(mut object) => {
                current = object.remove(part)?;
            }
            _ => return None,
        }
    }
    Some(TemplateRuntimeValue::Json(current))
}
