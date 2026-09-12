use super::TemplateRuntimeValue;

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
        let part = part
            .strip_prefix('"')
            .and_then(|part| part.strip_suffix('"'))
            .unwrap_or(part);
        match current {
            serde_json::Value::Object(mut object) => {
                current = object.remove(part)?;
            }
            _ => return None,
        }
    }
    Some(TemplateRuntimeValue::Json(current))
}
