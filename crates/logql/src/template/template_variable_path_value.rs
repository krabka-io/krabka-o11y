use super::TemplateRuntimeValue;

pub(crate) fn template_variable_path_value(
    value: &TemplateRuntimeValue,
    path: &[String],
) -> Option<TemplateRuntimeValue> {
    if path.is_empty() {
        return Some(value.clone());
    }
    if let TemplateRuntimeValue::Reference(reference) = value {
        return reference
            .field(&path[0])
            .and_then(|value| template_variable_path_value(&value, &path[1..]));
    }
    if let TemplateRuntimeValue::HistogramError(error) = value {
        return error
            .field(&path[0])
            .and_then(|value| template_variable_path_value(&value, &path[1..]));
    }
    if let TemplateRuntimeValue::Sample(value) = value {
        return value
            .get(&path[0])
            .and_then(|value| template_variable_path_value(value, &path[1..]));
    }
    if let TemplateRuntimeValue::FloatHistogram(value) = value {
        return value
            .field(&path[0])
            .and_then(|value| template_variable_path_value(&value, &path[1..]));
    }
    if let TemplateRuntimeValue::HistogramBucket(value) = value {
        return value
            .field(&path[0])
            .and_then(|value| template_variable_path_value(&value, &path[1..]));
    }
    if let TemplateRuntimeValue::HistogramSpan(value) = value {
        let field = match path[0].as_str() {
            "Offset" => TemplateRuntimeValue::Integer32(value.offset),
            "Length" => TemplateRuntimeValue::Unsigned32(value.length),
            _ => return None,
        };
        return template_variable_path_value(&field, &path[1..]);
    }
    if let TemplateRuntimeValue::ByteLabels(labels) = value {
        return (path.len() == 1).then(|| {
            TemplateRuntimeValue::Bytes(labels.get(&path[0]).cloned().unwrap_or_default())
        });
    }
    if let TemplateRuntimeValue::Object(object) = value {
        return object
            .get(&path[0])
            .and_then(|value| template_variable_path_value(value, &path[1..]))
            .or(Some(TemplateRuntimeValue::Json(serde_json::Value::Null)));
    }
    if let TemplateRuntimeValue::Labels(labels) = value {
        return (path.len() == 1).then(|| {
            TemplateRuntimeValue::String(labels.get(&path[0]).cloned().unwrap_or_default())
        });
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
                current = object.remove(part).unwrap_or(serde_json::Value::Null);
            }
            _ => return None,
        }
    }
    Some(TemplateRuntimeValue::Json(current))
}
