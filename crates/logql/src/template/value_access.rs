use super::{TemplateRuntimeValue, format_template_float, parse_template_bound};

// Template Index Value
pub(crate) fn template_index_value(
    value: &TemplateRuntimeValue,
    index: &str,
) -> Option<TemplateRuntimeValue> {
    match value {
        TemplateRuntimeValue::Bytes(value) | TemplateRuntimeValue::ByteSlice(value) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| value.get(index).copied())
            .map(TemplateRuntimeValue::Byte),
        TemplateRuntimeValue::FloatSlice(values) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| values.get(index)),
        TemplateRuntimeValue::HistogramSpans(values) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| values.get(index)),
        TemplateRuntimeValue::ByteLabels(labels) => Some(TemplateRuntimeValue::Bytes(
            labels.get(index).cloned().unwrap_or_default(),
        )),
        TemplateRuntimeValue::Object(object) => Some(
            object
                .get(index)
                .cloned()
                .unwrap_or(TemplateRuntimeValue::Json(serde_json::Value::Null)),
        ),
        TemplateRuntimeValue::QueryResult(values) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| values.get(index)),
        TemplateRuntimeValue::Array(values) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| values.get(index).cloned()),
        TemplateRuntimeValue::Labels(labels) => Some(TemplateRuntimeValue::String(
            labels.get(index).cloned().unwrap_or_default(),
        )),
        TemplateRuntimeValue::Json(serde_json::Value::Object(object)) => {
            Some(TemplateRuntimeValue::Json(
                object
                    .get(index)
                    .cloned()
                    .unwrap_or(serde_json::Value::Null),
            ))
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
            .map(TemplateRuntimeValue::Byte),
        TemplateRuntimeValue::Json(serde_json::Value::String(value)) => index
            .parse::<usize>()
            .ok()
            .and_then(|index| value.as_bytes().get(index).copied())
            .map(TemplateRuntimeValue::Byte),
        _ => None,
    }
}

// Template Json Value To String
pub(crate) fn template_json_value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::Null => "<no value>".to_string(),
        serde_json::Value::Bool(value) => value.to_string(),
        serde_json::Value::Number(value) => {
            if value.is_f64() {
                value
                    .as_f64()
                    .map_or_else(|| value.to_string(), format_template_float)
            } else {
                value.to_string()
            }
        }
        serde_json::Value::String(value) => value.clone(),
        serde_json::Value::Array(values) => format!(
            "[{}]",
            values
                .iter()
                .map(template_json_value_to_string)
                .collect::<Vec<_>>()
                .join(" ")
        ),
        serde_json::Value::Object(values) => format!(
            "map[{}]",
            values
                .iter()
                .map(|(key, value)| format!("{key}:{}", template_json_value_to_string(value)))
                .collect::<Vec<_>>()
                .join(" ")
        ),
    }
}

// Template Variable Path Value
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

// Template Slice Bounds
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TemplateSliceBounds {
    pub start: usize,
    pub end: usize,
}

pub(crate) fn template_slice_bounds(
    len: usize,
    bounds: &[TemplateRuntimeValue],
) -> Option<TemplateSliceBounds> {
    if bounds.len() > 3 {
        return None;
    }
    let start = bounds.first().map_or(Some(0), parse_template_bound)?;
    let end = bounds.get(1).map_or(Some(len), parse_template_bound)?;
    if let Some(capacity) = bounds.get(2) {
        let capacity = parse_template_bound(capacity)?;
        if end > capacity || capacity > len {
            return None;
        }
    }
    (start <= end && end <= len).then_some(TemplateSliceBounds { start, end })
}
