use super::TemplateRuntimeValue;

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
