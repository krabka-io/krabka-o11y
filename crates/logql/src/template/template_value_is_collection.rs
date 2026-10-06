use super::TemplateRuntimeValue;

pub(crate) fn template_value_is_collection(value: &TemplateRuntimeValue) -> bool {
    matches!(
        value,
        TemplateRuntimeValue::FloatSlice(_)
            | TemplateRuntimeValue::HistogramSpans(_)
            | TemplateRuntimeValue::Labels(_)
            | TemplateRuntimeValue::Bytes(_)
            | TemplateRuntimeValue::ByteSlice(_)
            | TemplateRuntimeValue::ByteLabels(_)
            | TemplateRuntimeValue::Object(_)
            | TemplateRuntimeValue::Array(_)
            | TemplateRuntimeValue::QueryResult(_)
            | TemplateRuntimeValue::String(_)
            | TemplateRuntimeValue::Json(
                serde_json::Value::String(_)
                    | serde_json::Value::Array(_)
                    | serde_json::Value::Object(_)
            )
    )
}
