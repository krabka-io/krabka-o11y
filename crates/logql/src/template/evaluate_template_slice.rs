use super::{
    TemplateRuntimeValue, template_collection_first_args, template_slice_array,
    template_slice_string,
};

pub(crate) fn evaluate_template_slice(args: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    let Some((value, bounds)) = template_collection_first_args(args) else {
        return TemplateRuntimeValue::String(String::new());
    };
    match value {
        TemplateRuntimeValue::Bytes(value) => super::template_slice_bounds(value.len(), bounds)
            .map_or_else(
                || TemplateRuntimeValue::Bytes(Vec::new()),
                |(start, end)| TemplateRuntimeValue::Bytes(value[start..end].to_vec()),
            ),
        TemplateRuntimeValue::ByteSlice(values) => {
            super::template_slice_bounds(values.len(), bounds).map_or_else(
                || TemplateRuntimeValue::ByteSlice(Vec::new()),
                |(start, end)| TemplateRuntimeValue::ByteSlice(values[start..end].to_vec()),
            )
        }
        TemplateRuntimeValue::QueryResult(values) => {
            super::template_slice_bounds(values.len(), bounds).map_or_else(
                || TemplateRuntimeValue::QueryResult(Vec::new().into()),
                |(start, end)| TemplateRuntimeValue::QueryResult(values.slice(start, end)),
            )
        }
        TemplateRuntimeValue::FloatSlice(values) => {
            super::template_slice_bounds(values.len(), bounds).map_or_else(
                || TemplateRuntimeValue::FloatSlice(Vec::new().into()),
                |(start, end)| TemplateRuntimeValue::FloatSlice(values.slice(start, end)),
            )
        }
        TemplateRuntimeValue::HistogramSpans(values) => {
            super::template_slice_bounds(values.len(), bounds).map_or_else(
                || TemplateRuntimeValue::HistogramSpans(Vec::new().into()),
                |(start, end)| TemplateRuntimeValue::HistogramSpans(values.slice(start, end)),
            )
        }
        TemplateRuntimeValue::Array(values) => super::template_slice_bounds(values.len(), bounds)
            .map_or_else(
                || TemplateRuntimeValue::Array(Vec::new()),
                |(start, end)| TemplateRuntimeValue::Array(values[start..end].to_vec()),
            ),
        TemplateRuntimeValue::String(value) => template_slice_string(value, bounds),
        TemplateRuntimeValue::Json(serde_json::Value::String(value)) => {
            template_slice_string(value, bounds)
        }
        TemplateRuntimeValue::Json(serde_json::Value::Array(values)) => {
            template_slice_array(values, bounds)
        }
        _ => TemplateRuntimeValue::String(String::new()),
    }
}
