use super::{
    TemplateRuntimeValue, template_collection_first_args, template_slice_array,
    template_slice_string,
};

pub(crate) fn evaluate_template_slice(args: &[TemplateRuntimeValue]) -> TemplateRuntimeValue {
    let Some(split) = template_collection_first_args(args) else {
        return TemplateRuntimeValue::String(String::new());
    };
    match split.collection {
        TemplateRuntimeValue::Bytes(value) => super::template_slice_bounds(value.len(), split.args)
            .map_or_else(
                || TemplateRuntimeValue::Bytes(Vec::new()),
                |bounds| TemplateRuntimeValue::Bytes(value[bounds.start..bounds.end].to_vec()),
            ),
        TemplateRuntimeValue::ByteSlice(values) => {
            super::template_slice_bounds(values.len(), split.args).map_or_else(
                || TemplateRuntimeValue::ByteSlice(Vec::new()),
                |bounds| TemplateRuntimeValue::ByteSlice(values[bounds.start..bounds.end].to_vec()),
            )
        }
        TemplateRuntimeValue::QueryResult(values) => {
            super::template_slice_bounds(values.len(), split.args).map_or_else(
                || TemplateRuntimeValue::QueryResult(Vec::new().into()),
                |bounds| TemplateRuntimeValue::QueryResult(values.slice(bounds.start, bounds.end)),
            )
        }
        TemplateRuntimeValue::FloatSlice(values) => {
            super::template_slice_bounds(values.len(), split.args).map_or_else(
                || TemplateRuntimeValue::FloatSlice(Vec::new().into()),
                |bounds| TemplateRuntimeValue::FloatSlice(values.slice(bounds.start, bounds.end)),
            )
        }
        TemplateRuntimeValue::HistogramSpans(values) => {
            super::template_slice_bounds(values.len(), split.args).map_or_else(
                || TemplateRuntimeValue::HistogramSpans(Vec::new().into()),
                |bounds| {
                    TemplateRuntimeValue::HistogramSpans(values.slice(bounds.start, bounds.end))
                },
            )
        }
        TemplateRuntimeValue::Array(values) => {
            super::template_slice_bounds(values.len(), split.args).map_or_else(
                || TemplateRuntimeValue::Array(Vec::new()),
                |bounds| TemplateRuntimeValue::Array(values[bounds.start..bounds.end].to_vec()),
            )
        }
        TemplateRuntimeValue::String(value) => template_slice_string(value, split.args),
        TemplateRuntimeValue::Json(serde_json::Value::String(value)) => {
            template_slice_string(value, split.args)
        }
        TemplateRuntimeValue::Json(serde_json::Value::Array(values)) => {
            template_slice_array(values, split.args)
        }
        _ => TemplateRuntimeValue::String(String::new()),
    }
}
