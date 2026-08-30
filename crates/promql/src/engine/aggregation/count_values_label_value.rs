use super::{SampleValue, Result, PromqlError};

pub(crate) fn count_values_label_value(value: &SampleValue) -> Result<String> {
    match value {
        // Render the float with the crate's canonical Prometheus formatter so
        // non-finite values match the wire form (`+Inf`/`-Inf`/`NaN`) rather than
        // `f64::to_string`'s `inf`/`-inf`/`NaN`.
        SampleValue::Float(value) => Ok(crate::http_api::format_sample_value(*value)),
        SampleValue::Histogram(histogram) => serde_json::to_string(histogram).map_err(|error| {
            PromqlError::Exec(format!(
                "failed to encode histogram sample for count_values: {error}"
            ))
        }),
    }
}
