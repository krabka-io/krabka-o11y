use super::SampleValue;

pub(crate) fn count_values_label_value(value: &SampleValue) -> String {
    match value {
        // Render the float with the crate's canonical Prometheus formatter so
        // non-finite values match the wire form (`+Inf`/`-Inf`/`NaN`) rather than
        // `f64::to_string`'s `inf`/`-inf`/`NaN`.
        SampleValue::Float(value) => crate::http_api::format_sample_value(*value),
        SampleValue::Histogram(histogram) => crate::http_api::native_histogram_string(histogram),
    }
}
