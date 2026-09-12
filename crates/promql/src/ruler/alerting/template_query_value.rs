use serde_json::{Value, json};

use super::{QueryResult, SampleValue};

pub(crate) fn template_query_value(result: QueryResult) -> Value {
    let QueryResult::InstantVector(samples) = result else {
        return Value::Array(Vec::new());
    };
    Value::Array(
        samples
            .into_iter()
            .filter_map(|sample| {
                let SampleValue::Float(value) = sample.value else {
                    return None;
                };
                Some(json!({
                    "Labels": sample.labels,
                    "Value": crate::http_api::format_sample_value(value),
                }))
            })
            .collect(),
    )
}
