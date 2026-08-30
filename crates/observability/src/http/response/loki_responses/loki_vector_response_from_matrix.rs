use super::*;

pub(crate) fn loki_vector_response_from_matrix(mut value: Value) -> Value {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("matrix") {
        return value;
    }

    value["data"]["resultType"] = json!("vector");
    if let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    {
        for result in results {
            if let Some(values) = result.get_mut("values").and_then(Value::as_array_mut) {
                let value_sample = values.pop().unwrap_or_else(|| json!([]));
                result["value"] = value_sample;
            }
            if let Some(object) = result.as_object_mut() {
                object.remove("values");
            }
        }
    }

    value
}
