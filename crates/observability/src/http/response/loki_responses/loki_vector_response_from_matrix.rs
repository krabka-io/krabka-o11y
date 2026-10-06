use super::{Value, json};

pub(crate) fn loki_vector_response_from_matrix(mut value: Value) -> Value {
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("matrix") {
        return value;
    }

    value["data"]["resultType"] = json!("vector");
    if let Some(results) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    {
        results.retain_mut(|result| {
            if let Some(values) = result.get_mut("values").and_then(Value::as_array_mut) {
                let Some(value_sample) = values.pop() else {
                    return false;
                };
                result["value"] = value_sample;
            }
            if let Some(object) = result.as_object_mut() {
                object.remove("values");
            }
            true
        });
    }

    value
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_window_is_absent_and_a_real_zero_sample_is_preserved() {
        let response = json!({"status":"success", "data": {
            "resultType":"matrix", "stats":{"summary":{"totalLinesProcessed":7}},
            "result":[
                {"metric":{"level":"stale"}, "values":[]},
                {"metric":{"level":"zero"}, "values":[[5,"0"]]},
                {"metric":{"level":"live"}, "values":[[4,"1"],[5,"2"]]}
            ]
        }});
        assert2::assert!(
            loki_vector_response_from_matrix(response)
                == json!({
                    "status":"success", "data": {
                        "resultType":"vector", "stats":{"summary":{"totalLinesProcessed":7}},
                        "result":[
                            {"metric":{"level":"zero"}, "value":[5,"0"]},
                            {"metric":{"level":"live"}, "value":[5,"2"]}
                        ]
                    }
                })
        );
    }
}
