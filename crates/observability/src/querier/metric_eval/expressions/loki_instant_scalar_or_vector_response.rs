use super::*;

pub(crate) fn loki_instant_scalar_or_vector_response(
    timestamp_ns: i64,
    result: ScalarVectorExpressionResult,
) -> Value {
    let timestamp = unix_ns_string_to_loki_seconds(&timestamp_ns.to_string());
    match result {
        ScalarVectorExpressionResult::Scalar { sample } => loki_success_value(json!({
            "resultType": "scalar",
            "result": [timestamp, sample]
        })),
        ScalarVectorExpressionResult::Vector { sample, metric } => {
            let timestamp = json!(timestamp_ns);
            let result = sample.map_or_else(Vec::new, |sample| {
                vec![json!({
                    "metric": metric,
                    "value": [
                        timestamp,
                        sample
                    ]
                })]
            });
            loki_success_value(json!({
                "resultType": "vector",
                "result": result
            }))
        }
    }
}
