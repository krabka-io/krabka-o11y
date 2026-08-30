use super::{
    BTreeMap, ScalarVectorExpressionResult, TimeRange, Value, eval_times, json, loki_success_value,
    unix_ns_string_to_loki_seconds,
};

pub(crate) fn loki_range_vector_response(
    time_range: TimeRange,
    step_ns: i64,
    result: ScalarVectorExpressionResult,
) -> Value {
    let (sample, metric) = match result {
        ScalarVectorExpressionResult::Scalar { sample } => (Some(sample), BTreeMap::new()),
        ScalarVectorExpressionResult::Vector { sample, metric } => (sample, metric),
    };
    let result = sample.map_or_else(Vec::new, |sample| {
        vec![json!({
            "metric": metric,
            "values": eval_times(time_range, step_ns)
                .into_iter()
                .map(|timestamp_ns| {
                    json!([
                        unix_ns_string_to_loki_seconds(&timestamp_ns.to_string()),
                        sample
                    ])
                })
                .collect::<Vec<_>>()
        })]
    });
    loki_success_value(json!({
        "resultType": "matrix",
        "result": result
    }))
}
