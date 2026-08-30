use super::*;

pub(crate) fn loki_volume_vector_response(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    timestamp: i64,
    limit: usize,
) -> Value {
    let result = limit_volume_series(volumes, limit)
        .into_iter()
        .map(|(metric, samples)| {
            let value = samples.values().copied().fold(0_u64, u64::saturating_add);
            json!({
                "metric": metric,
                "value": [timestamp, value.to_string()],
            })
        })
        .collect::<Vec<_>>();

    loki_success_value(json!({
        "resultType": "vector",
        "result": result,
    }))
}
