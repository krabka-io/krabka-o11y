use super::{
    BTreeMap, Labels, Value, json, limit_volume_series, loki_success_value,
    unix_ns_string_to_loki_seconds,
};

/// The `index/volume_range` answer when it holds series: one series per
/// metric, one sample per step that holds bytes.
///
/// Loki answers such a `volume_range` with a `matrix`. It answers an empty one,
/// and every `index/volume`, with a `vector`.
pub(crate) fn loki_volume_matrix_response(
    volumes: BTreeMap<Labels, BTreeMap<i64, u64>>,
    limit: usize,
) -> Value {
    let result = limit_volume_series(volumes, limit)
        .into_iter()
        .map(|(metric, samples)| {
            let values = samples
                .into_iter()
                .map(|(timestamp_ns, bytes)| {
                    json!([
                        unix_ns_string_to_loki_seconds(&timestamp_ns.to_string()),
                        bytes.to_string()
                    ])
                })
                .collect::<Vec<_>>();
            json!({
                "metric": metric,
                "values": values,
            })
        })
        .collect::<Vec<_>>();

    loki_success_value(json!({
        "resultType": "matrix",
        "result": result,
    }))
}
