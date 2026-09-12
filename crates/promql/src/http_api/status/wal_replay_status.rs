use super::{
    Arc, MetricStore, PrometheusApiState, Response, State, Value, json, success_data_response,
};

pub(crate) async fn wal_replay_status<S: MetricStore>(
    State(state): State<Arc<PrometheusApiState<S>>>,
) -> Response {
    let Some(head) = &state.wal_head else {
        return success_data_response(json!({
            "min": null,
            "max": null,
            "current": null,
            "state": "not applicable (no WAL head configured)",
        }));
    };
    let watermarks = head.watermarks();
    let tail_is_attached = state
        .wal_head_readiness
        .as_ref()
        .is_some_and(krabka_observability::ReadinessGate::is_ready);
    let min = watermarks
        .values()
        .map(|watermark| watermark.low_water_offset.get())
        .min()
        .map_or(Value::Null, Value::from);
    let current = watermarks
        .values()
        .map(|watermark| watermark.high_water_offset.get())
        .max()
        .map_or(Value::Null, Value::from);
    success_data_response(json!({
        "min": min,
        "max": null,
        "current": current,
        "state": if !tail_is_attached {
            "waiting (WAL tail is not attached)"
        } else if watermarks.is_empty() {
            "unknown (WAL tail attached; no offsets materialized; broker high watermark unavailable)"
        } else {
            "unknown (WAL tail attached; broker high watermark unavailable)"
        },
    }))
}
