use super::{
    Annotations, IntoResponse, Json, Map, QueryResult, Response, Value, json, result_json,
};

/// Builds the `status`/`data` success envelope and attaches the annotations.
///
/// Prometheus adds a top-level `warnings` array for its `PromQLWarning`-class
/// annotations and a top-level `infos` array for its `PromQLInfo`-class ones. It
/// omits each key when that class raised nothing, and it answers 200 either way.
/// This function reproduces that shape, so a Grafana datasource sees the same
/// envelope from Krabka as from Prometheus.
pub(crate) fn success_response(result: QueryResult, annotations: &Annotations) -> Response {
    let mut envelope = Map::new();
    envelope.insert("status".to_string(), json!("success"));
    envelope.insert("data".to_string(), result_json(result));
    if !annotations.warnings.is_empty() {
        envelope.insert("warnings".to_string(), json!(annotations.warnings));
    }
    if !annotations.infos.is_empty() {
        envelope.insert("infos".to_string(), json!(annotations.infos));
    }
    Json(Value::Object(envelope)).into_response()
}
