use super::{
    Arc, BlockCatalog, Extension, HeaderMap, IntoResponse, Json, Path, Principal, QuerierBackend,
    QueryFrontend, Response, State, StatusCode, Uri, backend_error_response, optional_time_bounds,
    parse_hex16, request_tenant,
};
use axum::http::header;
use base64::Engine as _;
use prost::Message as _;

pub(crate) async fn trace_by_id_v1<B, C>(
    State(qf): State<Arc<QueryFrontend<B, C>>>,
    Extension(principal): Extension<Principal>,
    headers: HeaderMap,
    Path(trace_id): Path<String>,
    uri: Uri,
) -> Response
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    if trace_id.len() != 32 || hex::decode(&trace_id).is_err() {
        return (StatusCode::BAD_REQUEST, "trace id must be 32 hex chars").into_response();
    }
    let tenant = match request_tenant(&headers, &principal, &qf.cfg.tenant_policy) {
        Ok(tenant) => tenant,
        Err(rejection) => return *rejection,
    };
    let (start_ns, end_ns) = match optional_time_bounds(&uri) {
        Ok(bounds) => bounds,
        Err(err) => return (StatusCode::BAD_REQUEST, err).into_response(),
    };
    match qf
        .trace_by_id(&tenant, parse_hex16(&trace_id), start_ns, end_ns)
        .await
    {
        Ok((Some(trace), _, _, _)) => {
            if wants_json(&headers) {
                return Json(trace.trace).into_response();
            }
            match trace_protobuf(trace.trace) {
                Ok(bytes) => {
                    ([(header::CONTENT_TYPE, "application/protobuf")], bytes).into_response()
                }
                Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
            }
        }
        Ok((None, _, _, _)) => (StatusCode::NOT_FOUND, "trace not found").into_response(),
        Err(err) => backend_error_response(&err),
    }
}

fn wants_json(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| {
            accept
                .split(',')
                .any(|part| part.trim() == "application/json")
        })
}

fn trace_protobuf(trace: crate::frontend::wire::TraceEnvelopeJson) -> Result<Vec<u8>, String> {
    let mut value = serde_json::to_value(trace).map_err(|error| error.to_string())?;
    ids_to_hex(&mut value);
    let trace: opentelemetry_proto::tonic::trace::v1::TracesData =
        serde_json::from_value(value).map_err(|error| error.to_string())?;
    Ok(trace.encode_to_vec())
}

fn ids_to_hex(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(object) => {
            for (name, value) in object {
                if matches!(name.as_str(), "traceId" | "spanId" | "parentSpanId")
                    && let Some(encoded) = value.as_str()
                    && let Ok(bytes) = base64::engine::general_purpose::STANDARD.decode(encoded)
                {
                    *value = serde_json::Value::String(hex::encode(bytes));
                } else {
                    ids_to_hex(value);
                }
            }
        }
        serde_json::Value::Array(values) => values.iter_mut().for_each(ids_to_hex),
        _ => {}
    }
}
