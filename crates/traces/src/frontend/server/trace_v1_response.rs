use axum::http::header;
use base64::Engine as _;
use opentelemetry_proto::tonic::{
    common::v1::KeyValue,
    trace::v1::{ResourceSpans, ScopeSpans, Span, TracesData},
};
use prost::Message as _;
use serde::de::DeserializeOwned;
use serde_json::Value;

use super::{HeaderMap, IntoResponse, Json, Response, StatusCode};
use crate::{frontend::wire::TraceByIdResponseJson, querier::http::wants_json};

/// Tempo's v1 trace-by-id answer, for `/api/traces/{id}`.
///
/// Grafana's Tempo *backend* datasource fetches the trace-view there with
/// `Accept: application/protobuf` and proto-decodes the body as OTLP. The
/// answer therefore defaults to OTLP `TracesData` protobuf, which is Tempo's
/// v1 default. It falls back to the wrapped JSON for humans.
pub(crate) fn trace_v1_response(
    headers: &HeaderMap,
    trace: Option<TraceByIdResponseJson>,
) -> Response {
    let Some(trace) = trace else {
        return (StatusCode::NOT_FOUND, "trace not found").into_response();
    };
    if wants_json(headers) {
        return Json(trace.trace).into_response();
    }
    match trace_protobuf(trace.trace) {
        Ok(bytes) => ([(header::CONTENT_TYPE, "application/protobuf")], bytes).into_response(),
        Err(error) => (StatusCode::INTERNAL_SERVER_ERROR, error).into_response(),
    }
}

fn trace_protobuf(trace: crate::frontend::wire::TraceEnvelopeJson) -> Result<Vec<u8>, String> {
    let mut value = serde_json::to_value(trace).map_err(|error| error.to_string())?;
    ids_to_hex(&mut value);
    let resources = take_array(&mut value, "resourceSpans")?;
    let trace = TracesData {
        resource_spans: resources
            .into_iter()
            .map(resource_from_json)
            .collect::<Result<_, _>>()?,
    };
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

fn take_array(value: &mut Value, key: &str) -> Result<Vec<Value>, String> {
    match value.as_object_mut().and_then(|value| value.remove(key)) {
        None => Ok(Vec::new()),
        Some(Value::Array(values)) => Ok(values),
        Some(_) => Err(format!("{key} must be an array")),
    }
}

fn take_attributes(value: &mut Value) -> Result<Vec<KeyValue>, String> {
    take_array(value, "attributes")?
        .into_iter()
        .map(|mut entry| {
            let raw = entry
                .as_object_mut()
                .ok_or("OTLP attribute must be an object")?
                .remove("value");
            let mut attribute: KeyValue = decode_json(entry)?;
            attribute.value = raw
                .as_ref()
                .map(crate::span::AttrValue::parse_otlp_json)
                .transpose()
                .map_err(|error| error.to_string())?;
            Ok(attribute)
        })
        .collect()
}

fn decode_json<T: DeserializeOwned>(value: Value) -> Result<T, String> {
    serde_json::from_value(value).map_err(|error| error.to_string())
}

fn resource_from_json(mut value: Value) -> Result<ResourceSpans, String> {
    let scopes = take_array(&mut value, "scopeSpans")?;
    let attributes = take_attributes(&mut value["resource"])?;
    let mut resource: ResourceSpans = decode_json(value)?;
    if let Some(resource) = &mut resource.resource {
        resource.attributes = attributes;
    }
    resource.scope_spans = scopes
        .into_iter()
        .map(scope_from_json)
        .collect::<Result<_, _>>()?;
    Ok(resource)
}

fn scope_from_json(mut value: Value) -> Result<ScopeSpans, String> {
    let spans = take_array(&mut value, "spans")?;
    let attributes = take_attributes(&mut value["scope"])?;
    let mut scope: ScopeSpans = decode_json(value)?;
    if let Some(scope) = &mut scope.scope {
        scope.attributes = attributes;
    }
    scope.spans = spans
        .into_iter()
        .map(span_from_json)
        .collect::<Result<_, _>>()?;
    Ok(scope)
}

fn span_from_json(mut value: Value) -> Result<Span, String> {
    let events = take_array(&mut value, "events")?;
    let links = take_array(&mut value, "links")?;
    let attributes = take_attributes(&mut value)?;
    let mut span: Span = decode_json(value)?;
    span.attributes = attributes;
    span.events = events
        .into_iter()
        .map(|mut event| {
            let attributes = take_attributes(&mut event)?;
            let mut event: opentelemetry_proto::tonic::trace::v1::span::Event = decode_json(event)?;
            event.attributes = attributes;
            Ok(event)
        })
        .collect::<Result<_, String>>()?;
    span.links = links
        .into_iter()
        .map(|mut link| {
            let attributes = take_attributes(&mut link)?;
            let mut link: opentelemetry_proto::tonic::trace::v1::span::Link = decode_json(link)?;
            link.attributes = attributes;
            Ok(link)
        })
        .collect::<Result<_, String>>()?;
    Ok(span)
}
