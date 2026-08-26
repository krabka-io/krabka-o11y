//! Read-side wrapper over the traces hot tier.

use std::sync::Arc;

use arrow::{
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use crabka_traceql::{
    AttrValue, EventRef, LinkRef, ScopedTag, SpanRef, TagScope, TraceSpans, TraceqlError,
    TypedValue,
};
use crabka_units::{Time, convert::TimeExt as _};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, any_value::Value as OtlpValue},
    trace::v1::TracesData,
};
use prost::Message as _;
use reqwest::Url;

use super::store::SharedTraceIndex;

pub type Result<T> = std::result::Result<T, TraceqlError>;
const LIVE_SPAN_BATCHES_PATH: &str = "/api/crabka/live/span-batches";

/// OTLP carries nanosecond fields as `uint64`. Saturate rather than wrap when
/// one exceeds what a `Time` extent can be built from.
fn time_from_nanos_u64(nanos: u64) -> Time {
    Time::from_nanos(i64::try_from(nanos).unwrap_or(i64::MAX))
}

#[async_trait::async_trait]
pub trait LiveSource: Send + Sync {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>>;

    async fn trace_spans(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>>;

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>>;

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>>;

    fn block_builder_frontier_ns(&self, tenant: &str) -> i64;
}

///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn encode_span_batches(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let Some(first) = batches.first() else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut out, &first.schema())
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        }
        writer
            .finish()
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
    }
    Ok(out)
}

fn decode_span_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let reader =
        StreamReader::try_new(bytes, None).map_err(|err| TraceqlError::Plan(err.to_string()))?;
    reader
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|err| TraceqlError::Plan(err.to_string()))
}

pub struct RemoteLiveSource {
    base_url: Url,
    trace_index: SharedTraceIndex,
    http: reqwest::Client,
}

impl RemoteLiveSource {
    #[must_use]
    pub fn new(base_url: Url, trace_index: SharedTraceIndex) -> Self {
        Self {
            base_url,
            trace_index,
            http: reqwest::Client::new(),
        }
    }
}

#[async_trait::async_trait]
impl LiveSource for RemoteLiveSource {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>> {
        let mut url = self
            .base_url
            .join(LIVE_SPAN_BATCHES_PATH)
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        url.query_pairs_mut()
            .append_pair("start", &start_ns.to_string())
            .append_pair("end", &end_ns.to_string());
        let resp = self
            .http
            .get(url)
            .header("x-scope-orgid", tenant)
            .send()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        if !resp.status().is_success() {
            return Err(TraceqlError::Plan(format!(
                "remote live-store returned {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        decode_span_batches(&bytes)
    }

    async fn trace_spans(&self, tenant: &str, trace_id: &[u8; 16]) -> Result<Option<TraceSpans>> {
        // Use the v1 endpoint for internal federation: it returns the bare OTLP
        // `TracesData` we decode below. The v2 endpoint wraps the trace in a
        // Tempo `TraceByIDResponse` for Grafana's backend datasource.
        let path = format!("/api/traces/{}", hex::encode(trace_id));
        let url = self
            .base_url
            .join(&path)
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        let resp = self
            .http
            .get(url)
            .header("x-scope-orgid", tenant)
            .header("accept", "application/x-protobuf")
            .send()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(TraceqlError::Plan(format!(
                "remote live-store returned {}",
                resp.status()
            )));
        }
        let bytes = resp
            .bytes()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        let data = TracesData::decode(bytes).map_err(|err| TraceqlError::Plan(err.to_string()))?;
        trace_spans_from_otlp(trace_id, data).map(Some)
    }

    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        let mut url = self
            .base_url
            .join("/api/v2/search/tags")
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("start", &ns_floor_seconds(start_ns).to_string())
                .append_pair("end", &ns_ceil_seconds(end_ns).to_string());
            if let Some(scope) = scope {
                query.append_pair("scope", tag_scope_name(scope));
            }
        }
        let json = self.get_json(tenant, url).await?;
        scoped_tags_from_json(&json)
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        let mut url = self
            .base_url
            .join(&format!("/api/v2/search/tag/{tag}/values"))
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        url.query_pairs_mut()
            .append_pair("start", &ns_floor_seconds(start_ns).to_string())
            .append_pair("end", &ns_ceil_seconds(end_ns).to_string());
        let json = self.get_json(tenant, url).await?;
        typed_values_from_json(&json)
    }

    fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        let trace_index = self.trace_index.load();
        trace_index
            .trace_blocks(tenant)
            .iter()
            .map(|block| block.max_ts.saturating_add(1))
            .max()
            .unwrap_or_default()
    }
}

impl RemoteLiveSource {
    async fn get_json(&self, tenant: &str, url: Url) -> Result<serde_json::Value> {
        let resp = self
            .http
            .get(url)
            .header("x-scope-orgid", tenant)
            .send()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))?;
        if !resp.status().is_success() {
            return Err(TraceqlError::Plan(format!(
                "remote live-store returned {}",
                resp.status()
            )));
        }
        resp.json()
            .await
            .map_err(|err| TraceqlError::Plan(err.to_string()))
    }
}

fn tag_scope_name(scope: TagScope) -> &'static str {
    match scope {
        TagScope::Resource => "resource",
        TagScope::Span => "span",
        TagScope::Intrinsic => "intrinsic",
        TagScope::Event => "event",
        TagScope::Link => "link",
        TagScope::Instrumentation => "instrumentation",
    }
}

fn ns_floor_seconds(ns: i64) -> i64 {
    ns.div_euclid(1_000_000_000)
}

fn ns_ceil_seconds(ns: i64) -> i64 {
    ns.div_euclid(1_000_000_000) + i64::from(ns.rem_euclid(1_000_000_000) != 0)
}

fn tag_scope_from_name(value: &str) -> Option<TagScope> {
    match value {
        "resource" => Some(TagScope::Resource),
        "span" => Some(TagScope::Span),
        "intrinsic" => Some(TagScope::Intrinsic),
        "event" => Some(TagScope::Event),
        "link" => Some(TagScope::Link),
        "instrumentation" => Some(TagScope::Instrumentation),
        _ => None,
    }
}

fn scoped_tags_from_json(json: &serde_json::Value) -> Result<Vec<ScopedTag>> {
    let scopes = json
        .get("scopes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            TraceqlError::Plan("remote live-store tags response missing scopes".into())
        })?;
    let mut out = Vec::new();
    for scope in scopes {
        let Some(name) = scope.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(scope_name) = tag_scope_from_name(name) else {
            continue;
        };
        let tags = scope
            .get("tags")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|tag| tag.as_str().map(ToString::to_string))
            .collect();
        out.push(ScopedTag {
            scope: scope_name,
            tags,
        });
    }
    Ok(out)
}

fn typed_values_from_json(json: &serde_json::Value) -> Result<Vec<TypedValue>> {
    let values = json
        .get("tagValues")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            TraceqlError::Plan("remote live-store tag values response missing tagValues".into())
        })?;
    Ok(values
        .iter()
        .filter_map(|value| {
            Some(TypedValue {
                type_: value.get("type")?.as_str()?.to_string(),
                value: value.get("value")?.as_str()?.to_string(),
            })
        })
        .collect())
}

fn trace_spans_from_otlp(trace_id: &[u8; 16], data: TracesData) -> Result<TraceSpans> {
    let mut trace = TraceSpans {
        trace_id: *trace_id,
        root_service_name: String::new(),
        root_trace_name: String::new(),
        resource_attributes: Vec::new(),
        spans: Vec::new(),
    };
    for resource_spans in data.resource_spans {
        let resource_attrs = resource_spans
            .resource
            .as_ref()
            .map_or_else(Vec::new, |resource| attrs_from_otlp(&resource.attributes));
        if trace.resource_attributes.is_empty() {
            trace.resource_attributes.clone_from(&resource_attrs);
        }
        if trace.root_service_name.is_empty() {
            trace.root_service_name = resource_attrs
                .iter()
                .find_map(|(key, value)| {
                    (key == "service.name").then(|| match value {
                        AttrValue::Str(value) => Some(value.clone()),
                        _ => None,
                    })?
                })
                .unwrap_or_default();
        }
        for scope_spans in resource_spans.scope_spans {
            let (instrumentation_name, instrumentation_version) = scope_spans
                .scope
                .map_or_else(Default::default, |scope| (scope.name, scope.version));
            for span in scope_spans.spans {
                let span_id = fixed_8(&span.span_id)?;
                let parent_span_id = if span.parent_span_id.is_empty() {
                    None
                } else {
                    Some(fixed_8(&span.parent_span_id)?)
                };
                let duration = time_from_nanos_u64(
                    span.end_time_unix_nano
                        .saturating_sub(span.start_time_unix_nano),
                );
                if trace.root_trace_name.is_empty() && parent_span_id.is_none() {
                    trace.root_trace_name.clone_from(&span.name);
                }
                let status = span.status.unwrap_or_default();
                trace.spans.push(SpanRef {
                    span_id,
                    parent_span_id,
                    name: span.name,
                    kind: span.kind,
                    nested_set_left: 0,
                    nested_set_right: 0,
                    nested_set_parent: 0,
                    start_time_unix_nano: span.start_time_unix_nano,
                    duration,
                    status_code: status.code,
                    status_message: status.message,
                    instrumentation_name: instrumentation_name.clone(),
                    instrumentation_version: instrumentation_version.clone(),
                    resource_attributes: resource_attrs.clone(),
                    attributes: attrs_from_otlp(&span.attributes),
                    events: span
                        .events
                        .into_iter()
                        .map(|event| EventRef {
                            time_since_start: time_from_nanos_u64(
                                event
                                    .time_unix_nano
                                    .saturating_sub(span.start_time_unix_nano),
                            ),
                            name: event.name,
                            attributes: attrs_from_otlp(&event.attributes),
                        })
                        .collect(),
                    links: span
                        .links
                        .into_iter()
                        .map(|link| {
                            Ok(LinkRef {
                                trace_id: fixed_16(&link.trace_id)?,
                                span_id: fixed_8(&link.span_id)?,
                                attributes: attrs_from_otlp(&link.attributes),
                            })
                        })
                        .collect::<Result<Vec<_>>>()?,
                });
            }
        }
    }
    if trace.root_trace_name.is_empty() {
        trace.root_trace_name = trace
            .spans
            .first()
            .map(|span| span.name.clone())
            .unwrap_or_default();
    }
    Ok(trace)
}

fn attrs_from_otlp(
    attrs: &[opentelemetry_proto::tonic::common::v1::KeyValue],
) -> Vec<(String, AttrValue)> {
    attrs
        .iter()
        .filter_map(|attr| {
            attr.value
                .as_ref()
                .and_then(attr_value_from_otlp)
                .map(|value| (attr.key.clone(), value))
        })
        .collect()
}

fn attr_value_from_otlp(value: &AnyValue) -> Option<AttrValue> {
    match value.value.as_ref()? {
        OtlpValue::StringValue(value) => Some(AttrValue::Str(value.clone())),
        OtlpValue::IntValue(value) => Some(AttrValue::Int(*value)),
        OtlpValue::DoubleValue(value) => Some(AttrValue::Float(*value)),
        OtlpValue::BoolValue(value) => Some(AttrValue::Bool(*value)),
        OtlpValue::BytesValue(value) => Some(AttrValue::Str(hex::encode(value))),
        OtlpValue::ArrayValue(array) => array.values.first().and_then(attr_value_from_otlp),
        OtlpValue::KvlistValue(_) | OtlpValue::StringValueStrindex(_) => None,
    }
}

fn fixed_16(bytes: &[u8]) -> Result<[u8; 16]> {
    bytes
        .try_into()
        .map_err(|_| TraceqlError::Plan("expected 16-byte trace id".into()))
}

fn fixed_8(bytes: &[u8]) -> Result<[u8; 8]> {
    bytes
        .try_into()
        .map_err(|_| TraceqlError::Plan("expected 8-byte span id".into()))
}

pub struct LiveTier {
    source: Arc<dyn LiveSource>,
}

impl LiveTier {
    #[must_use]
    pub fn new(source: Arc<dyn LiveSource>) -> Self {
        Self { source }
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<RecordBatch>> {
        self.source.span_batches(tenant, start_ns, end_ns).await
    }

    ///
    /// # Errors
    /// Returns an error when the live source query fails.
    pub async fn trace_spans(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceSpans>> {
        self.source.trace_spans(tenant, trace_id).await
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<ScopedTag>> {
        self.source.tag_names(tenant, scope, start_ns, end_ns).await
    }

    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<Vec<TypedValue>> {
        self.source.tag_values(tenant, tag, start_ns, end_ns).await
    }

    #[must_use]
    pub fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        self.source.block_builder_frontier_ns(tenant)
    }
}

#[cfg(test)]
mod tests {

    /// The remaining remote reads -- span batches, tag names and tag values --
    /// each collapse to an empty result, and each shares one failure path
    /// through `get_json`. An empty result is what a caller sees when the
    /// live tier genuinely holds nothing, so a body that always returns empty
    /// is invisible unless something non-empty comes back; and a remote that
    /// fails must raise rather than report emptiness, or a federated query
    /// silently loses a shard.
    ///
    /// The tenant header is echoed into the response, so a request that drops
    /// it is caught too: without that, every tenant reads alike.
    #[tokio::test]
    async fn the_remote_live_reads_return_what_the_remote_sent() {
        use axum::{
            Router,
            body::Body,
            extract::{Path, RawQuery},
            http::{HeaderMap, StatusCode},
            response::Response,
            routing::get,
        };

        use crate::querier::live::LiveSource as _;

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("n", arrow::datatypes::DataType::Int32, false),
        ]));
        let batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
            )
            .expect("the batch is well formed"),
        ];
        let stream = super::encode_span_batches(&batches).expect("the batches encode");

        let json_ok = |body: String| {
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(body))
                .expect("the response builds")
        };
        let app = Router::new()
            .route(
                super::LIVE_SPAN_BATCHES_PATH,
                get(move |headers: HeaderMap| {
                    let stream = stream.clone();
                    async move {
                        // Refuse anything but the tenant under test, so a
                        // request that sends a fixed tenant fails outright.
                        if headers.get("x-scope-orgid").map(|value| value.as_bytes())
                            != Some(b"t".as_slice())
                        {
                            return Response::builder()
                                .status(StatusCode::FORBIDDEN)
                                .body(Body::empty())
                                .expect("the response builds");
                        }
                        Response::builder()
                            .status(StatusCode::OK)
                            .body(Body::from(stream))
                            .expect("the response builds")
                    }
                }),
            )
            .route(
                "/api/v2/search/tags",
                get(move |headers: HeaderMap, RawQuery(query): RawQuery| async move {
                    // Echo both the tenant and the requested scope, so a
                    // request that drops either is distinguishable from one
                    // that sends it.
                    let tenant = headers
                        .get("x-scope-orgid")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or("none")
                        .to_string();
                    let scope = query
                        .unwrap_or_default()
                        .split('&')
                        .find_map(|pair| pair.strip_prefix("scope=").map(str::to_string))
                        .unwrap_or_else(|| "no-scope".to_string());
                    json_ok(format!(
                        r#"{{"scopes":[{{"name":"span","tags":["{tenant}","{scope}"]}}]}}"#
                    ))
                }),
            )
            .route(
                "/api/v2/search/tag/{tag}/values",
                get(move |Path(tag): Path<String>| async move {
                    if tag == "boom" {
                        // Valid JSON with a 500. If the status check is
                        // dropped, this parses cleanly and the call wrongly
                        // succeeds -- an empty body would have failed to
                        // parse and hidden that.
                        return Response::builder()
                            .status(StatusCode::INTERNAL_SERVER_ERROR)
                            .body(Body::from(
                                r#"{"tagValues":[{"type":"string","value":"boom"}]}"#,
                            ))
                            .expect("the response builds");
                    }
                    json_ok(format!(
                        r#"{{"tagValues":[{{"type":"string","value":"{tag}"}}]}}"#
                    ))
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free port");
        let addr = listener.local_addr().expect("the port is bound");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("the server runs");
        });

        let source = RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}/")).expect("a valid url"),
            Arc::new(arc_swap::ArcSwap::from_pointee(
                crabka_blockstore::TraceIndex::new(),
            )),
        );

        // Span batches come back as sent, not as an empty tier.
        check!(
            source.span_batches("t", 0, 10_000).await.expect("batches read") == batches,
            "the remote's batches are returned, not an empty list"
        );

        // Tag names carry the scope, and the echoed tenant proves the header
        // reached the remote.
        let tags = source
            .tag_names("t", Some(TagScope::Span), 0, 10_000)
            .await
            .expect("tags read");
        check!(tags.len() == 1);
        check!(tags[0].scope == TagScope::Span);
        check!(
            tags[0].tags == vec!["t".to_string(), "span".to_string()],
            "the tenant header and the scope parameter both reached the remote"
        );

        // Tag values echo the tag asked for, so a request built with a fixed
        // tag is caught alongside a body that returns nothing.
        check!(
            source.tag_values("t", "http.method", 0, 10_000).await.expect("values read")
                == vec![TypedValue {
                    type_: "string".to_string(),
                    value: "http.method".to_string(),
                }]
        );

        // A failing remote raises rather than reporting an empty result.
        check!(source.tag_values("t", "boom", 0, 10_000).await.is_err());
    }

    /// `trace_spans` over the federation endpoint has three outcomes that a
    /// caller must be able to tell apart: the trace is here, the trace is
    /// genuinely absent, or the remote failed. A body collapsed to `Ok(None)`
    /// makes the first two identical, and swapping the not-found test makes
    /// all three wrong in different directions -- so each outcome is served
    /// by a real HTTP response rather than asserted in isolation.
    #[tokio::test]
    async fn a_remote_live_trace_tells_found_absent_and_failed_apart() {
        use axum::{
            Router, body::Body, extract::Path, http::StatusCode, response::Response, routing::get,
        };
        use opentelemetry_proto::tonic::{
            resource::v1::Resource,
            trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan},
        };
        use prost::Message as _;

        use crate::querier::live::LiveSource as _;

        let found = [0xAA_u8; 16];
        let absent = [0xBB_u8; 16];
        let failing = [0xCC_u8; 16];

        let mut body = Vec::new();
        TracesData {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource::default()),
                scope_spans: vec![ScopeSpans {
                    spans: vec![OtlpSpan {
                        trace_id: found.to_vec(),
                        span_id: vec![1; 8],
                        name: "root-op".to_string(),
                        start_time_unix_nano: 1_000,
                        end_time_unix_nano: 1_200,
                        ..OtlpSpan::default()
                    }],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        }
        .encode(&mut body)
        .expect("the payload encodes");

        let found_hex = hex::encode(found);
        let absent_hex = hex::encode(absent);
        let app = Router::new().route(
            "/api/traces/{id}",
            get(move |Path(id): Path<String>| {
                let body = body.clone();
                let (found_hex, absent_hex) = (found_hex.clone(), absent_hex.clone());
                async move {
                    let (status, payload) = if id == found_hex {
                        (StatusCode::OK, Body::from(body))
                    } else if id == absent_hex {
                        (StatusCode::NOT_FOUND, Body::empty())
                    } else {
                        (StatusCode::INTERNAL_SERVER_ERROR, Body::empty())
                    };
                    Response::builder()
                        .status(status)
                        .body(payload)
                        .expect("the response builds")
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free port");
        let addr = listener.local_addr().expect("the port is bound");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("the server runs");
        });

        let source = RemoteLiveSource::new(
            Url::parse(&format!("http://{addr}/")).expect("a valid url"),
            Arc::new(arc_swap::ArcSwap::from_pointee(
                crabka_blockstore::TraceIndex::new(),
            )),
        );

        // Present: the trace comes back, and it is the one that was asked for.
        let trace = source
            .trace_spans("t", &found)
            .await
            .expect("a found trace is not an error")
            .expect("a found trace is not absent");
        check!(trace.trace_id == found);
        check!(trace.root_trace_name == "root-op");

        // Absent: None, not an error -- a 404 from the live tier means the
        // trace is not there yet, which callers fall through on.
        check!(
            source
                .trace_spans("t", &absent)
                .await
                .expect("a 404 is not an error")
                .is_none()
        );

        // Failed: an error, not None. Reporting a broken remote as "absent"
        // would silently drop results from a federated query.
        check!(source.trace_spans("t", &failing).await.is_err());
    }

    /// `encode_span_batches` writes an Arrow IPC stream that
    /// `decode_span_batches` reads back. Round-tripping is what pins it: a
    /// body collapsed to a fixed byte is not a decodable stream at all, and
    /// one collapsed to no bytes decodes to no batches.
    #[test]
    fn span_batches_round_trip_through_the_ipc_stream() {
        use arrow::{
            array::Int32Array,
            datatypes::{DataType, Field, Schema},
        };

        let schema = Arc::new(Schema::new(vec![Field::new("n", DataType::Int32, false)]));
        let batch = |values: Vec<i32>| {
            RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))])
                .expect("the batch is well formed")
        };
        // Two batches of different lengths, so a writer that emits only the
        // first is caught as well as one that emits none.
        let batches = vec![batch(vec![1, 2, 3]), batch(vec![4])];

        let encoded = super::encode_span_batches(&batches).expect("the batches encode");
        check!(!encoded.is_empty(), "a real stream has bytes");
        check!(
            super::decode_span_batches(&encoded).expect("the stream decodes") == batches,
            "and reads back as what went in"
        );

        // No batches means no stream, and no stream means no batches. The
        // two halves have to agree, or an empty live tier is an error.
        check!(super::encode_span_batches(&[]).expect("nothing encodes").is_empty());
        check!(super::decode_span_batches(&[]).expect("nothing decodes").is_empty());
    }

    /// The block-builder frontier is one nanosecond past the newest block, so
    /// a reader can ask for everything at or after it without re-reading that
    /// block. It is a maximum over blocks, not the first or the last, so the
    /// newest block is placed in the middle of the list.
    #[test]
    fn the_block_frontier_is_one_past_the_newest_block() {
        use std::collections::{BTreeMap, BTreeSet};

        use arc_swap::ArcSwap;
        use crabka_blockstore::{ShardedTraceBloom, TraceBlockStats, TraceIndex};

        let block = |key: &str, min_ts, max_ts| TraceBlockStats {
            object_key: key.to_string(),
            min_ts,
            max_ts,
            bloom: ShardedTraceBloom::with_tempo_defaults(1),
            tag_names: BTreeSet::new(),
            tag_values: BTreeMap::new(),
        };
        let mut index = TraceIndex::new();
        // The newest is neither first nor last, so taking either end is wrong.
        index.add_trace_block("t", block("a", 100, 500));
        index.add_trace_block("t", block("b", 200, 900));
        index.add_trace_block("t", block("c", 300, 700));
        // A second tenant with a later block, which must not leak across.
        index.add_trace_block("other", block("d", 400, 5_000));

        let source = RemoteLiveSource::new(
            Url::parse("http://localhost:1/").expect("a valid url"),
            Arc::new(ArcSwap::from_pointee(index)),
        );

        check!(source.block_builder_frontier_ns("t") == 901, "one past the newest");
        check!(source.block_builder_frontier_ns("other") == 5_001);
        check!(
            source.block_builder_frontier_ns("absent") == 0,
            "a tenant with no blocks has no frontier"
        );
    }

    /// `trace_spans_from_otlp` folds an OTLP payload into one trace, picking
    /// the root service and root span names as it goes. Both choices are
    /// first-wins, and the guards that make them first-wins are what survived.
    ///
    /// The root-name guard is `name is empty AND this span has no parent`.
    /// Loosening it to `OR` is invisible on an ordinary trace: any span that
    /// satisfies one half is followed by one satisfying both, which overwrites
    /// the difference away. The two shapes below are the ones where it shows.
    #[test]
    fn an_otlp_trace_takes_its_root_names_from_the_first_span_that_qualifies() {
        use opentelemetry_proto::tonic::{
            common::v1::KeyValue as OtlpKv,
            resource::v1::Resource,
            trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan},
        };

        let attr = |key: &str, value: &str| OtlpKv {
            key: key.to_string(),
            value: Some(AnyValue {
                value: Some(OtlpValue::StringValue(value.to_string())),
            }),
            ..OtlpKv::default()
        };
        let span = |name: &str, id: u8, parent: Option<u8>| OtlpSpan {
            trace_id: vec![7; 16],
            span_id: vec![id; 8],
            parent_span_id: parent.map(|p| vec![p; 8]).unwrap_or_default(),
            name: name.to_string(),
            start_time_unix_nano: 1_000,
            end_time_unix_nano: 1_200,
            ..OtlpSpan::default()
        };
        let resource = |spans: Vec<OtlpSpan>, attrs: Vec<OtlpKv>| ResourceSpans {
            resource: Some(Resource {
                attributes: attrs,
                ..Resource::default()
            }),
            scope_spans: vec![ScopeSpans {
                spans,
                ..ScopeSpans::default()
            }],
            ..ResourceSpans::default()
        };
        let payload = |spans: Vec<OtlpSpan>, attrs: Vec<OtlpKv>| TracesData {
            resource_spans: vec![resource(spans, attrs)],
        };

        // The service name is read from the attribute of that name, not from
        // whichever attribute happens to come first.
        let trace = super::trace_spans_from_otlp(
            &[7; 16],
            payload(
                vec![span("root-op", 1, None)],
                vec![attr("cloud.region", "us-east-1"), attr("service.name", "svc")],
            ),
        )
        .expect("the payload converts");
        check!(trace.root_service_name == "svc");
        check!(trace.root_trace_name == "root-op");

        // Two roots: the first wins. Loosening the guard to `OR` lets the
        // second overwrite it, since its name is non-empty but it is a root.
        let trace = super::trace_spans_from_otlp(
            &[7; 16],
            payload(
                vec![span("first-root", 1, None), span("second-root", 2, None)],
                vec![attr("service.name", "svc")],
            ),
        )
        .expect("the payload converts");
        check!(trace.root_trace_name == "first-root", "the first root wins");

        // No root span at all: the guard never fires, and a fallback names the
        // trace after its first span rather than leaving it blank.
        let trace = super::trace_spans_from_otlp(
            &[7; 16],
            payload(
                vec![span("child-op", 2, Some(1)), span("other-child", 3, Some(1))],
                vec![attr("service.name", "svc")],
            ),
        )
        .expect("the payload converts");
        check!(
            trace.root_trace_name == "child-op",
            "the fallback names a rootless trace after its first span"
        );
        check!(trace.spans.len() == 2, "and its spans are still carried");

        // Two resource batches naming different services: the first wins.
        // With only one batch the first-wins guard is trivially true, so
        // dropping it changes nothing -- this is the shape that shows it.
        let trace = super::trace_spans_from_otlp(
            &[7; 16],
            TracesData {
                resource_spans: vec![
                    resource(
                        vec![span("root-op", 1, None)],
                        vec![attr("service.name", "first-svc")],
                    ),
                    resource(
                        vec![span("later-op", 2, Some(1))],
                        vec![attr("service.name", "second-svc")],
                    ),
                ],
            },
        )
        .expect("the payload converts");
        check!(
            trace.root_service_name == "first-svc",
            "the first resource batch names the trace"
        );
        check!(trace.spans.len() == 2, "and both batches contribute spans");
    }

    /// `tag_scope_name` names a scope for the wire. The six names are
    /// asserted to be pairwise distinct, so an arm returning a neighbour's
    /// name cannot pass for its own.
    #[test]
    fn every_tag_scope_has_its_own_wire_name() {
        let name = super::tag_scope_name;

        check!(name(TagScope::Resource) == "resource");
        check!(name(TagScope::Span) == "span");
        check!(name(TagScope::Intrinsic) == "intrinsic");
        check!(name(TagScope::Event) == "event");
        check!(name(TagScope::Link) == "link");
        check!(name(TagScope::Instrumentation) == "instrumentation");

        let mut names = vec![
            name(TagScope::Resource),
            name(TagScope::Span),
            name(TagScope::Intrinsic),
            name(TagScope::Event),
            name(TagScope::Link),
            name(TagScope::Instrumentation),
        ];
        names.sort_unstable();
        names.dedup();
        check!(names.len() == 6, "the six names must all differ: {names:?}");
    }

    /// `ns_floor_seconds` is the twin of `ns_ceil_seconds`, rounding *down*
    /// rather than toward zero. The two only disagree on negatives, so the
    /// sub-second negative cases are what pin the direction. The answers also
    /// avoid 0, 1 and -1 where they can, since a body collapsed to any of
    /// those constants is otherwise indistinguishable.
    #[test]
    fn nanoseconds_floor_down_to_whole_seconds() {
        let floor = super::ns_floor_seconds;

        check!(floor(5_000_000_000) == 5);
        check!(floor(5_999_999_999) == 5, "a partial second is dropped");
        check!(floor(7_000_000_000) == 7);
        check!(floor(0) == 0);

        // Below zero the two roundings part company: truncation would give 0
        // here, and -1 for the whole second below it.
        check!(floor(-1) == -1, "rounding down, not toward zero");
        check!(floor(-999_999_999) == -1);
        check!(floor(-1_000_000_000) == -1, "an exact second is not rounded further");
        check!(floor(-1_000_000_001) == -2);
        check!(floor(-5_000_000_000) == -5);
    }

    /// `ns_ceil_seconds` rounds nanoseconds up to whole seconds, and rounds
    /// *up* rather than toward zero -- which is only visible on negatives,
    /// where the two disagree. Euclidean division is what makes that work.
    #[test]
    fn nanoseconds_round_up_to_whole_seconds() {
        let ceil = super::ns_ceil_seconds;

        check!(ceil(0) == 0);
        check!(ceil(1) == 1, "any remainder rounds up");
        check!(ceil(999_999_999) == 1, "just under a second is still a second");
        check!(ceil(1_000_000_000) == 1, "exactly a second does not round up");
        check!(ceil(1_000_000_001) == 2);
        check!(ceil(2_000_000_000) == 2);

        // Negatives round toward positive infinity, not toward zero.
        check!(ceil(-1) == 0, "just below zero rounds up to zero");
        check!(ceil(-999_999_999) == 0);
        check!(ceil(-1_000_000_000) == -1, "exactly a second is exact either way");
        check!(ceil(-1_000_000_001) == -1);
    }

    /// `tag_scope_from_name` refuses what it does not know, rather than
    /// falling back to a scope. Every entry is checked, since a table is where
    /// two names quietly map to one variant.
    #[test]
    fn a_tag_scope_name_maps_only_to_its_own_scope() {
        use super::TagScope;
        let scope = super::tag_scope_from_name;

        check!(scope("resource") == Some(TagScope::Resource));
        check!(scope("span") == Some(TagScope::Span));
        check!(scope("intrinsic") == Some(TagScope::Intrinsic));
        check!(scope("event") == Some(TagScope::Event));
        check!(scope("link") == Some(TagScope::Link));
        check!(scope("instrumentation") == Some(TagScope::Instrumentation));

        check!(scope("") == None);
        check!(scope("Span") == None, "case-sensitive");
        check!(scope("spans") == None, "not a prefix match");
        check!(scope("unknown") == None);
    }
    use std::{collections::BTreeMap, sync::Arc};

    use arrow::record_batch::RecordBatch;
    use assert2::check;
    use crabka_traceql::{AttrValue, ScopedTag, SpanRef, TagScope, TraceSpans, TypedValue};
    use crabka_units::nanos;

    use super::*;

    #[derive(Default)]
    struct FakeLiveSource {
        batches: Vec<RecordBatch>,
        trace: Option<TraceSpans>,
        tags: Vec<ScopedTag>,
        values: Vec<TypedValue>,
        frontiers: BTreeMap<String, i64>,
    }

    #[async_trait::async_trait]
    impl LiveSource for FakeLiveSource {
        async fn span_batches(
            &self,
            _tenant: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<RecordBatch>> {
            Ok(self.batches.clone())
        }

        async fn trace_spans(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<TraceSpans>> {
            Ok(self.trace.clone())
        }

        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<ScopedTag>> {
            Ok(self.tags.clone())
        }

        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<TypedValue>> {
            Ok(self.values.clone())
        }

        fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
            self.frontiers.get(tenant).copied().unwrap_or_default()
        }
    }

    fn trace() -> TraceSpans {
        TraceSpans {
            trace_id: [1; 16],
            root_service_name: "api".into(),
            root_trace_name: "GET /".into(),
            resource_attributes: vec![("service.name".into(), AttrValue::Str("api".into()))],
            spans: vec![SpanRef {
                span_id: [2; 8],
                parent_span_id: None,
                name: "GET /".into(),
                kind: 0,
                nested_set_left: 1,
                nested_set_right: 2,
                nested_set_parent: 0,
                start_time_unix_nano: 2_000,
                duration: nanos(50),
                status_code: 0,
                status_message: String::new(),
                instrumentation_name: String::new(),
                instrumentation_version: String::new(),
                resource_attributes: vec![("service.name".into(), AttrValue::Str("api".into()))],
                attributes: vec![("svc".into(), AttrValue::Str("api".into()))],
                events: Vec::new(),
                links: Vec::new(),
            }],
        }
    }

    #[tokio::test]
    async fn live_tier_delegates_reads_to_source() {
        let mut source = FakeLiveSource {
            trace: Some(trace()),
            tags: vec![ScopedTag {
                scope: TagScope::Span,
                tags: vec!["svc".into()],
            }],
            values: vec![TypedValue {
                type_: "string".into(),
                value: "api".into(),
            }],
            ..FakeLiveSource::default()
        };
        source.frontiers.insert("tenant-a".into(), 1_500);
        let live = LiveTier::new(Arc::new(source));

        check!(live.block_builder_frontier_ns("tenant-a") == 1_500);
        check!(
            live.span_batches("tenant-a", 0, 5_000)
                .await
                .unwrap()
                .is_empty()
        );
        check!(
            live.trace_spans("tenant-a", &[1; 16])
                .await
                .unwrap()
                .unwrap()
                .spans
                .len()
                == 1
        );
        check!(
            live.tag_names("tenant-a", Some(TagScope::Span), 0, 5_000)
                .await
                .unwrap()[0]
                .tags
                == vec!["svc"]
        );
        check!(live.tag_values("tenant-a", ".svc", 0, 5_000).await.unwrap()[0].value == "api");
    }
}
