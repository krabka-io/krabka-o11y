//! Read-side wrapper over the traces hot tier.

use std::sync::Arc;

use arrow::{
    ipc::{reader::StreamReader, writer::StreamWriter},
    record_batch::RecordBatch,
};
use krabka_traceql::{
    AttrValue, EventRef, LinkRef, ScopedTag, SpanRef, TagScope, TraceSpans, TraceqlError,
    TypedValue,
};
use krabka_units::{Time, convert::TimeExt as _};
use opentelemetry_proto::tonic::{
    common::v1::{AnyValue, any_value::Value as OtlpValue},
    trace::v1::TracesData,
};
use prost::Message as _;
use reqwest::Url;

use super::store::SharedTraceIndex;

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
                        if headers
                            .get("x-scope-orgid")
                            .map(object_store::HeaderValue::as_bytes)
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
                get(
                    move |headers: HeaderMap, RawQuery(query): RawQuery| async move {
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
                    },
                ),
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
                krabka_blockstore::TraceIndex::new(),
            )),
        );

        // Span batches come back as sent, not as an empty tier.
        check!(
            source
                .span_batches("t", 0, 10_000)
                .await
                .expect("batches read")
                == batches,
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
            source
                .tag_values("t", "http.method", 0, 10_000)
                .await
                .expect("values read")
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
                krabka_blockstore::TraceIndex::new(),
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
        check!(
            super::encode_span_batches(&[])
                .expect("nothing encodes")
                .is_empty()
        );
        check!(
            super::decode_span_batches(&[])
                .expect("nothing decodes")
                .is_empty()
        );
    }

    /// The block-builder frontier is one nanosecond past the newest block, so
    /// a reader can ask for everything at or after it without re-reading that
    /// block. It is a maximum over blocks, not the first or the last, so the
    /// newest block is placed in the middle of the list.
    #[test]
    fn the_block_frontier_is_one_past_the_newest_block() {
        use std::collections::{BTreeMap, BTreeSet};

        use arc_swap::ArcSwap;
        use krabka_blockstore::{ShardedTraceBloom, TraceBlockStats, TraceIndex};

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

        check!(
            source.block_builder_frontier_ns("t") == 901,
            "one past the newest"
        );
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
                vec![
                    attr("cloud.region", "us-east-1"),
                    attr("service.name", "svc"),
                ],
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
                vec![
                    span("child-op", 2, Some(1)),
                    span("other-child", 3, Some(1)),
                ],
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
        check!(
            floor(-1_000_000_000) == -1,
            "an exact second is not rounded further"
        );
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
        check!(
            ceil(999_999_999) == 1,
            "just under a second is still a second"
        );
        check!(
            ceil(1_000_000_000) == 1,
            "exactly a second does not round up"
        );
        check!(ceil(1_000_000_001) == 2);
        check!(ceil(2_000_000_000) == 2);

        // Negatives round toward positive infinity, not toward zero.
        check!(ceil(-1) == 0, "just below zero rounds up to zero");
        check!(ceil(-999_999_999) == 0);
        check!(
            ceil(-1_000_000_000) == -1,
            "exactly a second is exact either way"
        );
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
    use krabka_traceql::{AttrValue, ScopedTag, SpanRef, TagScope, TraceSpans, TypedValue};
    use krabka_units::nanos;

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

// === split-modules: generated submodules ===
mod attr_value_from_otlp;
mod attrs_from_otlp;
mod decode_span_batches;
mod encode_span_batches;
mod fixed_16;
mod fixed_8;
mod live_source;
mod live_span_batches_path;
mod live_tier;
mod ns_ceil_seconds;
mod ns_floor_seconds;
mod remote_live_source;
mod result;
mod scoped_tags_from_json;
mod tag_scope_from_name;
mod tag_scope_name;
mod time_from_nanos_u64;
mod trace_spans_from_otlp;
mod typed_values_from_json;

use attr_value_from_otlp::attr_value_from_otlp;
use attrs_from_otlp::attrs_from_otlp;
use decode_span_batches::decode_span_batches;
pub use encode_span_batches::encode_span_batches;
use fixed_8::fixed_8;
use fixed_16::fixed_16;
pub use live_source::LiveSource;
use live_span_batches_path::LIVE_SPAN_BATCHES_PATH;
pub use live_tier::LiveTier;
use ns_ceil_seconds::ns_ceil_seconds;
use ns_floor_seconds::ns_floor_seconds;
pub use remote_live_source::RemoteLiveSource;
pub use result::Result;
use scoped_tags_from_json::scoped_tags_from_json;
use tag_scope_from_name::tag_scope_from_name;
use tag_scope_name::tag_scope_name;
use time_from_nanos_u64::time_from_nanos_u64;
use trace_spans_from_otlp::trace_spans_from_otlp;
use typed_values_from_json::typed_values_from_json;
