//! Distributor role: push-door HTTP routes into the traces WAL.

use std::{collections::BTreeMap, io::Read, net::SocketAddr, sync::Arc};

use axum::{
    Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::post,
};
use flate2::read::GzDecoder;
use krabka_client_producer::{Header, Producer, ProducerRecord};
use krabka_units::{
    ByteSize, Frequency,
    convert::{ByteSizeExt as _, FrequencyExt, StdDurationExt as _},
    kibibytes, mebibytes,
};
use opentelemetry_proto::tonic::{
    collector::trace::v1::{
        ExportTraceServiceRequest, ExportTraceServiceResponse,
        trace_service_server::{TraceService, TraceServiceServer},
    },
    trace::v1::TracesData,
};
use prost::Message as _;
use tokio_util::sync::CancellationToken;
use tonic::{
    Request as GrpcRequest, Response as GrpcResponse, Status as GrpcStatus, metadata::MetadataMap,
    transport::Server as GrpcServer,
};
use tracing::Instrument as _;

use crate::{
    error::{TracesError, tempo_error_response},
    limits::{IngestEnforcer, LimitError, Limits, OverridesProvider},
    metrics::ServiceMetrics,
    span::{AttrValue, KeyValue, Span},
    wal::{SpanRecord, TRACES_WAL_TOPIC, partition_key},
    wire::{
        jaeger::{decode_jaeger_binary_thrift, decode_jaeger_thrift},
        jaeger_grpc::{
            api_v2::{
                PostSpansRequest, PostSpansResponse,
                collector_service_server::{CollectorService, CollectorServiceServer},
            },
            decode_jaeger_grpc_batch,
        },
        otlp::decode_otlp,
        zipkin::decode_zipkin,
    },
};

#[cfg(test)]
mod tests {
    use std::{io::Write as _, sync::Mutex};

    use assert2::check;
    use axum::{body::Body, http::Request};
    use flate2::{Compression, write::GzEncoder};
    use http_body_util::BodyExt as _;
    use krabka_units::{bytes, per_sec};
    use opentelemetry_proto::tonic::{
        collector::trace::v1::ExportTraceServiceRequest,
        trace::v1::{ResourceSpans, ScopeSpans, Span as OtlpSpan, TracesData},
    };
    use prost::Message as _;
    use tonic::Request as GrpcRequest;
    use tower::ServiceExt as _;

    use super::*;

    /// `usize::MAX` is the "no limit" sentinel and converts to the zero the
    /// wire format reads as unlimited. Every other value converts to itself,
    /// which is what separates the sentinel test from its negation: inverted,
    /// it is every ordinary value that collapses to zero.
    #[test]
    fn the_no_limit_sentinel_converts_to_zero_and_nothing_else_does() {
        let limit = super::u64_limit_from_usize;

        check!(limit(usize::MAX) == 0, "the sentinel means unlimited");
        check!(limit(0) == 0, "and a real zero is already zero");
        check!(limit(1) == 1);
        check!(limit(7) == 7);
        check!(limit(usize::MAX - 1) == u64::try_from(usize::MAX - 1).expect("fits in u64"));
    }

    /// `decode_body` bounds how far a compressed body may expand. It reads
    /// one byte past the limit and then rejects anything longer, so both
    /// halves of that pair only differ from their mutations at the exact
    /// boundary: a payload of precisely the limit must come back whole, and
    /// one byte more must be refused.
    #[test]
    fn decoding_a_body_enforces_the_limit_at_the_exact_boundary() {
        let limit = ByteSize::from_bytes(64);
        let gzip = |payload: &[u8]| {
            let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
            encoder
                .write_all(payload)
                .expect("gzip accepts the payload");
            encoder.finish().expect("the gzip stream finishes")
        };
        let encoded = |name: &str| {
            let mut headers = HeaderMap::new();
            headers.insert(
                CONTENT_ENCODING,
                name.parse().expect("a valid header value"),
            );
            headers
        };
        let exact = vec![b'a'; 64];
        let over = vec![b'a'; 65];
        let gzipped = encoded("gzip");

        // Exactly at the limit: accepted, and returned whole. Reading only
        // `limit - 1` bytes would truncate this silently; rejecting on `>=`
        // would refuse it outright.
        check!(super::decode_body(&gzipped, &gzip(&exact), limit).expect("at the limit") == exact);

        // One byte over: refused. Reading only `limit` bytes would truncate
        // to exactly the limit and then report success.
        check!(let Err(TracesError::TooLarge { limit: 64 }) =
            super::decode_body(&gzipped, &gzip(&over), limit));

        // An absent header means identity, which is bounded by the same test.
        let plain = HeaderMap::new();
        check!(super::decode_body(&plain, &exact, limit).expect("no encoding header") == exact);
        check!(let Err(TracesError::TooLarge { .. }) = super::decode_body(&plain, &over, limit));
        check!(
            super::decode_body(&encoded("identity"), &exact, limit).expect("named identity")
                == exact
        );

        // The encoding name is matched case-insensitively ...
        check!(
            super::decode_body(&encoded("GZIP"), &gzip(&exact), limit).expect("GZIP is gzip")
                == exact
        );
        check!(
            super::decode_body(&encoded("Identity"), &exact, limit).expect("Identity is identity")
                == exact
        );

        // ... and any other encoding is refused by name rather than guessed at.
        check!(let Err(TracesError::UnsupportedContentType(_)) =
            super::decode_body(&encoded("br"), &exact, limit));
    }

    #[derive(Default)]
    struct RecordingSink {
        records: Mutex<Vec<SpanRecord>>,
    }

    #[async_trait::async_trait]
    impl WalSink for RecordingSink {
        async fn append(&self, rec: SpanRecord) -> Result<(), TracesError> {
            self.records.lock().unwrap().push(rec);
            Ok(())
        }
    }

    impl RecordingSink {
        fn count(&self) -> usize {
            self.records.lock().unwrap().len()
        }

        fn tenant(&self, idx: usize) -> String {
            self.records.lock().unwrap()[idx].tenant.clone()
        }

        fn span_name(&self, idx: usize) -> String {
            self.records.lock().unwrap()[idx].span.name.clone()
        }
    }

    fn test_state() -> (Arc<DistributorState>, Arc<RecordingSink>) {
        test_state_with_limits(TenantLimits::default())
    }

    fn test_state_with_limits(limits: TenantLimits) -> (Arc<DistributorState>, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        let mut state = DistributorState::new(sink.clone());
        state.shared_limits = limits.to_shared_limits();
        state.limits = limits;
        state.max_decompressed = mebibytes(1);
        (Arc::new(state), sink)
    }

    fn test_state_with_shared_limits(
        limits: crate::limits::Limits,
    ) -> (Arc<DistributorState>, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        let mut state = DistributorState::new(sink.clone());
        state.shared_limits = limits;
        state.max_decompressed = mebibytes(1);
        (Arc::new(state), sink)
    }

    fn test_state_with_overrides(
        overrides: crate::limits::OverridesProvider,
    ) -> (Arc<DistributorState>, Arc<RecordingSink>) {
        let sink = Arc::new(RecordingSink::default());
        let mut state = DistributorState::new(sink.clone());
        state.overrides = Some(overrides);
        state.max_decompressed = mebibytes(1);
        (Arc::new(state), sink)
    }

    fn otlp_body() -> Vec<u8> {
        TracesData {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans: vec![OtlpSpan {
                        trace_id: vec![1; 16],
                        span_id: vec![2; 8],
                        name: "GET /".into(),
                        start_time_unix_nano: 1_000,
                        end_time_unix_nano: 1_500,
                        ..OtlpSpan::default()
                    }],
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        }
        .encode_to_vec()
    }

    fn otlp_body_with_spans(n: u8) -> Vec<u8> {
        let spans = (0..n)
            .map(|idx| OtlpSpan {
                trace_id: vec![1; 16],
                span_id: vec![idx.saturating_add(1); 8],
                name: format!("span-{idx}"),
                start_time_unix_nano: 1_000 + u64::from(idx),
                end_time_unix_nano: 1_500 + u64::from(idx),
                ..OtlpSpan::default()
            })
            .collect();
        TracesData {
            resource_spans: vec![ResourceSpans {
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        }
        .encode_to_vec()
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).unwrap();
        encoder.finish().unwrap()
    }

    #[tokio::test]
    async fn otlp_push_returns_200_and_appends() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/x-protobuf")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        check!(resp.status() == StatusCode::OK);
        assert2::assert!(sink.count() == 1);
        assert2::assert!(sink.tenant(0) == "tenant-a".to_string());
    }

    #[tokio::test]
    async fn otlp_push_returns_export_response_protobuf() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/x-protobuf")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(resp.status() == StatusCode::OK);
        assert2::assert!(
            resp.headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                == Some("application/x-protobuf")
        );
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let response = ExportTraceServiceResponse::decode(body.as_ref()).unwrap();
        assert2::assert!(response.partial_success.is_none());
        assert2::assert!(sink.count() == 1);
    }

    #[tokio::test]
    async fn otlp_push_accepts_application_protobuf_content_type() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "application/protobuf")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(resp.status() == StatusCode::OK);
        assert2::assert!(sink.count() == 1);
    }

    #[tokio::test]
    async fn otlp_push_accepts_case_insensitive_gzip_encoding() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-encoding", "GZip")
                    .body(Body::from(gzip(&otlp_body())))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(resp.status() == StatusCode::OK);
        assert2::assert!(sink.count() == 1);
    }

    #[tokio::test]
    async fn otlp_push_rejects_declared_non_protobuf_content_type() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("content-type", "text/plain")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(resp.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert2::assert!(sink.count() == 0);
    }

    #[tokio::test]
    async fn otlp_grpc_export_appends_and_returns_success() {
        let (state, sink) = test_state();
        let service = OtlpGrpcService::new(state);
        let mut req = GrpcRequest::new(ExportTraceServiceRequest {
            resource_spans: TracesData::decode(otlp_body().as_slice())
                .unwrap()
                .resource_spans,
        });
        req.metadata_mut()
            .insert("x-scope-orgid", "tenant-a".parse().unwrap());

        let resp = service.export(req).await.unwrap();

        check!(resp.into_inner().partial_success.is_none());
        assert2::assert!(sink.count() == 1);
        assert2::assert!(sink.tenant(0) == "tenant-a".to_string());
    }

    #[tokio::test]
    async fn jaeger_grpc_post_spans_appends_and_returns_success() {
        let (state, sink) = test_state();
        let service = JaegerGrpcService::new(state);
        let mut req = GrpcRequest::new(crate::wire::jaeger_grpc::api_v2::PostSpansRequest {
            batch: Some(crate::wire::jaeger_grpc::api_v2::Batch {
                process: Some(crate::wire::jaeger_grpc::api_v2::Process {
                    service_name: "checkout".into(),
                    tags: Vec::new(),
                }),
                spans: vec![crate::wire::jaeger_grpc::api_v2::Span {
                    trace_id: vec![0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
                    span_id: vec![0, 0, 0, 0, 0, 0, 0, 3],
                    operation_name: "GET /grpc".into(),
                    start_time: Some(prost_types::Timestamp {
                        seconds: 1,
                        nanos: 2_000,
                    }),
                    duration: Some(prost_types::Duration {
                        seconds: 0,
                        nanos: 25_000,
                    }),
                    tags: vec![
                        crate::wire::jaeger_grpc::api_v2::KeyValue {
                            key: "span.kind".into(),
                            v_type: crate::wire::jaeger_grpc::api_v2::ValueType::String.into(),
                            v_str: "server".into(),
                            ..Default::default()
                        },
                        crate::wire::jaeger_grpc::api_v2::KeyValue {
                            key: "error".into(),
                            v_type: crate::wire::jaeger_grpc::api_v2::ValueType::Bool.into(),
                            v_bool: true,
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                }],
            }),
        });
        req.metadata_mut()
            .insert("x-scope-orgid", "tenant-a".parse().unwrap());

        let resp = service.post_spans(req).await.unwrap();

        assert2::assert!(sink.count() == 1);
        assert2::assert!(sink.tenant(0) == "tenant-a".to_string());
        assert2::assert!(sink.span_name(0) == "GET /grpc".to_string());
        check!(resp.into_inner() == crate::wire::jaeger_grpc::api_v2::PostSpansResponse {});
    }

    #[tokio::test]
    async fn tempo_push_uses_anonymous_tenant_when_header_absent() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/push")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(resp.status() == StatusCode::OK);
        assert2::assert!(sink.tenant(0) == "anonymous");
    }

    #[tokio::test]
    async fn zipkin_push_returns_202_and_appends() {
        let (state, sink) = test_state();
        let body = r#"[{"traceId":"0000000000000001","id":"0000000000000002","name":"x","timestamp":1,"duration":1}]"#;
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/v2/spans")
                    .header("content-type", "application/json")
                    .header("x-scope-orgid", "t")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(resp.status() == StatusCode::ACCEPTED);
        assert2::assert!(sink.count() == 1);
    }

    #[tokio::test]
    async fn jaeger_push_returns_202_and_appends() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/traces")
                    .header("x-scope-orgid", "t")
                    .body(Body::from(
                        crate::wire::jaeger::test_support::encode_sample_batch(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        check!(resp.status() == StatusCode::ACCEPTED);
        assert2::assert!(sink.count() == 1);
        assert2::assert!(sink.span_name(0) == "GET /".to_string());
    }

    #[tokio::test]
    async fn jaeger_binary_thrift_push_returns_202_and_appends() {
        let (state, sink) = test_state();
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/api/traces")
                    .header("content-type", "application/vnd.apache.thrift.binary")
                    .header("x-scope-orgid", "t")
                    .body(Body::from(jaeger_binary_batch()))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(resp.status() == StatusCode::ACCEPTED);
        assert2::assert!(sink.count() == 1);
        assert2::assert!(sink.span_name(0) == "GET /binary".to_string());
    }

    #[tokio::test]
    async fn jaeger_compact_datagram_appends() {
        let (state, sink) = test_state();

        handle_jaeger_compact_datagram(
            &state,
            "tenant-a",
            &crate::wire::jaeger::test_support::encode_sample_batch(),
        )
        .await
        .unwrap();

        assert2::assert!(sink.count() == 1);
        assert2::assert!(sink.tenant(0) == "tenant-a".to_string());
        assert2::assert!(sink.span_name(0) == "GET /".to_string());
    }

    #[tokio::test]
    async fn over_span_limit_is_400() {
        let limits = TenantLimits {
            max_spans_per_request: 0,
            ..TenantLimits::default()
        };
        let (state, sink) = test_state_with_limits(limits);
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(resp.status() == StatusCode::BAD_REQUEST);
        assert2::assert!(sink.count() == 0);
    }

    #[tokio::test]
    async fn oversized_trace_limit_is_400() {
        let limits = TenantLimits {
            max_spans_per_trace: 0,
            ..TenantLimits::default()
        };
        let (state, sink) = test_state_with_limits(limits);
        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(resp.status() == StatusCode::BAD_REQUEST);
        assert2::assert!(sink.count() == 0);
    }

    #[tokio::test]
    async fn shared_trace_span_limit_is_enforced_before_append() {
        let limits = crate::limits::Limits {
            max_spans_per_trace: 1,
            ..crate::limits::Limits::default()
        };
        let (state, sink) = test_state_with_shared_limits(limits);

        let resp = router(state)
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .body(Body::from(otlp_body_with_spans(2)))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(resp.status() == StatusCode::BAD_REQUEST);
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        check!(json["status"] == "error");
        check!(
            json["error"]
                .as_str()
                .is_some_and(|message| message.contains("max spans per trace"))
        );
        check!(sink.count() == 0);
    }

    #[tokio::test]
    async fn tenant_override_trace_span_limit_is_enforced_before_append() {
        let overrides = crate::limits::OverridesProvider::from_yaml(
            r"
overrides:
  tenant-tight:
    max_spans_per_trace: 1
",
        )
        .unwrap();
        let (state, sink) = test_state_with_overrides(overrides);
        let app = router(state);

        let tight = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-tight")
                    .body(Body::from(otlp_body_with_spans(2)))
                    .unwrap(),
            )
            .await
            .unwrap();
        let loose = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-loose")
                    .body(Body::from(otlp_body_with_spans(2)))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(tight.status() == StatusCode::BAD_REQUEST);
        check!(loose.status() == StatusCode::OK);
        assert2::assert!(sink.count() == 2);
        assert2::assert!(sink.tenant(0) == "tenant-loose".to_string());
        assert2::assert!(sink.tenant(1) == "tenant-loose".to_string());
    }

    #[tokio::test]
    async fn shared_ingest_rate_limit_is_per_tenant() {
        let limits = crate::limits::Limits {
            ingestion_rate: per_sec(1),
            ingestion_burst_spans: 1,
            ..crate::limits::Limits::default()
        };
        let (state, sink) = test_state_with_shared_limits(limits);
        let app = router(state);

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let other_tenant = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-b")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(first.status() == StatusCode::OK);
        assert2::assert!(second.status() == StatusCode::TOO_MANY_REQUESTS);
        let body = second.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        check!(json["status"] == "error");
        check!(
            json["error"]
                .as_str()
                .is_some_and(|message| message.contains("ingestion rate"))
        );
        check!(other_tenant.status() == StatusCode::OK);
        assert2::assert!(sink.count() == 2);
        assert2::assert!(sink.tenant(0) == "tenant-a".to_string());
        assert2::assert!(sink.tenant(1) == "tenant-b".to_string());
    }

    #[tokio::test]
    async fn ingest_rate_limit_is_per_tenant() {
        let limits = TenantLimits {
            max_ingest_rate: per_sec(1),
            ingest_rate_burst: 1,
            ..TenantLimits::default()
        };
        let (state, sink) = test_state_with_limits(limits);
        let app = router(state);

        let first = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let second = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();
        let other_tenant = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/traces")
                    .header("x-scope-orgid", "tenant-b")
                    .body(Body::from(otlp_body()))
                    .unwrap(),
            )
            .await
            .unwrap();

        check!(first.status() == StatusCode::OK);
        check!(second.status() == StatusCode::TOO_MANY_REQUESTS);
        check!(other_tenant.status() == StatusCode::OK);
        assert2::assert!(sink.count() == 2);
        assert2::assert!(sink.tenant(0) == "tenant-a".to_string());
        assert2::assert!(sink.tenant(1) == "tenant-b".to_string());
    }

    #[tokio::test]
    async fn otlp_grpc_limit_errors_are_resource_exhausted() {
        let limits = TenantLimits {
            max_spans_per_request: 0,
            ..TenantLimits::default()
        };
        let (state, sink) = test_state_with_limits(limits);
        let service = OtlpGrpcService::new(state);
        let req = GrpcRequest::new(ExportTraceServiceRequest {
            resource_spans: TracesData::decode(otlp_body().as_slice())
                .unwrap()
                .resource_spans,
        });

        let err = service.export(req).await.unwrap_err();

        assert2::assert!(err.code() == tonic::Code::ResourceExhausted);
        assert2::assert!(sink.count() == 0);
    }

    #[test]
    fn validate_rejects_large_attribute_values() {
        let limits = TenantLimits {
            max_attr_value: bytes(2),
            ..TenantLimits::default()
        };
        let span = Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "x".into(),
            kind: crate::span::SpanKind::Internal,
            start_ns: 0,
            duration_ns: 1,
            status: crate::span::StatusCode::Unset,
            status_message: String::new(),
            resource_attrs: vec![KeyValue {
                key: "service.name".into(),
                value: AttrValue::Str("api".into()),
            }],
            span_attrs: Vec::new(),
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        };
        assert2::assert!(validate(&[span], &limits).is_err());
    }

    #[test]
    fn validate_rejects_large_attribute_keys() {
        let limits = TenantLimits {
            max_attr_value: bytes(4),
            ..TenantLimits::default()
        };
        let span = Span {
            trace_id: [1; 16],
            span_id: [2; 8],
            parent_span_id: None,
            name: "x".into(),
            kind: crate::span::SpanKind::Internal,
            start_ns: 0,
            duration_ns: 1,
            status: crate::span::StatusCode::Unset,
            status_message: String::new(),
            resource_attrs: Vec::new(),
            span_attrs: vec![KeyValue {
                key: "too-large".into(),
                value: AttrValue::Bool(true),
            }],
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        };

        assert2::assert!(validate(&[span], &limits).is_err());
    }

    #[test]
    fn validate_rejects_traces_over_span_limit() {
        let limits = TenantLimits {
            max_spans_per_trace: 1,
            ..TenantLimits::default()
        };
        let first = Span {
            trace_id: [1; 16],
            span_id: [1; 8],
            parent_span_id: None,
            name: "root".into(),
            kind: crate::span::SpanKind::Internal,
            start_ns: 0,
            duration_ns: 1,
            status: crate::span::StatusCode::Unset,
            status_message: String::new(),
            resource_attrs: Vec::new(),
            span_attrs: Vec::new(),
            events: Vec::new(),
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        };
        let second = Span {
            span_id: [2; 8],
            ..first.clone()
        };
        let other_trace = Span {
            trace_id: [2; 16],
            ..first.clone()
        };

        assert2::assert!(validate(&[first.clone(), other_trace], &limits).is_ok());
        assert2::assert!(validate(&[first, second], &limits).is_err());
    }

    fn jaeger_binary_batch() -> Vec<u8> {
        const T_STOP: u8 = 0;
        const T_BOOL: u8 = 2;
        const T_I32: u8 = 8;
        const T_I64: u8 = 10;
        const T_BINARY: u8 = 11;
        const T_STRUCT: u8 = 12;
        const T_LIST: u8 = 15;

        fn field(out: &mut Vec<u8>, type_: u8, id: i16) {
            out.push(type_);
            out.extend_from_slice(&id.to_be_bytes());
        }
        fn string(out: &mut Vec<u8>, value: &str) {
            out.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        fn string_field(out: &mut Vec<u8>, id: i16, value: &str) {
            field(out, T_BINARY, id);
            string(out, value);
        }
        fn i32_field(out: &mut Vec<u8>, id: i16, value: i32) {
            field(out, T_I32, id);
            out.extend_from_slice(&value.to_be_bytes());
        }
        fn i64_field(out: &mut Vec<u8>, id: i16, value: i64) {
            field(out, T_I64, id);
            out.extend_from_slice(&value.to_be_bytes());
        }
        fn bool_field(out: &mut Vec<u8>, id: i16, value: bool) {
            field(out, T_BOOL, id);
            out.push(u8::from(value));
        }
        fn key_value_string(out: &mut Vec<u8>, key: &str, value: &str) {
            string_field(out, 1, key);
            i32_field(out, 2, 0);
            string_field(out, 3, value);
            out.push(T_STOP);
        }
        fn key_value_bool(out: &mut Vec<u8>, key: &str, value: bool) {
            string_field(out, 1, key);
            i32_field(out, 2, 3);
            bool_field(out, 5, value);
            out.push(T_STOP);
        }

        let mut out = Vec::new();
        field(&mut out, T_STRUCT, 1);
        string_field(&mut out, 1, "checkout");
        field(&mut out, T_LIST, 2);
        out.push(T_STRUCT);
        out.extend_from_slice(&1_i32.to_be_bytes());
        key_value_string(&mut out, "process.tag", "present");
        out.push(T_STOP);

        field(&mut out, T_LIST, 2);
        out.push(T_STRUCT);
        out.extend_from_slice(&1_i32.to_be_bytes());
        i64_field(&mut out, 1, 2);
        i64_field(&mut out, 2, 1);
        i64_field(&mut out, 3, 3);
        i64_field(&mut out, 4, 0);
        string_field(&mut out, 5, "GET /binary");
        i64_field(&mut out, 8, 1_000);
        i64_field(&mut out, 9, 25);
        field(&mut out, T_LIST, 10);
        out.push(T_STRUCT);
        out.extend_from_slice(&3_i32.to_be_bytes());
        key_value_string(&mut out, "span.kind", "server");
        key_value_string(&mut out, "http.method", "GET");
        key_value_bool(&mut out, "error", true);
        out.push(T_STOP);
        out.push(T_STOP);
        out
    }

    #[test]
    fn oversized_bytes_attr_is_rejected_by_attribute_size_cap() {
        let limits = crate::limits::Limits {
            max_attribute: bytes(4),
            ..crate::limits::Limits::default()
        };
        // A `Bytes` value of 8 bytes exceeds the 4-byte cap. The old stringless
        // path measured it as length 0 and let it through.
        let attrs = vec![KeyValue {
            key: "blob".into(),
            value: AttrValue::Bytes(vec![0u8; 8]),
        }];

        let err = check_shared_attrs(&limits, &attrs).unwrap_err();
        assert2::assert!(matches!(err, TracesError::Limit(_)));

        // A small `Bytes` value within the cap is accepted.
        let small = vec![KeyValue {
            key: "b".into(),
            value: AttrValue::Bytes(vec![0u8; 2]),
        }];
        assert2::assert!(check_shared_attrs(&limits, &small).is_ok());
    }
}

mod append_decoded;
mod append_decoded_response;
mod check_shared_attrs;
mod content_encoding;
mod decode_body;
mod distributor_state;
mod error_response;
mod grpc_status_from_error;
mod handle_jaeger_compact_datagram;
mod is_jaeger_binary_thrift;
mod jaeger_grpc_service;
mod jaeger_push;
mod kafka_sink;
mod limit_error_to_traces_error;
mod otlp_grpc_service;
mod otlp_push;
mod otlp_success_response;
mod produce_spans;
mod record_ingest_response;
mod require_content_type;
mod router;
mod serve;
mod serve_jaeger_compact_udp;
mod serve_jaeger_grpc;
mod serve_otlp_grpc;
mod shared_attr_measured;
mod tenant;
mod tenant_header;
mod tenant_limits;
mod tenant_metadata;
mod u64_limit_from_usize;
mod validate;
mod validate_attrs;
mod validate_shared;
mod wal_sink;
mod zipkin_push;

use append_decoded::append_decoded;
use append_decoded_response::append_decoded_response;
use check_shared_attrs::check_shared_attrs;
use content_encoding::CONTENT_ENCODING;
use decode_body::decode_body;
pub use distributor_state::DistributorState;
use error_response::error_response;
use grpc_status_from_error::grpc_status_from_error;
use handle_jaeger_compact_datagram::handle_jaeger_compact_datagram;
use is_jaeger_binary_thrift::is_jaeger_binary_thrift;
pub use jaeger_grpc_service::JaegerGrpcService;
use jaeger_push::jaeger_push;
pub use kafka_sink::KafkaSink;
use limit_error_to_traces_error::limit_error_to_traces_error;
pub use otlp_grpc_service::OtlpGrpcService;
use otlp_push::otlp_push;
use otlp_success_response::otlp_success_response;
pub use produce_spans::produce_spans;
use record_ingest_response::record_ingest_response;
use require_content_type::require_content_type;
pub use router::router;
pub use serve::serve;
pub use serve_jaeger_compact_udp::serve_jaeger_compact_udp;
pub use serve_jaeger_grpc::serve_jaeger_grpc;
pub use serve_otlp_grpc::serve_otlp_grpc;
use shared_attr_measured::shared_attr_measured;
use tenant::tenant;
use tenant_header::TENANT_HEADER;
pub use tenant_limits::TenantLimits;
use tenant_metadata::tenant_metadata;
use u64_limit_from_usize::u64_limit_from_usize;
pub use validate::validate;
use validate_attrs::validate_attrs;
use validate_shared::validate_shared;
pub use wal_sink::WalSink;
use zipkin_push::zipkin_push;
