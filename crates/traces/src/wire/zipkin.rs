//! Zipkin v2 JSON to internal spans.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::WireError;
use crate::span::{AttrValue, KeyValue, Span, SpanKind, StatusCode};

#[cfg(test)]
mod tests {

    /// `zipkin_kind` names four kinds and falls back to internal for
    /// everything else, so "INTERNAL" and an unknown string reach the same
    /// answer by different routes. Both routes are pinned, and every named
    /// kind is checked so none can quietly fall through to the default.
    #[test]
    fn zipkin_kind_falls_back_to_internal_only_when_unrecognised() {
        use assert2::check;

        use crate::span::SpanKind;
        let kind = super::zipkin_kind;

        check!(kind(Some("SERVER")) == SpanKind::Server);
        check!(kind(Some("CLIENT")) == SpanKind::Client);
        check!(kind(Some("PRODUCER")) == SpanKind::Producer);
        check!(kind(Some("CONSUMER")) == SpanKind::Consumer);

        // Both ways to reach internal.
        check!(
            kind(Some("INTERNAL")) == SpanKind::Internal,
            "by falling through"
        );
        check!(kind(None) == SpanKind::Internal, "and by being absent");
        check!(kind(Some("")) == SpanKind::Internal);
        check!(kind(Some("nonsense")) == SpanKind::Internal);

        // The match is case-sensitive, so a lower-case spelling defaults.
        check!(kind(Some("server")) == SpanKind::Internal, "case-sensitive");
    }

    use super::*;
    use crate::span::EventRecord;

    const BODY: &str = r#"[
      {
        "traceId": "0000000000000001",
        "id": "0000000000000002",
        "name": "get /",
        "timestamp": 1000,
        "duration": 500,
        "kind": "SERVER",
        "localEndpoint": { "serviceName": "api" },
        "tags": { "http.method": "GET" },
        "annotations": [{ "timestamp": 1100, "value": "cache miss" }]
      }
    ]"#;

    #[test]
    fn decodes_zipkin_span() {
        let spans = decode_zipkin(BODY.as_bytes()).unwrap();
        assert2::assert!(
            spans
                == vec![Span {
                    trace_id: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
                    span_id: [0, 0, 0, 0, 0, 0, 0, 2],
                    parent_span_id: None,
                    name: "get /".into(),
                    kind: SpanKind::Server,
                    start_ns: 1_000_000,
                    duration_ns: 500_000,
                    status: StatusCode::Unset,
                    status_message: String::new(),
                    resource_attrs: vec![KeyValue {
                        key: "service.name".into(),
                        value: AttrValue::Str("api".into()),
                    }],
                    span_attrs: vec![KeyValue {
                        key: "http.method".into(),
                        value: AttrValue::Str("GET".into()),
                    }],
                    events: vec![EventRecord {
                        time_unix_nano: 1_100_000,
                        name: "cache miss".into(),
                        attrs: Vec::new(),
                    }],
                    links: Vec::new(),
                    instrumentation_scope: String::new(),
                    instrumentation_version: String::new(),
                }]
        );
    }

    #[test]
    fn error_tag_sets_status_error() {
        let body = r#"[
          {
            "traceId": "0000000000000001",
            "id": "0000000000000002",
            "tags": { "error": "true" }
          }
        ]"#;

        let spans = decode_zipkin(body.as_bytes()).unwrap();

        assert2::assert!(
            (
                spans[0].status,
                spans[0]
                    .span_attrs
                    .iter()
                    .map(|attr| (attr.key.as_str(), &attr.value))
                    .collect::<Vec<_>>(),
            ) == (
                StatusCode::Error,
                vec![("error", &AttrValue::Str("true".into()))]
            )
        );
    }

    #[test]
    fn error_tag_description_sets_status_message() {
        let body = r#"[
          {
            "traceId": "0000000000000001",
            "id": "0000000000000002",
            "tags": { "error": "deadline exceeded" }
          }
        ]"#;

        let spans = decode_zipkin(body.as_bytes()).unwrap();

        assert2::assert!(
            (spans[0].status, spans[0].status_message.as_str())
                == (StatusCode::Error, "deadline exceeded")
        );
    }

    #[test]
    fn remote_endpoint_service_name_becomes_peer_service_attribute() {
        let body = r#"[
          {
            "traceId": "0000000000000001",
            "id": "0000000000000002",
            "remoteEndpoint": { "serviceName": "postgres" }
          }
        ]"#;

        let spans = decode_zipkin(body.as_bytes()).unwrap();

        assert2::assert!(
            spans[0]
                .span_attrs
                .iter()
                .any(|attr| attr.key == "peer.service"
                    && attr.value == AttrValue::Str("postgres".into()))
        );
    }

    #[test]
    fn rejects_odd_length_hex_id() {
        let bad = r#"[{ "traceId": "xyz", "id": "0000000000000002", "name": "x" }]"#;
        assert2::assert!(decode_zipkin(bad.as_bytes()).is_err());
    }
}

// === split-modules: generated submodules ===
mod decode_zipkin;
mod hex_fixed;
mod zipkin_annotation;
mod zipkin_endpoint;
mod zipkin_kind;
mod zipkin_span;
mod zipkin_status;

pub use decode_zipkin::decode_zipkin;
use hex_fixed::hex_fixed;
use zipkin_annotation::ZipkinAnnotation;
use zipkin_endpoint::ZipkinEndpoint;
use zipkin_kind::zipkin_kind;
use zipkin_span::ZipkinSpan;
use zipkin_status::zipkin_status;
