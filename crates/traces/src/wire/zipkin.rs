//! Zipkin v2 JSON to internal spans.

use std::collections::BTreeMap;

use serde::Deserialize;

use super::WireError;
use crate::span::{AttrValue, KeyValue, Span, SpanKind, StatusCode, WireSpanStatus};


/// Decode a Zipkin v2 JSON span array.
///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_zipkin(body: &[u8]) -> Result<Vec<Span>, WireError> {
    let raw: Vec<ZipkinSpan> =
        serde_json::from_slice(body).map_err(|err| WireError::Decode(err.to_string()))?;
    let mut out = Vec::with_capacity(raw.len());

    for span in raw {
        let resource_attrs = span
            .local_endpoint
            .and_then(|endpoint| endpoint.service_name)
            .map_or_else(Default::default, |service| {
                vec![KeyValue {
                    key: "service.name".into(),
                    value: AttrValue::Str(service),
                }]
            });
        let status = zipkin_status(&span.tags);
        let mut span_attrs = span
            .tags
            .into_iter()
            .map(|entry| KeyValue {
                key: entry.0,
                value: AttrValue::Str(entry.1),
            })
            .collect::<Vec<_>>();
        if let Some(service) = span
            .remote_endpoint
            .and_then(|endpoint| endpoint.service_name)
        {
            span_attrs.push(KeyValue {
                key: "peer.service".into(),
                value: AttrValue::Str(service),
            });
        }
        let events = span
            .annotations
            .into_iter()
            .map(|annotation| crate::span::EventRecord {
                time_unix_nano: annotation.timestamp.saturating_mul(1_000),
                name: annotation.value,
                attrs: Vec::new(),
            })
            .collect();

        out.push(Span {
            trace_id: hex_fixed::<16>(&span.trace_id)?,
            span_id: hex_fixed::<8>(&span.id)?,
            parent_span_id: span.parent_id.as_deref().map(hex_fixed::<8>).transpose()?,
            name: span.name,
            kind: zipkin_kind(span.kind.as_deref()),
            start_ns: span.timestamp.saturating_mul(1_000),
            duration_ns: span.duration.saturating_mul(1_000),
            status: status.code,
            status_message: status.message,
            resource_attrs,
            span_attrs,
            events,
            links: Vec::new(),
            instrumentation_scope: String::new(),
            instrumentation_version: String::new(),
        });
    }

    Ok(out)
}

#[derive(Deserialize)]
pub(crate) struct ZipkinAnnotation {
    pub(crate) timestamp: i64,
    pub(crate) value: String,
}

#[derive(Deserialize)]
pub(crate) struct ZipkinEndpoint {
    #[serde(rename = "serviceName")]
    pub(crate) service_name: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct ZipkinSpan {
    #[serde(rename = "traceId")]
    pub(crate) trace_id: String,
    pub(crate) id: String,
    #[serde(rename = "parentId")]
    pub(crate) parent_id: Option<String>,
    #[serde(default)]
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) timestamp: i64,
    #[serde(default)]
    pub(crate) duration: i64,
    pub(crate) kind: Option<String>,
    #[serde(rename = "localEndpoint")]
    pub(crate) local_endpoint: Option<ZipkinEndpoint>,
    #[serde(rename = "remoteEndpoint")]
    pub(crate) remote_endpoint: Option<ZipkinEndpoint>,
    #[serde(default)]
    pub(crate) tags: BTreeMap<String, String>,
    #[serde(default)]
    pub(crate) annotations: Vec<ZipkinAnnotation>,
}

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
        let [span] = spans.as_slice() else {
            panic!("expected exactly one decoded span")
        };
        assert2::assert!(span.trace_id == [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1]);
        assert2::assert!(span.span_id == [0, 0, 0, 0, 0, 0, 0, 2]);
        assert2::assert!(span.parent_span_id.is_none());
        assert2::assert!(span.name == "get /");
        assert2::assert!(span.kind == SpanKind::Server);
        assert2::assert!(span.start_ns == 1_000_000 && span.duration_ns == 500_000);
        assert2::assert!(span.status == StatusCode::Unset);
        assert2::assert!(span.status_message.is_empty());
        assert2::assert!(
            span.resource_attrs
                == vec![KeyValue {
                    key: "service.name".into(),
                    value: AttrValue::Str("api".into()),
                }]
        );
        assert2::assert!(
            span.span_attrs
                == vec![KeyValue {
                    key: "http.method".into(),
                    value: AttrValue::Str("GET".into()),
                }]
        );
        assert2::assert!(
            span.events
                == vec![EventRecord {
                    time_unix_nano: 1_100_000,
                    name: "cache miss".into(),
                    attrs: Vec::new(),
                }]
        );
        assert2::assert!(span.links.is_empty());
        assert2::assert!(span.instrumentation_scope.is_empty());
        assert2::assert!(span.instrumentation_version.is_empty());
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



pub(crate) fn hex_fixed<const N: usize>(hex: &str) -> Result<[u8; N], WireError> {
    if hex.is_empty() || !hex.len().is_multiple_of(2) || hex.len() > N * 2 {
        return Err(WireError::Invalid(format!("bad hex id {hex:?}")));
    }

    let bytes = hex::decode(hex).map_err(|err| WireError::Invalid(err.to_string()))?;
    let mut out = [0; N];
    out[N - bytes.len()..].copy_from_slice(&bytes);
    Ok(out)
}

pub(crate) fn zipkin_kind(kind: Option<&str>) -> SpanKind {
    match kind {
        Some("SERVER") => SpanKind::Server,
        Some("CLIENT") => SpanKind::Client,
        Some("PRODUCER") => SpanKind::Producer,
        Some("CONSUMER") => SpanKind::Consumer,
        _ => SpanKind::Internal,
    }
}

pub(crate) fn zipkin_status(tags: &BTreeMap<String, String>) -> WireSpanStatus {
    match tags.get("error") {
        Some(value) => {
            let message = if value == "true" || value == "false" {
                String::new()
            } else {
                value.clone()
            };
            WireSpanStatus {
                code: StatusCode::Error,
                message,
            }
        }
        None => WireSpanStatus {
            code: StatusCode::Unset,
            message: String::new(),
        },
    }
}
