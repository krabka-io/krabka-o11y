//! The Tempo HTTP-API JSON edge model the query-frontend renders and parses.
//!
//! This is the same body shape the querier emits from Slice 5, `querier/http`.
//! The frontend parses per-job partials, merges them under `limit` and `spss`,
//! accumulates the `metrics{}` job-accounting block, and re-emits this exact
//! shape. The trace values it carries, `TraceResult`, `SpanSet` and `SpanRef`,
//! are the pinned `krabka-traceql` result types from Slice 2. This module is
//! their HTTP projection.
//!
//! Note: the `krabka-traceql` result types do **not** derive serde. The search
//! edge model is therefore a standalone serde mirror with lossless `From` and
//! reverse-`From` projections. The by-id edge model is a minimal typed
//! OTLP-JSON mirror, `TraceByIdResponseJson`, shaped to the querier's v2 body.

use krabka_traceql::{AttrValue, SpanRef, SpanSet, TraceResult};
use krabka_units::{
    ByteSize, Time,
    convert::{ByteSizeExt, TimeExt as _},
};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Trace-by-id v2 edge model.
//
// A minimal typed OTLP-JSON mirror of the querier's `/api/v2/traces/{id}` body:
// `{ trace: { resourceSpans: [...] }, status, message }`. Just enough nested
// structure (resourceSpans -> scopeSpans -> spans with spanId) to union
// resourceSpans across queriers, dedupe spans by spanId, and size-estimate the
// assembled trace. We carry the rest of each span as an opaque
// `serde_json::Value` so we round-trip the querier's exact span JSON (kind /
// status / events / links / nanos) without re-stating its full shape.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use krabka_units::{millis, nanos};

    use super::*;

    #[test]
    fn search_response_serializes_as_tempo_json() {
        let resp = SearchResponseJson {
            traces: vec![TraceJson {
                trace_id: "0a".repeat(16),
                root_service_name: "checkout".to_string(),
                root_trace_name: "POST /pay".to_string(),
                start_time_unix_nano: "1700000000000000000".to_string(),
                duration: millis(42),
                span_sets: vec![SpanSetJson {
                    spans: vec![SpanJson {
                        span_id: "0b".repeat(8),
                        start_time_unix_nano: "1700000000000000000".to_string(),
                        duration_nanos: "42000000".to_string(),
                        attributes: vec![],
                    }],
                    matched: 1,
                }],
            }],
            metrics: Metrics {
                total_jobs: 3,
                completed_jobs: 3,
                total_blocks: 2,
                inspected_traces: 10,
                inspected_bytes: 4096,
                inspected_spans: 50,
            },
        };
        let json = serde_json::to_value(&resp).unwrap();
        assert2::assert!(
            json == serde_json::json!({
                "traces": [{
                    "traceID": "0a".repeat(16),
                    "rootServiceName": "checkout",
                    "rootTraceName": "POST /pay",
                    "startTimeUnixNano": "1700000000000000000",
                    "durationMs": 42,
                    "spanSets": [{
                        "spans": [{
                            "spanID": "0b".repeat(8),
                            "startTimeUnixNano": "1700000000000000000",
                            "durationNanos": "42000000",
                            "attributes": []
                        }],
                        "matched": 1
                    }]
                }],
                "metrics": {
                    "totalJobs": 3,
                    "completedJobs": 3,
                    "totalBlocks": 2,
                    "inspectedTraces": 10,
                    "inspectedBytes": 4096,
                    "inspectedSpans": 50
                }
            })
        );
    }

    #[test]
    fn search_response_parses_querier_body() {
        // The shape the querier's `search_json` emits, with string-encoded
        // metrics counters.
        let body = serde_json::json!({
            "traces": [{
                "traceID": "ab".repeat(16),
                "rootServiceName": "svc",
                "rootTraceName": "GET /",
                "startTimeUnixNano": "5",
                "durationMs": 12,
                "spanSets": [{
                    "spans": [{
                        "spanID": "cd".repeat(8),
                        "startTimeUnixNano": "5",
                        "durationNanos": "1000",
                        "attributes": [
                            { "key": "http.method", "value": { "stringValue": "GET" } },
                            { "key": "http.status", "value": { "intValue": "200" } }
                        ]
                    }],
                    "matched": 3
                }]
            }],
            "metrics": { "totalBlocks": "2", "inspectedTraces": "3", "inspectedBytes": "5" }
        });
        let resp: SearchResponseJson = serde_json::from_value(body).unwrap();
        assert2::assert!(
            resp == SearchResponseJson {
                traces: vec![TraceJson {
                    trace_id: "ab".repeat(16),
                    root_service_name: "svc".to_string(),
                    root_trace_name: "GET /".to_string(),
                    start_time_unix_nano: "5".to_string(),
                    duration: millis(12),
                    span_sets: vec![SpanSetJson {
                        spans: vec![SpanJson {
                            span_id: "cd".repeat(8),
                            start_time_unix_nano: "5".to_string(),
                            duration_nanos: "1000".to_string(),
                            attributes: vec![
                                KeyValueJson {
                                    key: "http.method".to_string(),
                                    value: AnyValueJson::StringValue("GET".to_string()),
                                },
                                KeyValueJson {
                                    key: "http.status".to_string(),
                                    value: AnyValueJson::IntValue("200".to_string()),
                                },
                            ],
                        }],
                        matched: 3,
                    }],
                }],
                metrics: Metrics {
                    total_jobs: 0,
                    completed_jobs: 0,
                    total_blocks: 2,
                    inspected_traces: 3,
                    inspected_bytes: 5,
                    inspected_spans: 0,
                },
            }
        );
    }

    #[test]
    fn metrics_add_is_additive() {
        let mut a = Metrics::default();
        a.add(&Metrics {
            total_jobs: 1,
            completed_jobs: 1,
            total_blocks: 1,
            inspected_traces: 2,
            inspected_bytes: 100,
            inspected_spans: 9,
        });
        a.add(&Metrics {
            total_jobs: 1,
            completed_jobs: 1,
            total_blocks: 1,
            inspected_traces: 3,
            inspected_bytes: 200,
            inspected_spans: 11,
        });
        assert2::assert!(
            a == Metrics {
                total_jobs: 2,
                completed_jobs: 2,
                total_blocks: 2,
                inspected_traces: 5,
                inspected_bytes: 300,
                inspected_spans: 20,
            }
        );
    }

    #[test]
    fn hex_encodes_lowercase() {
        assert2::assert!(hex16(&[0xab; 16]) == "ab".repeat(16));
        assert2::assert!(hex8(&[0x0f; 8]) == "0f".repeat(8));
    }

    #[test]
    fn trace_result_round_trips_through_json_projection() {
        use krabka_traceql::{AttrValue, SpanRef, SpanSet, TraceResult};

        let span = SpanRef {
            span_id: [7; 8],
            parent_span_id: None,
            name: "op".into(),
            kind: 0,
            nested_set_left: 0,
            nested_set_right: 0,
            nested_set_parent: 0,
            start_time_unix_nano: 1234,
            duration: nanos(56),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            resource_attributes: Vec::new(),
            attributes: vec![("k".into(), AttrValue::Int(9))],
            events: Vec::new(),
            links: Vec::new(),
        };
        let trace = TraceResult {
            trace_id: [3; 16],
            root_service_name: "svc".into(),
            root_trace_name: "GET /".into(),
            start_time_unix_nano: 1234,
            duration: millis(5),
            span_sets: vec![SpanSet {
                spans: vec![span],
                matched: 1,
            }],
        };
        let json = TraceJson::from(&trace);
        let back = TraceResult::from(&json);
        assert2::assert!(
            back == TraceResult {
                trace_id: [3; 16],
                root_service_name: "svc".into(),
                root_trace_name: "GET /".into(),
                start_time_unix_nano: 1234,
                duration: millis(5),
                span_sets: vec![SpanSet {
                    spans: vec![SpanRef {
                        span_id: [7; 8],
                        parent_span_id: None,
                        name: String::new(),
                        kind: 0,
                        nested_set_left: 0,
                        nested_set_right: 0,
                        nested_set_parent: 0,
                        start_time_unix_nano: 1234,
                        duration: nanos(56),
                        status_code: 0,
                        status_message: String::new(),
                        instrumentation_name: String::new(),
                        instrumentation_version: String::new(),
                        resource_attributes: Vec::new(),
                        attributes: vec![("k".into(), AttrValue::Int(9))],
                        events: Vec::new(),
                        links: Vec::new(),
                    }],
                    matched: 1,
                }],
            }
        );
    }
}

// === split-modules: generated submodules ===
mod any_value_json;
mod array_value_json;
mod attr_value;
mod de_u64_lenient;
mod hex16;
mod hex8;
mod key_value_json;
mod metrics;
mod otlp_span_json;
mod parse_hex16;
mod parse_hex8;
mod resource_spans_json;
mod scope_spans_json;
mod search_response_json;
mod span_json;
mod span_ref;
mod span_set;
mod span_set_json;
mod trace_by_id_response_json;
mod trace_envelope_json;
mod trace_json;
mod trace_result;

pub use any_value_json::AnyValueJson;
pub use array_value_json::ArrayValueJson;
use de_u64_lenient::de_u64_lenient;
pub use hex8::hex8;
pub use hex16::hex16;
pub use key_value_json::KeyValueJson;
pub use metrics::Metrics;
pub use otlp_span_json::OtlpSpanJson;
pub use parse_hex8::parse_hex8;
pub use parse_hex16::parse_hex16;
pub use resource_spans_json::ResourceSpansJson;
pub use scope_spans_json::ScopeSpansJson;
pub use search_response_json::SearchResponseJson;
pub use span_json::SpanJson;
pub use span_set_json::SpanSetJson;
pub use trace_by_id_response_json::TraceByIdResponseJson;
pub use trace_envelope_json::TraceEnvelopeJson;
pub use trace_json::TraceJson;
