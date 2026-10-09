use serde::Serialize;

use super::{search_span_json::search_attrs_json, *};
use crate::frontend::wire::{SpanSetJson, TraceJson};

#[derive(Serialize)]
struct SearchJson {
    traces: Vec<TraceJson>,
    metrics: SearchMetricsJson,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct SearchMetricsJson {
    completed_jobs: u64,
    total_blocks: u64,
    inspected_traces: usize,
    inspected_spans: usize,
    inspected_bytes: u64,
}

pub(crate) fn search_json(resp: SearchResponse) -> impl Serialize {
    let inspected_traces = resp.inspected_traces;
    let inspected_bytes = resp.inspected.bytes_u64();
    // Spans this response scanned/returned: the distinct matched spans across
    // every returned trace's spanSets. The frontend folds this per-job sum into
    // the merged `metrics.inspectedSpans`.
    let inspected_spans: usize = resp
        .traces
        .iter()
        .flat_map(|trace| trace.span_sets.iter())
        .map(|set| set.spans.len())
        .sum();
    SearchJson {
        traces: resp
            .traces
            .into_iter()
            .map(|trace| {
                TraceJson {
                    trace_id: hex::encode(trace.trace_id),
                    root_service_name: trace.root_service_name,
                    root_trace_name: trace.root_trace_name,
                    start_time_unix_nano: trace.start_time_unix_nano.to_string(),
                    // Truncated, not rounded: Tempo integer-divides its nanosecond duration,
                    // and the frontend merges this querier JSON into the public search
                    // response, so a rounded value would surface there too.
                    duration: trace.duration,
                    span_sets: trace
                        .span_sets
                        .into_iter()
                        .map(|set| SpanSetJson {
                            spans: set.spans.iter().map(search_span_json).collect(),
                            matched: set.matched,
                            attributes: search_attrs_json(&set.attributes),
                        })
                        .collect::<Vec<_>>(),
                }
            })
            .collect::<Vec<_>>(),
        // Per-response job accounting the frontend folds (`metrics.add`): this
        // search ran as one completed job. `inspectedBytes` is the decoded size of
        // the cold+live data the scan inspected (threaded up from the SpanStore).
        metrics: SearchMetricsJson {
            completed_jobs: 1,
            total_blocks: 0,
            inspected_traces,
            inspected_spans,
            inspected_bytes,
        },
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use axum::{Json, response::IntoResponse};
    use http_body_util::BodyExt as _;
    use krabka_traceql::{AttrValue, SearchResponse, SpanRef, SpanSet, TraceResult};
    use krabka_units::{Time, bytes, convert::TimeExt as _};
    use serde_json::{Value, json};

    use super::search_json;

    fn span(attributes: Vec<(String, AttrValue)>) -> SpanRef {
        SpanRef {
            span_id: [1; 8],
            parent_span_id: None,
            name: "not a search field".to_owned(),
            kind: 0,
            nested_set_left: 1,
            nested_set_right: 2,
            nested_set_parent: -1,
            start_time_unix_nano: u64::MAX,
            duration: Time::from_nanos(i64::MAX),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: String::new(),
            instrumentation_version: String::new(),
            resource_attributes: vec![("not returned".to_owned(), AttrValue::Bool(true))],
            attributes,
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    fn response() -> SearchResponse {
        SearchResponse {
            traces: vec![TraceResult {
                trace_id: [2; 16],
                root_service_name: "svc\"\\\n".to_owned(),
                root_trace_name: "root-λ".to_owned(),
                start_time_unix_nano: u64::MAX,
                duration: Time::from_nanos(1_999_999),
                span_sets: vec![
                    SpanSet {
                        spans: vec![span(vec![
                            ("z".to_owned(), AttrValue::Int(i64::MIN)),
                            ("a".to_owned(), AttrValue::Bool(false)),
                            ("z".to_owned(), AttrValue::Str("second".to_owned())),
                            (
                                "nested".to_owned(),
                                AttrValue::Array(vec![
                                    AttrValue::Float(f64::NAN),
                                    AttrValue::Float(f64::INFINITY),
                                    AttrValue::Float(f64::NEG_INFINITY),
                                    AttrValue::Array(vec![AttrValue::Float(1.25)]),
                                ]),
                            ),
                            (
                                "opaque".to_owned(),
                                AttrValue::Unsupported(
                                    r#"{"kvlistValue":{"values":[{"key":"x","value":{}}]},"future":true}"#.to_owned(),
                                ),
                            ),
                        ])],
                        matched: u32::MAX,
                        attributes: vec![
                            ("z".to_owned(), AttrValue::Int(7)),
                            ("a".to_owned(), AttrValue::Array(Vec::new())),
                            ("z".to_owned(), AttrValue::Int(8)),
                        ],
                    },
                    SpanSet {
                        spans: vec![span(Vec::new())],
                        matched: 0,
                        attributes: Vec::new(),
                    },
                ],
            }],
            inspected_traces: 17,
            inspected: bytes(1234),
        }
    }

    #[tokio::test]
    async fn search_http_body_preserves_grouped_attributes_and_exact_wire_fields() {
        let response = Json(search_json(response())).into_response();
        assert!(response.status() == axum::http::StatusCode::OK);
        assert!(response.headers()[axum::http::header::CONTENT_TYPE] == "application/json");
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let actual: Value = serde_json::from_slice(&body).unwrap();
        assert!(
            actual
                == json!({
                    "traces": [{
                        "traceID": "02".repeat(16),
                        "rootServiceName": "svc\"\\\n",
                        "rootTraceName": "root-λ",
                        "startTimeUnixNano": "18446744073709551615",
                        "durationMs": 1,
                        "spanSets": [{
                            "spans": [{
                                "spanID": "01".repeat(8),
                                "startTimeUnixNano": "18446744073709551615",
                                "durationNanos": "9223372036854775807",
                                "attributes": [
                                    {"key":"z","value":{"arrayValue":{"values":[
                                        {"intValue":"-9223372036854775808"}, {"stringValue":"second"}
                                    ]}}},
                                    {"key":"a","value":{"boolValue":false}},
                                    {"key":"nested","value":{"arrayValue":{"values":[
                                        {"doubleValue":"NaN"}, {"doubleValue":"Infinity"},
                                        {"doubleValue":"-Infinity"},
                                        {"arrayValue":{"values":[{"doubleValue":1.25}]}}
                                    ]}}},
                                    {"key":"opaque","value":{
                                        "kvlistValue":{"values":[{"key":"x","value":{}}]},"future":true
                                    }}
                                ]
                            }],
                            "matched": 4_294_967_295_u64,
                            "attributes": [
                                {"key":"z","value":{"arrayValue":{"values":[
                                    {"intValue":"7"}, {"intValue":"8"}
                                ]}}},
                                {"key":"a","value":{"arrayValue":{"values":[]}}}
                            ]
                        }, {
                            "spans": [{
                                "spanID": "01".repeat(8),
                                "startTimeUnixNano": "18446744073709551615",
                                "durationNanos": "9223372036854775807",
                                "attributes": []
                            }],
                            "matched": 0
                        }]
                    }],
                    "metrics": {
                        "completedJobs": 1, "totalBlocks": 0, "inspectedTraces": 17,
                        "inspectedSpans": 2, "inspectedBytes": 1234
                    }
                })
        );
    }

    #[test]
    fn empty_search_keeps_job_metrics_and_an_empty_trace_array() {
        let actual = serde_json::to_value(search_json(SearchResponse {
            traces: Vec::new(),
            inspected_traces: 0,
            inspected: bytes(0),
        }))
        .unwrap();
        assert!(
            actual
                == json!({
                    "traces": [],
                    "metrics": {
                        "completedJobs": 1, "totalBlocks": 0, "inspectedTraces": 0,
                        "inspectedSpans": 0, "inspectedBytes": 0
                    }
                })
        );
    }

    #[test]
    fn search_trace_milliseconds_truncate_while_span_nanos_stay_strings() {
        for (duration, expected_ms) in [(-1_999_999, -1), (999_999, 0), (1_000_000, 1)] {
            let mut response = response();
            response.traces[0].duration = Time::from_nanos(duration);
            response.traces[0].span_sets[0].spans[0].duration = Time::from_nanos(duration);
            let actual = serde_json::to_value(search_json(response)).unwrap();
            assert!(actual["traces"][0]["durationMs"] == json!(expected_ms));
            assert!(
                actual["traces"][0]["spanSets"][0]["spans"][0]["durationNanos"]
                    == json!(duration.to_string())
            );
        }
    }
}
