//! The Loki push endpoint, and the WAL records it writes.

mod support;

use std::{collections::BTreeMap, io::Write as _};

use assert2::assert;
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use flate2::{
    Compression,
    write::{DeflateEncoder, GzEncoder},
};
use krabka_blockstore::{LogLabels, labels};
use krabka_observability::{
    InMemoryWalSink, LogWalSink, Role, ServiceDependencies, WalLogRecord, build_service_router,
    distributor_router,
};
use prost::Message as _;
use serde_json::{Value, json};
use snap::raw::Encoder as SnappyEncoder;
use support::{
    BodyHeaders, ExpectedLokiError, FailingWalSink, JsonStream, LokiProtoEntry, LokiProtoLabelPair,
    LokiProtoPushRequest, LokiProtoStream, LokiProtoTimestamp, PartialWalSink, PushOutcome,
    assert_loki_error, minimal_service_config, push_request, send, text_body,
};

const PUSH: &str = "/loki/api/v1/push";
const JSON_TYPE: BodyHeaders = BodyHeaders {
    content_type: "application/json",
    content_encoding: None,
};
const PROTOBUF_TYPE: BodyHeaders = BodyHeaders {
    content_type: "application/x-protobuf",
    content_encoding: None,
};
const NO_VALID_STREAM: &str = "error at least one valid stream is required for ingestion\n";
const UNKNOWN_VALUE_TYPE: &str =
    "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Unknown value type";
const LOOKS_LIKE_OBJECT: &str = "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like object";
const BAD_LABEL_PARSE: &str =
    "couldn't parse labels: 1:5: parse error: unexpected character inside braces: '-'\n";

async fn push(app: &Router, request: Request<Body>) -> (StatusCode, String) {
    let response = send(app, request).await;
    (response.status(), text_body(response).await)
}

async fn push_request_to_sink(request: Request<Body>) -> PushOutcome {
    let sink = InMemoryWalSink::default();
    let (status, body) = push(&distributor_router(sink.clone()), request).await;
    PushOutcome {
        status,
        body,
        records: sink.records(),
    }
}

async fn push_to_sink(headers: BodyHeaders<'_>, body: impl Into<Body>) -> PushOutcome {
    push_request_to_sink(push_request(PUSH, headers, body)).await
}

async fn push_json(payload: &Value) -> PushOutcome {
    push_to_sink(JSON_TYPE, payload.to_string()).await
}

async fn push_json_to(sink: impl LogWalSink, payload: &Value) -> (StatusCode, String) {
    push(
        &distributor_router(sink),
        push_request(PUSH, JSON_TYPE, payload.to_string()),
    )
    .await
}

async fn push_proto(labels: &str, entry: LokiProtoEntry) -> PushOutcome {
    push_to_sink(PROTOBUF_TYPE, snappy_push(labels, entry)).await
}

fn api_stream(values: Value) -> Value {
    JsonStream {
        stream: json!({ "app": "api" }),
        values,
    }
    .payload()
}

fn api_prod_stream(values: Value) -> Value {
    JsonStream {
        stream: json!({ "app": "api", "env": "prod" }),
        values,
    }
    .payload()
}

/// A tenant-a WAL record of `line` at `timestamp_ns`, with no structured
/// metadata yet.
fn record(stream_labels: LogLabels, timestamp_ns: i64, line: &str) -> WalLogRecord {
    WalLogRecord {
        tenant: "tenant-a".to_string(),
        labels: stream_labels,
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    }
}

/// Sets a record's structured metadata.
trait WithMetadata {
    fn with_metadata(self, metadata: LogLabels) -> Self;
}

impl WithMetadata for WalLogRecord {
    fn with_metadata(mut self, metadata: LogLabels) -> Self {
        self.structured_metadata = metadata;
        self
    }
}

const API: [(&str, &str); 2] = [("app", "api"), ("service_name", "api")];
const API_PROD: [(&str, &str); 3] = [("app", "api"), ("env", "prod"), ("service_name", "api")];
const UNKNOWN_SERVICE: [(&str, &str); 1] = [("service_name", "unknown_service")];

/// A protobuf `api error` entry at `timestamp`, with no structured metadata
/// yet.
fn proto_entry(timestamp: LokiProtoTimestamp) -> LokiProtoEntry {
    LokiProtoEntry {
        timestamp: Some(timestamp),
        line: "api error".to_string(),
        structured_metadata: vec![],
        parsed: vec![],
    }
}

/// Appends one structured metadata pair to a protobuf entry, keeping the
/// order and any repeated name.
trait ProtoMetadata {
    fn metadata(self, metadata_name: &str, metadata_value: &str) -> Self;
}

impl ProtoMetadata for LokiProtoEntry {
    fn metadata(mut self, metadata_name: &str, metadata_value: &str) -> Self {
        self.structured_metadata.push(LokiProtoLabelPair {
            name: metadata_name.to_string(),
            value: metadata_value.to_string(),
        });
        self
    }
}

fn snappy(request: &LokiProtoPushRequest) -> Vec<u8> {
    SnappyEncoder::new()
        .compress_vec(&request.encode_to_vec())
        .unwrap()
}

fn snappy_push(labels: &str, entry: LokiProtoEntry) -> Vec<u8> {
    snappy(&LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: labels.to_string(),
            entries: vec![entry],
            hash: 0,
        }],
    })
}

#[tokio::test]
async fn loki_push_endpoint_writes_tenant_scoped_wal_records() {
    let outcome = push_json(&api_prod_stream(json!([
        ["19", "api error", {"trace_id": "abc", "status": "500"}],
        ["20", "api ok"]
    ])))
    .await;

    assert!(
        outcome.accepted()
            == [
                record(labels(API_PROD), 19, "api error").with_metadata(labels([
                    ("detected_level", "error"),
                    ("status", "500"),
                    ("trace_id", "abc"),
                ])),
                record(labels(API_PROD), 20, "api ok")
                    .with_metadata(labels([("detected_level", "unknown")])),
            ]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_incomplete_json_value_as_empty_line() {
    let outcome = push_json(&api_prod_stream(json!([["19"]]))).await;

    assert!(
        outcome.accepted()
            == [record(labels(API_PROD), 19, "")
                .with_metadata(labels([("detected_level", "unknown")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_ignores_extra_json_value_fields_like_loki() {
    let outcome = push_json(&api_stream(json!([
        ["19", "api error", {"trace_id": "abc"}, "extra"]
    ])))
    .await;

    assert!(
        outcome.accepted()
            == [record(labels(API), 19, "api error")
                .with_metadata(labels([("detected_level", "error"), ("trace_id", "abc")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_decodes_empty_json_value_as_zero_timestamp_empty_line_like_loki() {
    let outcome = push_json(&api_stream(json!([[]]))).await;

    assert!(
        outcome.accepted()
            == [record(labels(API), 0, "").with_metadata(labels([("detected_level", "unknown")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_array_json_value_like_loki() {
    push_json(&api_stream(json!([["19", "api ok"], "not-a-push-value"])))
        .await
        .rejected_containing(&[UNKNOWN_VALUE_TYPE, "not-a-push-value"]);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_object_json_stream_like_loki() {
    push_json(&json!({ "streams": ["not-a-stream"] }))
        .await
        .rejected_containing(&[LOOKS_LIKE_OBJECT, "not-a-stream"]);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_array_json_streams_like_loki() {
    push_json(&json!({ "streams": "not-streams" }))
        .await
        .rejected_containing(&[
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: decode slice: expect [ or n, but found",
            "not-streams",
        ]);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_array_json_payload_like_loki() {
    push_to_sink(JSON_TYPE, r#"[{"streams": []}]"#)
        .await
        .rejected_containing(&[
            "readObjectStart: expect { or n, but found [",
            r#"[{"streams""#,
        ]);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_null_json_payload_like_loki() {
    push_to_sink(JSON_TYPE, "null")
        .await
        .rejected_with(StatusCode::UNPROCESSABLE_ENTITY, NO_VALID_STREAM);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_missing_json_streams_like_loki() {
    push_json(&json!({}))
        .await
        .rejected_with(StatusCode::UNPROCESSABLE_ENTITY, NO_VALID_STREAM);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_empty_json_streams_like_loki() {
    push_json(&json!({ "streams": [] }))
        .await
        .rejected_with(StatusCode::UNPROCESSABLE_ENTITY, NO_VALID_STREAM);
}

#[tokio::test]
async fn loki_push_endpoint_accepts_missing_json_values_like_loki() {
    push_json(&json!({ "streams": [{ "stream": { "app": "api" } }] }))
        .await
        .accepted_without_records();
}

#[tokio::test]
async fn loki_push_endpoint_accepts_null_json_values_like_loki() {
    push_json(&api_stream(Value::Null))
        .await
        .accepted_without_records();
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_array_json_values_like_loki() {
    push_json(&api_stream(json!("not-values")))
        .await
        .rejected_containing(&[UNKNOWN_VALUE_TYPE, "not-values"]);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_object_json_labels_like_loki() {
    push_json(
        &JsonStream {
            stream: json!("not-labels"),
            values: json!([["19", "labels field is not an object"]]),
        }
        .payload(),
    )
    .await
    .rejected_containing(&[LOOKS_LIKE_OBJECT, "labels field is not an object"]);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_null_json_labels_like_loki() {
    push_json(
        &JsonStream {
            stream: Value::Null,
            values: json!([["19", "null labels field"]]),
        }
        .payload(),
    )
    .await
    .rejected_containing(&[LOOKS_LIKE_OBJECT, "null labels field"]);
}

#[tokio::test]
async fn loki_push_endpoint_accepts_missing_json_labels_like_loki() {
    push_json(&json!({ "streams": [{ "values": [["19", "missing labels field"]] }] }))
        .await
        .accepted_without_records();
}

#[tokio::test]
async fn loki_push_endpoint_returns_server_error_when_wal_append_fails() {
    let (status, body) = push_json_to(
        FailingWalSink,
        &api_prod_stream(json!([["19", "api error"]])),
    )
    .await;

    assert!(status == StatusCode::SERVICE_UNAVAILABLE);
    assert_loki_error(
        &serde_json::from_str(&body).unwrap(),
        "server_error",
        "wal sink",
    );
}

/// A push whose entries appended in part must not reach the client as a
/// success. Loki has no way to tell Promtail or Alloy that half a push landed,
/// so both retry the whole push on a 5xx, and both read a 2xx as "every entry
/// is durable". The status stays 503 and the error names how far the batch
/// got, because the logs read path has no query-time deduplication: the retry
/// writes the entries that already landed a second time, permanently.
#[tokio::test]
async fn loki_push_endpoint_reports_a_partial_wal_batch_as_a_failure() {
    let (status, body) = push_json_to(
        PartialWalSink::new(2),
        &api_prod_stream(json!([
            ["19", "one"],
            ["20", "two"],
            ["21", "three"],
            ["22", "four"],
            ["23", "five"]
        ])),
    )
    .await;

    assert!(status == StatusCode::SERVICE_UNAVAILABLE);
    assert_loki_error(
        &serde_json::from_str(&body).unwrap(),
        "server_error",
        "wal append wrote 2 of 5 records",
    );
}

fn traced_api_prod_payload() -> String {
    api_prod_stream(json!([["19", "api error", {"trace_id": "abc"}]])).to_string()
}

fn traced_api_prod_record() -> WalLogRecord {
    record(labels(API_PROD), 19, "api error")
        .with_metadata(labels([("detected_level", "error"), ("trace_id", "abc")]))
}

async fn push_encoded(encoding: &str, payload: impl Into<Body>) -> PushOutcome {
    push_to_sink(
        BodyHeaders {
            content_type: "application/json",
            content_encoding: Some(encoding),
        },
        payload,
    )
    .await
}

#[tokio::test]
async fn loki_push_endpoint_accepts_gzipped_json_payloads() {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(traced_api_prod_payload().as_bytes())
        .unwrap();

    let outcome = push_encoded("gzip", encoder.finish().unwrap()).await;

    assert!(outcome.accepted() == [traced_api_prod_record()]);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_malformed_gzip_payload_without_wal_append() {
    push_encoded("gzip", "not gzip")
        .await
        .rejected_with(StatusCode::BAD_REQUEST, "unexpected EOF\n");
}

#[tokio::test]
async fn loki_push_endpoint_accepts_deflated_json_payloads() {
    let mut encoder = DeflateEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(traced_api_prod_payload().as_bytes())
        .unwrap();

    let outcome = push_encoded("deflate", encoder.finish().unwrap()).await;

    assert!(outcome.accepted() == [traced_api_prod_record()]);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_malformed_deflate_payload_without_wal_append() {
    push_encoded("deflate", "not deflate")
        .await
        .rejected_with(StatusCode::BAD_REQUEST, "EOF\n");
}

fn api_error_payload() -> String {
    api_stream(json!([["19", "api error"]])).to_string()
}

#[tokio::test]
async fn loki_push_endpoint_rejects_unsupported_content_encoding_without_wal_append() {
    push_encoded("br", api_error_payload()).await.rejected_with(
        StatusCode::BAD_REQUEST,
        "Content-Encoding \"br\" not supported\n",
    );
}

#[tokio::test]
async fn loki_push_endpoint_treats_non_json_content_type_as_snappy_protobuf() {
    // Real Loki 3.4.2 returns a plain-text body (`Content-Type: text/plain`) for a
    // failed snappy-protobuf decode on a non-JSON push, not a JSON error envelope.
    push_to_sink(
        BodyHeaders {
            content_type: "text/plain",
            content_encoding: None,
        },
        api_error_payload(),
    )
    .await
    .rejected_with(StatusCode::BAD_REQUEST, "snappy: corrupt input\n");
}

#[tokio::test]
async fn loki_push_endpoint_rejects_malformed_content_type_without_wal_append() {
    push_to_sink(
        BodyHeaders {
            content_type: "application/json; charset",
            content_encoding: None,
        },
        api_error_payload(),
    )
    .await
    .rejected_as_loki_error(&ExpectedLokiError {
        status: StatusCode::BAD_REQUEST,
        error_type: "bad_data",
        contains: "invalid media",
    });
}

#[tokio::test]
async fn loki_push_endpoint_accepts_json_content_type_parameters() {
    let outcome = push_to_sink(
        BodyHeaders {
            content_type: "application/json; charset=utf-8",
            content_encoding: None,
        },
        api_error_payload(),
    )
    .await;

    assert!(
        outcome.accepted()
            == [record(labels(API), 19, "api error")
                .with_metadata(labels([("detected_level", "error")]))]
    );
}

#[tokio::test]
async fn deprecated_api_prom_push_endpoint_writes_wal_records() {
    let outcome = push_request_to_sink(push_request(
        "/api/prom/push",
        JSON_TYPE,
        api_prod_stream(json!([["19", "api error"]])).to_string(),
    ))
    .await;

    assert!(
        outcome.accepted()
            == [record(labels(API_PROD), 19, "api error")
                .with_metadata(labels([("detected_level", "error")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_snappy_protobuf_payloads() {
    let outcome = push_proto(
        r#"{app="api", env="prod"}"#,
        proto_entry(LokiProtoTimestamp {
            seconds: 0,
            nanos: 19,
        })
        .metadata("status", "500")
        .metadata("trace_id", "abc"),
    )
    .await;

    assert!(
        outcome.accepted()
            == [
                record(labels(API_PROD), 19, "api error").with_metadata(labels([
                    ("detected_level", "error"),
                    ("status", "500"),
                    ("trace_id", "abc"),
                ]))
            ]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_empty_protobuf_labels_with_unknown_service() {
    let outcome = push_proto(
        "{}",
        proto_entry(LokiProtoTimestamp {
            seconds: 0,
            nanos: 19,
        }),
    )
    .await;

    assert!(
        outcome.accepted()
            == [record(labels(UNKNOWN_SERVICE), 19, "api error")
                .with_metadata(labels([("detected_level", "error")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_empty_string_protobuf_labels_with_unknown_service() {
    let outcome = push_proto(
        "",
        proto_entry(LokiProtoTimestamp {
            seconds: 0,
            nanos: 19,
        }),
    )
    .await;

    assert!(
        outcome.accepted()
            == [record(labels(UNKNOWN_SERVICE), 19, "api error")
                .with_metadata(labels([("detected_level", "error")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_snappy_protobuf_without_wal_append() {
    push_to_sink(PROTOBUF_TYPE, vec![0xff, 0xff, 0xff])
        .await
        .rejected_with(StatusCode::BAD_REQUEST, "snappy: corrupt input\n");
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_protobuf_without_wal_append() {
    push_to_sink(PROTOBUF_TYPE, vec![0x03, 0x08, 0xff, 0xff, 0xff])
        .await
        .rejected_with(StatusCode::BAD_REQUEST, "unexpected EOF\n");
}

#[tokio::test]
async fn loki_push_endpoint_rejects_empty_protobuf_push_like_loki_without_wal_append() {
    push_to_sink(
        PROTOBUF_TYPE,
        snappy(&LokiProtoPushRequest { streams: vec![] }),
    )
    .await
    .rejected_with(StatusCode::UNPROCESSABLE_ENTITY, NO_VALID_STREAM);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_protobuf_labels_like_loki_without_wal_append() {
    push_proto(
        r#"{bad-label="api"}"#,
        proto_entry(LokiProtoTimestamp {
            seconds: 0,
            nanos: 19,
        }),
    )
    .await
    .rejected_with(StatusCode::BAD_REQUEST, BAD_LABEL_PARSE);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_duplicate_protobuf_labels_without_wal_append() {
    push_proto(r#"{app="api", app="worker"}"#, proto_entry(LokiProtoTimestamp {
 seconds: 0,
 nanos: 19,
 }))
        .await
        .rejected_with(
            StatusCode::BAD_REQUEST,
            "stream '{app=\"api\", app=\"worker\", service_name=\"api\"}' has duplicate label name: 'app'\n",
        );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_duplicate_protobuf_structured_metadata_using_last_value() {
    let outcome = push_proto(
        r#"{app="api"}"#,
        proto_entry(LokiProtoTimestamp {
            seconds: 0,
            nanos: 19,
        })
        .metadata("trace_id", "abc")
        .metadata("trace_id", "def"),
    )
    .await;

    assert!(
        outcome.accepted()
            == [record(labels(API), 19, "api error")
                .with_metadata(labels([("detected_level", "error"), ("trace_id", "def")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_invalid_protobuf_structured_metadata_name() {
    let outcome = push_proto(
        r#"{app="api"}"#,
        proto_entry(LokiProtoTimestamp {
            seconds: 0,
            nanos: 19,
        })
        .metadata("9bad", "metadata"),
    )
    .await;

    assert!(
        outcome.accepted()
            == [record(labels(API), 19, "api error")
                .with_metadata(labels([("detected_level", "error"), ("9bad", "metadata")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_empty_protobuf_structured_metadata_name_like_loki() {
    push_proto(
        r#"{app="api"}"#,
        proto_entry(LokiProtoTimestamp {
            seconds: 0,
            nanos: 19,
        })
        .metadata("", "metadata"),
    )
    .await
    .rejected_with(StatusCode::INTERNAL_SERVER_ERROR, "label name is empty\n");
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_timestamp_without_wal_append() {
    let outcome = push_json(&api_stream(json!([["not-a-timestamp", "api error"]]))).await;

    assert!(outcome.status == StatusCode::BAD_REQUEST);
    assert!(outcome.records.is_empty());
}

fn number_decode_error(context: &str) -> String {
    format!(
        "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value looks like Number/Boolean/None, but can't find its end: ',' or '}}' symbol, error found in #10 byte of ...|estamp\"]]}}]}}|..., bigger context ...|{context}|...\n"
    )
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_json_timestamp_like_loki_without_wal_append() {
    push_json(&api_stream(json!([[
        "not-a-timestamp",
        "invalid push timestamp"
    ]])))
    .await
    .rejected_with(
        StatusCode::BAD_REQUEST,
        &number_decode_error("s\":[[\"not-a-timestamp\",\"invalid push timestamp\"]]}]}"),
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_string_json_timestamp_like_loki_without_wal_append() {
    push_json(&api_stream(json!([[
        1_000_000_000,
        "non-string push timestamp"
    ]])))
    .await
    .rejected_with(
        StatusCode::BAD_REQUEST,
        &number_decode_error("alues\":[[1000000000,\"non-string push timestamp\"]]}]}"),
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_object_json_timestamp_like_loki_without_wal_append() {
    push_json(&api_stream(
        json!([[{"ts": "1000000000"}, "object push timestamp"]]),
    ))
    .await
    .rejected_with(
        StatusCode::BAD_REQUEST,
        &number_decode_error("\":[[{\"ts\":\"1000000000\"},\"object push timestamp\"]]}]}"),
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_array_json_timestamp_like_loki_without_wal_append() {
    push_json(&api_stream(json!([[
        ["1000000000"],
        "array push timestamp"
    ]])))
    .await
    .rejected_with(
        StatusCode::BAD_REQUEST,
        &number_decode_error("values\":[[[\"1000000000\"],\"array push timestamp\"]]}]}"),
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_json_line_like_loki_without_wal_append() {
    push_json(&api_stream(json!([["1000000000", 500]])))
        .await
        .rejected_with(
            StatusCode::BAD_REQUEST,
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string, but can't find closing '\"' symbol, error found in #10 byte of ...|00\",500]]}]}|..., bigger context ...|ream\":{\"app\":\"api\"},\"values\":[[\"1000000000\",500]]}]}|...\n",
        );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_negative_timestamp_without_wal_append() {
    push_json(&api_stream(json!([["-1", "api error"]])))
        .await
        .rejected_as_loki_error(&ExpectedLokiError {
            status: StatusCode::BAD_REQUEST,
            error_type: "bad_data",
            contains: "timestamp",
        });
}

#[tokio::test]
async fn loki_push_endpoint_rejects_negative_protobuf_timestamp_like_loki_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = build_service_router(
        &minimal_service_config(Role::Distributor),
        ServiceDependencies::default().with_wal_sink(sink.clone()),
        None,
    )
    .await
    .unwrap();

    let (status, body) = push(
        &app,
        push_request(
            PUSH,
            PROTOBUF_TYPE,
            snappy_push(
                r#"{app="api"}"#,
                proto_entry(LokiProtoTimestamp {
                    seconds: -1,
                    nanos: 0,
                }),
            ),
        ),
    )
    .await;

    PushOutcome {
        status,
        body,
        records: sink.records(),
    }
    .rejected_containing(&[
        "timestamp too old",
        "1969-12-31T23:59:59Z",
        r#"{app="api", service_name="api"}"#,
    ]);
}

#[tokio::test]
async fn loki_push_endpoint_rejects_out_of_range_protobuf_timestamp_nanos_without_wal_append() {
    push_proto(
        r#"{app="api"}"#,
        proto_entry(LokiProtoTimestamp {
            seconds: 0,
            nanos: 1_000_000_000,
        }),
    )
    .await
    .rejected_as_loki_error(&ExpectedLokiError {
        status: StatusCode::BAD_REQUEST,
        error_type: "bad_data",
        contains: "timestamp",
    });
}

#[tokio::test]
async fn loki_push_endpoint_rejects_invalid_json_labels_without_wal_append() {
    push_json(
        &JsonStream {
            stream: json!({ "bad-label": "api" }),
            values: json!([["19", "api error"]]),
        }
        .payload(),
    )
    .await
    .rejected_with(StatusCode::BAD_REQUEST, BAD_LABEL_PARSE);
}

#[tokio::test]
async fn loki_push_endpoint_accepts_duplicate_json_labels_using_last_value() {
    let outcome = push_to_sink(
        JSON_TYPE,
        r#"{
            "streams": [
                {
                    "stream": {
                        "app": "api",
                        "app": "worker"
                    },
                    "values": [
                        ["19", "api error"]
                    ]
                }
            ]
        }"#,
    )
    .await;

    assert!(
        outcome.accepted()
            == [record(
                labels([("app", "worker"), ("service_name", "worker")]),
                19,
                "api error"
            )
            .with_metadata(labels([("detected_level", "error")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_empty_json_labels_with_unknown_service() {
    let outcome = push_json(
        &JsonStream {
            stream: json!({}),
            values: json!([["19", "api info"]]),
        }
        .payload(),
    )
    .await;

    assert!(
        outcome.accepted()
            == [record(labels(UNKNOWN_SERVICE), 19, "api info")
                .with_metadata(labels([("detected_level", "info")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_invalid_json_structured_metadata_name() {
    let outcome = push_json(&api_stream(
        json!([["19", "api error", {"9bad": "metadata"}]]),
    ))
    .await;

    assert!(
        outcome.accepted()
            == [record(labels(API), 19, "api error")
                .with_metadata(labels([("detected_level", "error"), ("9bad", "metadata")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_accepts_duplicate_json_structured_metadata_using_last_value() {
    let outcome = push_to_sink(
        JSON_TYPE,
        r#"{
            "streams": [
                {
                    "stream": {
                        "app": "api"
                    },
                    "values": [
                        [
                            "19",
                            "api error",
                            {
                                "trace_id": "abc",
                                "trace_id": "def"
                            }
                        ]
                    ]
                }
            ]
        }"#,
    )
    .await;

    assert!(
        outcome.accepted()
            == [record(labels(API), 19, "api error")
                .with_metadata(labels([("detected_level", "error"), ("trace_id", "def")]))]
    );
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_string_json_structured_metadata_without_wal_append() {
    let sink = InMemoryWalSink::default();
    let app = distributor_router(sink.clone());

    for structured_metadata in [json!({"status": 500}), json!({"nested": {"status": "500"}})] {
        let (status, body) = push(
            &app,
            push_request(
                PUSH,
                JSON_TYPE,
                api_stream(json!([["19", "api error", structured_metadata]])).to_string(),
            ),
        )
        .await;

        assert!(status == StatusCode::BAD_REQUEST);
        assert!(body.contains(
            "loghttp.PushRequest.Streams: []loghttp.LogProtoStream: unmarshalerDecoder: Value is string"
        ));
    }

    assert!(sink.records().is_empty());
}

#[tokio::test]
async fn loki_push_endpoint_rejects_non_object_json_structured_metadata_like_loki() {
    push_json(&api_stream(json!([["19", "api error", null]])))
        .await
        .rejected_containing(&[LOOKS_LIKE_OBJECT, "api error\",null"]);
}
