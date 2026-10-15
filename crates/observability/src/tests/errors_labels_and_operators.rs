use crate::tests::prelude::{
    Arc, BTreeMap, BlockIndex, CompactorDeleteRequest, HeaderMap, LabelIndex, Limits,
    ListDeleteRequestsParams, OtlpAnyValue, OtlpArrayValue, OtlpKeyValue, OtlpKeyValueList,
    ProtoAnyValue, ProtoKeyValue, QuerierState, TimeRange, check, delete_request_overlaps_filter,
    discover_detected_level_label, json, loki_json_push_payload_parse_error,
    loki_json_push_streams_parse_error, loki_label_set, loki_proto_label_parse_error,
    loki_push_label_parse_error, loki_structured_metadata_value_parse_error,
    otlp_severity_number_to_string, otlp_timestamp_ns, otlp_value_to_json,
    parse_cancel_delete_request_params, parse_create_delete_request_params,
    parse_list_delete_requests_params, parse_loki_delete_timestamp_query_param,
    previous_char_boundary, proto_any_value, proto_value_to_json, ranges_overlap,
};

/// `remove_empty_object_field` drops a field only when it is an object
/// with nothing in it. A field that holds something stays, and so does one
/// that is not an object at all -- an empty array is not an empty object.
#[test]
pub(crate) fn an_empty_object_field_is_removed_and_nothing_else_is() {
    let mut value = serde_json::json!({
        "empty": {},
        "full": {"a": 1},
        "array": [],
        "null": null,
    });
    for field in ["empty", "full", "array", "null"] {
        crate::tests::prelude::remove_empty_object_field(&mut value, field);
    }
    check!(
        value == serde_json::json!({"full": {"a": 1}, "array": [], "null": null}),
        "got {value}"
    );

    // A value that is not an object at all is left alone rather than
    // panicking on the way past.
    let mut scalar = serde_json::json!(7);
    crate::tests::prelude::remove_empty_object_field(&mut scalar, "empty");
    check!(scalar == serde_json::json!(7));
}

#[test]
pub(crate) fn loki_error_contexts_respect_utf8_boundaries_and_offsets() {
    let body = "{\"streams\":\"not-array\"}";
    assert!(
        loki_json_push_streams_parse_error(body.as_bytes(), &json!("not-array"))
            .contains("|{\"streams\":\"not-array\"}|")
    );
    // That assertion reads the *bigger* context, which is the whole body
    // either way. The narrow window is twenty bytes from nine before the
    // value, and stops short of the closing brace.
    check!(
        loki_json_push_streams_parse_error(body.as_bytes(), &json!("not-array"))
            .contains(r#"...|streams":"not-array"|..."#),
        "the narrow window is twenty bytes wide"
    );

    // The payload error's window is eleven bytes from the first
    // non-whitespace byte. A body that starts with one puts that at zero,
    // which is the only offset where a width computed by multiplying
    // rather than adding gives a different answer.
    check!(
        loki_json_push_payload_parse_error(b"\"not-json-at-all\"")
            .contains(r#"...|"not-json-a|..."#),
        "eleven bytes from the start"
    );

    let structured =
        br#"{"streams":[{"stream":{"app":"api"},"values":[["1","line",{"ok":true}]]}]}"#;
    let error = loki_structured_metadata_value_parse_error(structured, "ok", &json!(true));
    // The context window starts three bytes before the VALUE, which sits
    // one past the quoted key and its colon. A `contains` on the key and
    // value together is satisfied by any nearby offset -- the whole
    // eighty-byte window holds them -- so the window is pinned exactly.
    check!(
        error.contains(r#"...|k":true}]]}]}|..."#),
        "the context starts three bytes into the value: {error}"
    );

    let text = "ab\u{20ac}cd";
    assert_eq!(previous_char_boundary(text, 4), 2);
    assert_eq!(previous_char_boundary(text, text.len()), text.len());
}

#[test]
pub(crate) fn loki_label_and_level_helpers_pin_boundaries() {
    let mut rendered_labels = BTreeMap::new();
    rendered_labels.insert("app".to_string(), "api".to_string());
    rendered_labels.insert("env".to_string(), "prod".to_string());
    assert_eq!(
        loki_label_set(&rendered_labels),
        r#"{app="api",env="prod"}"#
    );
    check!(loki_push_label_parse_error(&rendered_labels, "bad-name").contains("1:5"));
    // Every character of "bad-name" is judged the same way whether or not
    // it is treated as the first, so that case cannot tell the two apart.
    // A digit can: it is allowed anywhere except at the start.
    let digit_then_invalid = loki_push_label_parse_error(&rendered_labels, "b9-name");
    check!(
        digit_then_invalid.contains("1:4"),
        "the hyphen is the third character: {digit_then_invalid}"
    );
    check!(
        digit_then_invalid.contains("'-'"),
        "and the hyphen is what is reported: {digit_then_invalid}"
    );
    check!(
        loki_proto_label_parse_error(r#"{9bad="x"}"#)
            .unwrap()
            .contains("1:2")
    );
    check!(
        loki_proto_label_parse_error(r#"{app="api",9bad="x"}"#)
            .unwrap()
            .contains("1:12")
    );
    // A digit is fine once a name has started. Both cases above are
    // rejections, so without this the tracking could judge every character
    // by the first one's rule and they would still pass.
    check!(loki_proto_label_parse_error(r#"{a9="x"}"#).is_none());
    check!(loki_proto_label_parse_error(r#"{app="api",b9="x"}"#).is_none());

    // A comma starts a new name even when no `=` came between: in
    // `{app="api",...}` the `=` has already reset the tracking, so only a
    // list without values shows the comma doing it.
    check!(
        loki_proto_label_parse_error("{app,9bad}")
            .unwrap()
            .contains("1:6")
    );
    check!(loki_proto_label_parse_error("{app,b9}").is_none());
    // After `=` the parser stops looking for a name, so an unquoted value
    // is not judged as one. A quoted value never shows this: the string
    // handling swallows it before the name check is reached.
    check!(loki_proto_label_parse_error("{app=bad-value}").is_none());

    let mut stream = BTreeMap::new();
    stream.insert("app".to_string(), "api".to_string());
    let mut detected = BTreeMap::new();
    discover_detected_level_label(
        &stream,
        &mut detected,
        "api ERROR happened",
        &Limits::default(),
    );
    check!(detected.get("detected_level").map(String::as_str) == Some("error"));
    let mut expected_stream = BTreeMap::new();
    expected_stream.insert("app".to_string(), "api".to_string());
    check!(stream == expected_stream);
    for held in ["level", "severity", "severity_text"] {
        let mut labels = BTreeMap::new();
        labels.insert(held.to_string(), "custom".to_string());
        let mut metadata = BTreeMap::new();
        discover_detected_level_label(
            &labels,
            &mut metadata,
            "api error happened",
            &Limits::default(),
        );
        check!(metadata.get("detected_level").map(String::as_str) == Some("custom"));
        check!(labels.len() == 1);
    }
}

#[test]
pub(crate) fn timestamp_and_value_conversions_cover_json_and_proto_shapes() {
    assert_eq!(otlp_timestamp_ns(&json!("123")).unwrap(), 123);
    assert_eq!(otlp_timestamp_ns(&json!(456)).unwrap(), 456);
    assert!(otlp_timestamp_ns(&json!(-1)).is_err());
    assert_eq!(
        otlp_severity_number_to_string(&json!("INFO")).unwrap(),
        "INFO"
    );
    assert_eq!(otlp_severity_number_to_string(&json!(9)).unwrap(), "9");

    let otlp_value = OtlpAnyValue::Kvlist(OtlpKeyValueList {
        values: Some(vec![
            OtlpKeyValue {
                key: "ok".to_string(),
                value: OtlpAnyValue::Bool(true),
            },
            OtlpKeyValue {
                key: "items".to_string(),
                value: OtlpAnyValue::Array(OtlpArrayValue {
                    values: Some(vec![OtlpAnyValue::String("a".to_string())]),
                }),
            },
        ]),
    });
    assert_eq!(
        otlp_value_to_json(&otlp_value),
        json!({"items": ["a"], "ok": true})
    );

    let proto_value = ProtoAnyValue {
        value: Some(proto_any_value::Value::KvlistValue(
            opentelemetry_proto::tonic::common::v1::KeyValueList {
                values: vec![ProtoKeyValue {
                    key: "answer".to_string(),
                    value: Some(ProtoAnyValue {
                        value: Some(proto_any_value::Value::IntValue(42)),
                    }),
                    ..Default::default()
                }],
            },
        )),
    };
    assert_eq!(proto_value_to_json(&proto_value), json!({"answer": 42}));
}

/// Cancelling a delete request removes the one request that matches BOTH
/// the tenant and the id. Two tenants can hold the same id -- the counter
/// is per store, but the ids are handed out per tenant view -- so a cancel
/// that matched on either alone would take a request belonging to someone
/// else.
#[test]
pub(crate) fn cancelling_a_delete_request_takes_only_that_tenant_s() {
    #[derive(Debug, PartialEq)]
    struct RequestIdentity {
        tenant: String,
        request_id: String,
    }

    let request = |tenant: &str, request_id: &str| crate::tests::prelude::CompactorDeleteRequest {
        tenant: tenant.to_string(),
        request_id: request_id.to_string(),
        query: r#"{app="api"}"#.to_string(),
        start_time: 0,
        end_time: 1,
        status: "received".to_string(),
        created_at: 0,
    };
    let state = crate::tests::prelude::CompactorDeleteState {
        delete_requests: crate::tests::prelude::SharedLogDeleteRequests::default(),
        query_authorizer: Arc::new(crate::tests::prelude::AllowAllQueryAuthorizer),
    };
    state
        .delete_requests
        .inner
        .lock()
        .expect("the delete state lock is held")
        .requests = vec![
        request("tenant-a", "delete-1"),
        request("tenant-b", "delete-1"),
        request("tenant-a", "delete-2"),
    ];

    let tenant = crate::tests::prelude::TenantId::new("tenant-a").expect("a valid tenant id");
    crate::tests::prelude::execute_cancel_delete_request(
        &state,
        &crate::tests::prelude::RequestSecurity::unauthenticated(),
        &tenant,
        Some("request_id=delete-1"),
    )
    .expect("the cancel succeeds");

    let left = state
        .delete_requests
        .inner
        .lock()
        .expect("the delete state lock is held")
        .requests
        .iter()
        .map(|request| RequestIdentity {
            tenant: request.tenant.clone(),
            request_id: request.request_id.clone(),
        })
        .collect::<Vec<_>>();
    check!(
        left == vec![
            RequestIdentity {
                tenant: "tenant-b".to_string(),
                request_id: "delete-1".to_string()
            },
            RequestIdentity {
                tenant: "tenant-a".to_string(),
                request_id: "delete-2".to_string()
            },
        ],
        "got {left:?}"
    );
}

struct DeleteOverlapCase {
    filter: ListDeleteRequestsParams,
    overlaps: bool,
}

struct RangeOverlapCase {
    right: TimeRange,
    overlaps: bool,
}

#[test]
pub(crate) fn delete_request_query_parsing_and_overlap_boundaries() {
    let params = parse_create_delete_request_params(Some(
        "query=%7Bapp%3D%22api%22%7D&start=10&end=20&max_interval=1h",
    ))
    .unwrap();
    assert_eq!(params.query, r#"{app="api"}"#);
    assert_eq!(params.start_time, 10);
    assert_eq!(params.end_time, 20);
    assert!(parse_create_delete_request_params(Some("query=x&start=20&end=10")).is_err());
    // A window of zero width is allowed: "end before start" is the error,
    // not "end not after start".
    check!(
        parse_create_delete_request_params(Some("query=x&start=10&end=10")).is_ok(),
        "a start and end at the same instant"
    );
    // `max_interval` is parsed for its own sake -- the value is discarded,
    // so only an invalid one shows the parse happening at all. The case
    // above passes `1h`, which is accepted whether or not it is read.
    check!(
        parse_create_delete_request_params(Some(
            "query=x&start=10&end=20&max_interval=notaduration"
        ))
        .is_err(),
        "an unparseable max_interval is refused"
    );

    let list = parse_list_delete_requests_params(Some("start=10&end=20")).unwrap();
    assert_eq!(list.start_time, Some(10));
    assert_eq!(list.end_time, Some(20));
    assert!(parse_list_delete_requests_params(Some("start=10")).is_err());
    assert_eq!(
        parse_cancel_delete_request_params(Some("request_id=delete-1&force=true")).unwrap(),
        "delete-1"
    );
    assert!(parse_cancel_delete_request_params(Some("request_id=delete-1&force=maybe")).is_err());
    assert_eq!(
        parse_loki_delete_timestamp_query_param("start", "1.5").unwrap(),
        1
    );

    let request = CompactorDeleteRequest {
        tenant: "tenant-a".to_string(),
        request_id: "delete-1".to_string(),
        query: r#"{app="api"}"#.to_string(),
        start_time: 10,
        end_time: 20,
        status: "received".to_string(),
        created_at: 1,
    };
    for case in [
        DeleteOverlapCase {
            filter: list,
            overlaps: true,
        },
        DeleteOverlapCase {
            filter: ListDeleteRequestsParams {
                start_time: Some(20),
                end_time: Some(30),
            },
            overlaps: true,
        },
        DeleteOverlapCase {
            filter: ListDeleteRequestsParams {
                start_time: Some(21),
                end_time: Some(30),
            },
            overlaps: false,
        },
    ] {
        assert_eq!(
            delete_request_overlaps_filter(&request, &case.filter),
            case.overlaps
        );
    }
    for case in [
        RangeOverlapCase {
            right: TimeRange::new(20, 30).unwrap(),
            overlaps: true,
        },
        RangeOverlapCase {
            right: TimeRange::new(21, 30).unwrap(),
            overlaps: false,
        },
    ] {
        assert_eq!(
            ranges_overlap(TimeRange::new(10, 20).unwrap(), case.right),
            case.overlaps
        );
    }
}

/// The three `LogQL` set operators each map to their own variant, and an
/// unknown word maps to none. Deleting an arm does not fail to compile --
/// it falls to the catch-all and the operator simply stops existing.
#[test]
pub(crate) fn every_metric_set_operator_maps_to_its_own_variant() {
    use crate::tests::prelude::MetricBinarySetOp;

    check!(crate::tests::prelude::parse_metric_set_operator("and") == Some(MetricBinarySetOp::And));
    check!(crate::tests::prelude::parse_metric_set_operator("or") == Some(MetricBinarySetOp::Or));
    check!(
        crate::tests::prelude::parse_metric_set_operator("unless")
            == Some(MetricBinarySetOp::Unless)
    );
    check!(crate::tests::prelude::parse_metric_set_operator("nor") == None);
    check!(crate::tests::prelude::parse_metric_set_operator("") == None);
}

struct QueryResponseBody {
    status: axum::http::StatusCode,
    bytes: axum::body::Bytes,
}

struct PostQueryCase {
    name: &'static str,
    response: axum::response::Response,
}

/// The four POST query endpoints each answer with a JSON body. Replacing
/// any of them with a default `Response` yields an empty 200 -- a status
/// check alone accepts that, so the body has to be read.
#[tokio::test]
pub(crate) async fn the_post_query_endpoints_answer_with_a_body() {
    use axum::{extract::State, response::IntoResponse as _};

    let dir = tempfile::TempDir::new().expect("temp dir");
    let state = QuerierState::new(dir.path(), LabelIndex::default(), BlockIndex::default());
    let mut headers = HeaderMap::new();
    headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));
    let body =
        || axum::body::Bytes::from_static(b"query=%7Bapp%3D%22web%22%7D&start=0&end=10000000001");
    let request = || crate::tests::prelude::PostedQueryRequest {
        security: crate::tests::prelude::RequestSecurity::unauthenticated(),
        headers: headers.clone(),
        raw_query: None,
        body: body(),
    };
    let read = |response: axum::response::Response| async move {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("the response body is readable");
        QueryResponseBody { status, bytes }
    };

    let cases = [
        PostQueryCase {
            name: "detected_fields",
            response: crate::tests::prelude::detected_fields_post(State(state.clone()), request())
                .await
                .expect("detected-fields request succeeds"),
        },
        PostQueryCase {
            name: "detected_labels",
            response: crate::tests::prelude::detected_labels_post(State(state.clone()), request())
                .await
                .expect("detected-labels request succeeds"),
        },
        PostQueryCase {
            name: "index_volume",
            response: crate::tests::prelude::index_volume_post(State(state.clone()), request())
                .await
                .expect("index-volume request succeeds"),
        },
        PostQueryCase {
            name: "label_names",
            response: crate::tests::prelude::api_prom_label_names_post(
                State(state.clone()),
                request(),
            )
            .await
            .expect("label-names request succeeds"),
        },
    ];
    for case in cases {
        let answer = read(case.response.into_response()).await;
        check!(
            answer.status == axum::http::StatusCode::OK,
            "{}: {}",
            case.name,
            answer.status
        );
        let value: serde_json::Value =
            serde_json::from_slice(&answer.bytes).unwrap_or_else(|error| {
                panic!(
                    "{}: body is not JSON ({error}): {:?}",
                    case.name, answer.bytes
                )
            });
        if case.name == "index_volume" {
            check!(
                value["status"] == "success" && value["data"]["resultType"] == "vector",
                "{}: got {value}",
                case.name
            );
            check!(
                value["data"]["result"].as_array().map(Vec::len) == Some(0),
                "an empty store has no volume series: {value}"
            );
        } else {
            // An empty store answers with an empty JSON object, not with an
            // empty body -- a client parsing the response needs something
            // to parse.
            check!(value == serde_json::json!({}), "{}: got {value}", case.name);
        }
    }
}
