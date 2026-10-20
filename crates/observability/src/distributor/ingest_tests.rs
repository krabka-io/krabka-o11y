use assert2::assert;
use proptest::prelude::*;

use super::*;

fn json_headers() -> HeaderMap {
    HeaderMap::from_iter([(crate::CONTENT_TYPE, "application/json".parse().unwrap())])
}

fn outcome(
    result: Result<Vec<WalLogRecord>, DistributorError>,
) -> Result<Vec<WalLogRecord>, (String, String, StatusCode)> {
    result.map_err(|error| {
        (
            format!("{error:?}"),
            error.to_string(),
            error.into_response().status(),
        )
    })
}

fn compare_with_legacy(body: &[u8]) {
    let tenant = TenantId::new("test").unwrap();
    let headers = json_headers();
    for limits in [Limits::unenforced(), Limits::default()] {
        assert!(
            outcome(normalize_loki_http_push(&tenant, &headers, body, &limits))
                == outcome(legacy_http_push(&tenant, &headers, body, &limits)),
            "body: {:?}",
            String::from_utf8_lossy(body)
        );
    }
}

#[test]
fn json_push_preserves_top_level_and_duplicate_field_contract() {
    for body in [
        "",
        " ",
        "null",
        " null \n",
        "null false",
        "true",
        "42",
        "1e400",
        "\"streams\"",
        "[]",
        "[null]",
        "[",
        "[1,]",
        "{} trailing",
        "\u{feff}{}",
        "{\"streams\":[]",
        "{\"streams\":[]}\0",
        r#"{}"#,
        r#"{"streams":null}"#,
        r#"{"streams":[]}"#,
        r#"{"streams":"bad"}"#,
        r#"{"streams":[],"streams":[]}"#,
        r#"{"streams":null,"streams":[]}"#,
        r#"{"streams":[],"streams":null}"#,
        r#"{"str\u0065ams":[],"streams":[]}"#,
        r#"{"streams":[],"unknown":1e400}"#,
        r#"{"unknown":1e400,"streams":[]}"#,
        r#"{"unknown":null,"unknown":false,"streams":[]}"#,
        r#"{"streams":[{"stream":{"app":"first","app":"last"},"values":[["1000000000","line",{"trace_id":"first","trace_id":"last"}]]}]}"#,
        r#"{"streams":[{"stream":"bad","stream":{"app":"api"},"values":false,"values":[["1000000000","line"]]}]}"#,
        r#"{"streams":[{"stream":{"app":"api"},"stream":null,"values":[]}]}"#,
    ] {
        compare_with_legacy(body.as_bytes());
    }
    compare_with_legacy(b"{\"streams\":[],\"unknown\":\"\xff\"}");
    for depth in [125, 126, 127, 128, 129] {
        let nested = format!(
            "{{\"unknown\":{}0{},\"streams\":[]}}",
            "[".repeat(depth),
            "]".repeat(depth)
        );
        compare_with_legacy(nested.as_bytes());
    }
}

#[test]
fn json_push_preserves_validation_order_and_malformed_errors() {
    let entry = serde_json::json!(["1000000000", "line"]);
    let labels = serde_json::json!({"app": "api"});
    for streams in [
        serde_json::json!([null]),
        serde_json::json!([{"stream": null, "values": []}]),
        serde_json::json!([{"stream": labels, "values": false}]),
        serde_json::json!([{"stream": labels, "values": null}]),
        serde_json::json!([{"stream": labels}]),
        serde_json::json!([{"values": [entry]}]),
        serde_json::json!([{"stream": {"app": 1}, "values": []}]),
        serde_json::json!([{"stream": {}, "values": [entry]}]),
        serde_json::json!([{"stream": {"bad-label": "api"}, "values": [entry]}]),
        serde_json::json!([{"stream": labels, "values": ["bad"]}]),
        serde_json::json!([{"stream": labels, "values": [[], ["1000000000"]]}]),
        serde_json::json!([{"stream": labels, "values": [[1, "line"]]}]),
        serde_json::json!([{"stream": labels, "values": [[{}, "line"]]}]),
        serde_json::json!([{"stream": labels, "values": [["bad", "line"]]}]),
        serde_json::json!([{"stream": labels, "values": [["1000000000", 1]]}]),
        serde_json::json!([{"stream": labels, "values": [["1000000000", "line", null]]}]),
        serde_json::json!([{"stream": labels, "values": [["1000000000", "line", {"status": 500}]]}]),
        serde_json::json!([{"stream": labels, "values": [["1000000000", "line", {"9bad": "value"}]]}]),
        serde_json::json!([{"stream": labels, "values": [["1000000000", "line", {}, "extra"]]}]),
        // Stream shape errors precede array errors, which precede timestamp
        // errors, which precede metadata errors across the whole batch.
        serde_json::json!([{"stream": labels, "values": ["bad"]}, null]),
        serde_json::json!([{"stream": labels, "values": [[1, "line"]]}, {"stream": labels, "values": ["bad"]}]),
        serde_json::json!([{"stream": labels, "values": [["1000000000", "line", null], [1, "line"]]}]),
        serde_json::json!([{"stream": labels, "values": [entry]}]),
    ] {
        let body = serde_json::to_vec(&serde_json::json!({"streams": streams})).unwrap();
        compare_with_legacy(&body);
        for end in 0..body.len() {
            compare_with_legacy(&body[..end]);
        }
        let mut trailing = body;
        trailing.extend_from_slice(b" false");
        compare_with_legacy(&trailing);
    }
}

proptest! {
    #[test]
    fn valid_json_pushes_preserve_complete_records(
        lines in proptest::collection::vec(".*", 1..16),
        metadata in proptest::collection::btree_map("[a-z_]{1,12}", ".*", 0..8),
    ) {
        let streams: Vec<_> = lines.into_iter().map(|line| serde_json::json!({
            "stream": {"app": "api"},
            "values": [["1000000000", line, metadata]],
        })).collect();
        let body = serde_json::to_vec(&serde_json::json!({"streams": streams})).unwrap();
        compare_with_legacy(&body);
    }

    #[test]
    fn arbitrary_bytes_preserve_json_push_errors(body in proptest::collection::vec(any::<u8>(), 0..512)) {
        compare_with_legacy(&body);
    }

    #[test]
    fn repeated_top_level_fields_preserve_json_push_errors(
        fields in proptest::collection::vec(
            (
                prop_oneof![Just("streams"), Just("unknown"), Just("str\\u0065ams")],
                prop_oneof![Just("null"), Just("[]"), Just("{}"), Just("false"), Just("1e400"), Just("[null]")],
            ),
            0..8,
        )
    ) {
        let fields: Vec<_> = fields.into_iter().map(|(key, value)| format!("\"{key}\":{value}")).collect();
        compare_with_legacy(format!("{{{}}}", fields.join(",")).as_bytes());
    }
}

// This benchmark needs private ingest functions and the reference implementation.
// Run with cargo test -p krabka-observability --release --lib
// json_push_decode_benchmark -- --ignored --nocapture.
#[test]
#[ignore = "manual before/after ingest benchmark"]
fn json_push_decode_benchmark() {
    let tenant = TenantId::new("benchmark").unwrap();
    let headers = json_headers();
    let limits = Limits::unenforced();
    println!("body_bytes,streams,metadata_bytes,legacy_mean_us,optimized_mean_us,speedup");
    for size in [100_000, 1_000_000, 10_000_000] {
        for stream_count in [1, 64] {
            for metadata_size in [0, 1024] {
                let body = benchmark_body(size, stream_count, metadata_size);
                assert!(
                    legacy_http_push(&tenant, &headers, &body, &limits).unwrap()
                        == normalize_loki_http_push(&tenant, &headers, &body, &limits).unwrap()
                );
                let mut legacy = std::time::Duration::ZERO;
                let mut optimized = std::time::Duration::ZERO;
                // Alternate order to reduce systematic cache and CPU clock bias.
                for iteration in 0..20 {
                    for old in [iteration % 2 == 0, iteration % 2 != 0] {
                        let started = std::time::Instant::now();
                        let normalize = if old {
                            legacy_http_push
                        } else {
                            normalize_loki_http_push
                        };
                        drop(std::hint::black_box(
                            normalize(&tenant, &headers, std::hint::black_box(&body), &limits)
                                .unwrap(),
                        ));
                        if old {
                            legacy += started.elapsed();
                        } else {
                            optimized += started.elapsed();
                        }
                    }
                }
                println!(
                    "{size},{stream_count},{metadata_size},{:.1},{:.1},{:.2}",
                    legacy.as_secs_f64() * 1_000_000.0 / 20.0,
                    optimized.as_secs_f64() * 1_000_000.0 / 20.0,
                    legacy.as_secs_f64() / optimized.as_secs_f64(),
                );
            }
        }
    }
}

fn benchmark_body(size: usize, stream_count: usize, metadata_size: usize) -> Vec<u8> {
    let mut payload = serde_json::json!({"streams": (0..stream_count).map(|index| {
        serde_json::json!({"stream": {"app": "benchmark", "instance": index.to_string()}, "values": []})
    }).collect::<Vec<_>>()});
    let base_size = serde_json::to_vec(&payload).unwrap().len();
    let mut entry = serde_json::json!(["1000000000", "x".repeat(128)]);
    if metadata_size != 0 {
        entry
            .as_array_mut()
            .unwrap()
            .push(serde_json::json!({"trace_id": "m".repeat(metadata_size)}));
    }
    let entry_size = serde_json::to_vec(&entry).unwrap().len();
    let entry_count = (size - base_size + stream_count) / (entry_size + 1);
    for index in 0..entry_count {
        payload["streams"][index % stream_count]["values"]
            .as_array_mut()
            .unwrap()
            .push(entry.clone());
    }
    let padding = size - serde_json::to_vec(&payload).unwrap().len();
    let line = payload["streams"][0]["values"][0][1].as_str().unwrap();
    payload["streams"][0]["values"][0][1] = Value::String(format!("{line}{}", "x".repeat(padding)));
    let body = serde_json::to_vec(&payload).unwrap();
    assert!(body.len() == size);
    body
}

// Reference implementation for compatibility checks and before/after measurements.
fn legacy_http_push(
    tenant: &TenantId,
    headers: &HeaderMap,
    body: &[u8],
    limits: &Limits,
) -> Result<Vec<WalLogRecord>, DistributorError> {
    let body = decode_loki_http_body(headers, body)?;
    if is_loki_json_content_type(headers)? {
        let raw_payload: Value =
            serde_json::from_slice(&body).map_err(|_| DistributorError::InvalidPushPayload)?;
        if raw_payload.is_null() {
            return Err(DistributorError::NoValidStreams);
        }
        if !raw_payload.is_object() {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_payload_parse_error(&body),
            ));
        }
        let payload =
            serde_json::from_slice(&body).map_err(|_| DistributorError::InvalidPushPayload)?;
        let payload = legacy_stream_objects(payload, &body)?;
        validate_loki_json_push_value_arrays(&payload, &body)?;
        validate_loki_json_push_timestamp_types(&payload, &body)?;
        validate_loki_json_structured_metadata_value_types(&payload, &body)?;
        normalize_loki_push(tenant, payload, limits)
    } else {
        let decompressed = SnappyDecoder::new()
            .decompress_vec(&body)
            .map_err(DistributorError::LokiSnappyDecode)?;
        let payload = LokiProtoPushRequest::decode(decompressed.as_slice())
            .map_err(DistributorError::LokiDecode)?;
        normalize_loki_proto_push(tenant, payload, limits)
    }
}

fn legacy_stream_objects(
    payload: LokiPushRequest,
    body: &[u8],
) -> Result<LokiTypedPushRequest, DistributorError> {
    let Some(streams) = payload.streams else {
        return Err(DistributorError::NoValidStreams);
    };
    let Some(raw_streams) = streams.as_array() else {
        return Err(DistributorError::InvalidJsonPushValueSyntax(
            loki_json_push_streams_parse_error(body, &streams),
        ));
    };
    if raw_streams.is_empty() {
        return Err(DistributorError::NoValidStreams);
    }
    let mut streams = Vec::with_capacity(raw_streams.len());
    for stream in raw_streams {
        if !stream.is_object() {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_stream_parse_error(body, stream),
            ));
        }
        if let Some(labels) = stream.get("stream")
            && !labels.is_object()
        {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_labels_field_parse_error(body),
            ));
        }
        if let Some(values) = stream.get("values")
            && !values.is_array()
            && !values.is_null()
        {
            return Err(DistributorError::InvalidJsonPushValueSyntax(
                loki_json_push_values_field_parse_error(body, values),
            ));
        }
        let stream = serde_json::from_value(stream.clone())
            .map_err(|_| DistributorError::InvalidPushPayload)?;
        streams.push(stream);
    }

    Ok(LokiTypedPushRequest { streams })
}
