use assert2::assert;

use super::{Labels, Limits, discover_detected_level_label};
use crate::{
    LokiProtoLabelPair, LokiProtoPushRequest, LokiProtoTimestamp, LokiTypedPushRequest, TenantId,
    current_unix_time_ns,
    distributor::router::{LokiProtoEntry, LokiProtoStream},
    normalize_loki_proto_push, normalize_loki_push,
};

#[test]
fn discovery_uses_entry_metadata_and_field_precedence_before_bounded_text() {
    let labels = Labels::from([("app".into(), "api".into())]);
    for (line, expected) in [
        ("level=debug error=none", "debug"),
        ("ſeverity=DBG error=none", "debug"),
        (r#"{"level":"INFO","error":"fatal"}"#, "info"),
        (r#"{"severity":"fatal","level":"info"}"#, "fatal"),
        (r#"{"level":"info","severity":"fatal"}"#, "info"),
        (r#"{"nested":{"severity":"warn"},"level":"info"}"#, "warn"),
        (r#"{"level":"debug","level":"fatal"}"#, "debug"),
        (r#"{"level":"\u0069nfo","message":"fatal"}"#, "fatal"),
        (r#"{"l\u0065vel":"DBG"}"#, "debug"),
        ("severity=WARN level=DBG", "debug"),
        ("info before error", "info"),
        ("misc:error terror error_code", "unknown"),
        ("plain", "unknown"),
        ("fatal: boom", "fatal"),
    ] {
        let mut metadata = Labels::new();
        discover_detected_level_label(&labels, &mut metadata, line, &Limits::default());
        assert!(
            metadata == Labels::from([("detected_level".into(), expected.into())]),
            "{line}"
        );
        assert!(labels == Labels::from([("app".into(), "api".into())]));
    }
    let mut existing = Labels::from([
        ("detected_level".into(), "WrN".into()),
        ("level".into(), "fatal".into()),
    ]);
    discover_detected_level_label(&labels, &mut existing, "level=debug", &Limits::default());
    assert!(existing["detected_level"] == "warn");
    let stream = Labels::from([("level".into(), "custom".into())]);
    let mut metadata = Labels::from([("level".into(), "fatal".into())]);
    discover_detected_level_label(&stream, &mut metadata, "level=debug", &Limits::default());
    assert!(metadata["detected_level"] == "custom");
}

#[test]
fn discovery_honors_disabled_custom_fields_json_depth_and_otlp_severity() {
    let labels = Labels::from([("app".into(), "api".into())]);
    let disabled = Limits {
        discover_log_levels: false,
        ..Limits::default()
    };
    let mut metadata = Labels::new();
    discover_detected_level_label(&labels, &mut metadata, "level=error", &disabled);
    assert!(metadata.is_empty());
    metadata.insert("detected_level".into(), "DBG".into());
    discover_detected_level_label(&labels, &mut metadata, "level=error", &disabled);
    assert!(metadata["detected_level"] == "DBG");
    for (fields, depth, line, expected) in [
        (
            vec!["priority".into()],
            2,
            "priority=DBG error=none",
            "debug",
        ),
        (
            vec!["priority".into()],
            2,
            r#"{"outer":{"inner":{"priority":"INF"}}}"#,
            "unknown",
        ),
        (
            vec!["priority".into()],
            0,
            r#"{"outer":{"inner":{"priority":"INF"}}}"#,
            "info",
        ),
        (
            vec!["priority".into()],
            -1,
            r#"{"outer":{"inner":{"priority":"INF"}}}"#,
            "info",
        ),
        (vec![], 2, "level=DBG error=none", "debug"),
    ] {
        let config = Limits {
            log_level_fields: fields,
            log_level_from_json_max_depth: depth,
            ..Limits::default()
        };
        let mut metadata = Labels::new();
        discover_detected_level_label(&labels, &mut metadata, line, &config);
        assert!(
            metadata["detected_level"] == expected,
            "{line} depth{depth}"
        );
    }
    for (number, expected) in [
        ("0", "unknown"),
        ("4", "trace"),
        ("8", "debug"),
        ("12", "info"),
        ("16", "warn"),
        ("20", "error"),
        ("24", "fatal"),
        ("25", "unknown"),
        ("bad", "info"),
    ] {
        let mut metadata = Labels::from([("severity_number".into(), number.into())]);
        discover_detected_level_label(&labels, &mut metadata, "level=debug", &Limits::default());
        assert!(metadata["detected_level"] == expected);
    }
}

#[test]
fn json_and_protobuf_push_keep_one_series_and_per_entry_level_metadata() {
    let time = current_unix_time_ns();
    let tenant = TenantId::new("level-ledger").unwrap();
    let payload: LokiTypedPushRequest = serde_json::from_value(serde_json::json!({"streams":[{
    "stream":{"app":"api"},"values":[
        [time.to_string(),"level=debug error=none"],
        [(time+1).to_string(),"level=info",{"detected_level":"WRN","trace_id":"abc"}],
        [(time+2).to_string(),"plain"]
    ]}]}))
    .unwrap();
    let json = normalize_loki_push(&tenant, payload, &Limits::default()).unwrap();
    let proto = LokiProtoPushRequest {
        streams: vec![LokiProtoStream {
            labels: r#"{app="api"}"#.into(),
            hash: 0,
            entries: [
                ("level=debug error=none", vec![]),
                (
                    "level=info",
                    vec![
                        LokiProtoLabelPair {
                            name: "detected_level".into(),
                            value: "WRN".into(),
                        },
                        LokiProtoLabelPair {
                            name: "trace_id".into(),
                            value: "abc".into(),
                        },
                    ],
                ),
                ("plain", vec![]),
            ]
            .into_iter()
            .enumerate()
            .map(|(offset, (line, metadata))| {
                let time = time + i64::try_from(offset).unwrap();
                LokiProtoEntry {
                    timestamp: Some(LokiProtoTimestamp {
                        seconds: time / 1_000_000_000,
                        nanos: i32::try_from(time % 1_000_000_000).unwrap(),
                    }),
                    line: line.into(),
                    structured_metadata: metadata,
                    parsed: vec![],
                }
            })
            .collect(),
        }],
    };
    let protobuf = normalize_loki_proto_push(&tenant, proto, &Limits::default()).unwrap();
    assert!(protobuf == json);
    for (row, level) in json.iter().zip(["debug", "warn", "unknown"]) {
        assert!(
            row.labels
                == Labels::from([
                    ("app".into(), "api".into()),
                    ("service_name".into(), "api".into())
                ])
        );
        assert!(row.structured_metadata["detected_level"] == level);
    }
    assert!(json[1].structured_metadata["trace_id"] == "abc");
}

#[test]
fn queries_do_not_recreate_missing_or_dropped_discovery_metadata() {
    let labels = Labels::from([("app".into(), "api".into())]);
    let detected = Labels::from([("detected_level".into(), "unknown".into())]);
    for (pipeline, metadata, expected) in [
        ("", Labels::new(), None),
        ("", detected.clone(), Some("unknown")),
        (" | drop detected_level", detected.clone(), None),
        (" | keep app", detected, None),
    ] {
        let stream = crate::parse_query(&format!(r#"{{app="api"}}{pipeline}"#)).unwrap();
        let (fields, entry) =
            crate::matching_loki_stream_entry(&stream, &labels, "plain", &metadata, 1, false)
                .unwrap();
        assert!(fields.get("detected_level").map(String::as_str) == expected);
        assert!(
            entry
                .structured_metadata
                .get("detected_level")
                .map(String::as_str)
                == expected
        );
        let metric =
            crate::parse_metric_query(&format!(r#"count_over_time({{app="api"}}{pipeline}[1m])"#))
                .unwrap();
        let (fields, _, _) =
            crate::querier::aggregate::record_matching::matching_loki_metric_sample(
                &metric, &labels, "plain", &metadata, 1,
            )
            .unwrap()
            .unwrap();
        assert!(fields.get("detected_level").map(String::as_str) == expected);
    }
}
