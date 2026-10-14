use assert2::assert;

use super::{Labels, Limits, discover_detected_level_label};
use crate::{
    LokiProtoLabelPair, LokiProtoPushRequest, LokiProtoTimestamp, LokiTypedPushRequest, TenantId,
    current_unix_time_ns,
    distributor::router::{LokiProtoEntry, LokiProtoStream},
    normalize_loki_proto_push, normalize_loki_push,
};

struct LevelTextCase {
    line: &'static str,
    expected: &'static str,
}

struct FieldDiscoveryCase {
    fields: Vec<String>,
    depth: i64,
    line: &'static str,
    expected: &'static str,
}

struct SeverityNumberCase {
    number: &'static str,
    expected: &'static str,
}

struct ProtobufEntryCase {
    line: &'static str,
    metadata: Vec<LokiProtoLabelPair>,
}

struct QueryMetadataCase {
    pipeline: &'static str,
    metadata: Labels,
    expected: Option<&'static str>,
}

struct LabelFixture {
    name: &'static str,
    value: &'static str,
}

fn label_fixture(entries: impl IntoIterator<Item = LabelFixture>) -> Labels {
    let mut labels = Labels::new();
    for entry in entries {
        labels.insert(entry.name.into(), entry.value.into());
    }
    labels
}

#[test]
fn discovery_uses_entry_metadata_and_field_precedence_before_bounded_text() {
    let labels = label_fixture([LabelFixture {
        name: "app",
        value: "api",
    }]);
    for case in [
        LevelTextCase {
            line: "level=debug error=none",
            expected: "debug",
        },
        LevelTextCase {
            line: "ſeverity=DBG error=none",
            expected: "debug",
        },
        LevelTextCase {
            line: r#"{"level":"INFO","error":"fatal"}"#,
            expected: "info",
        },
        LevelTextCase {
            line: r#"{"severity":"fatal","level":"info"}"#,
            expected: "fatal",
        },
        LevelTextCase {
            line: r#"{"level":"info","severity":"fatal"}"#,
            expected: "info",
        },
        LevelTextCase {
            line: r#"{"nested":{"severity":"warn"},"level":"info"}"#,
            expected: "warn",
        },
        LevelTextCase {
            line: r#"{"level":"debug","level":"fatal"}"#,
            expected: "debug",
        },
        LevelTextCase {
            line: r#"{"level":"\u0069nfo","message":"fatal"}"#,
            expected: "fatal",
        },
        LevelTextCase {
            line: r#"{"l\u0065vel":"DBG"}"#,
            expected: "debug",
        },
        LevelTextCase {
            line: "severity=WARN level=DBG",
            expected: "debug",
        },
        LevelTextCase {
            line: "info before error",
            expected: "info",
        },
        LevelTextCase {
            line: "misc:error terror error_code",
            expected: "unknown",
        },
        LevelTextCase {
            line: "plain",
            expected: "unknown",
        },
        LevelTextCase {
            line: "fatal: boom",
            expected: "fatal",
        },
    ] {
        let line = case.line;
        let expected = case.expected;
        let mut metadata = Labels::new();
        discover_detected_level_label(&labels, &mut metadata, line, &Limits::default());
        assert!(
            metadata
                == label_fixture([LabelFixture {
                    name: "detected_level",
                    value: expected
                }]),
            "{line}"
        );
        assert!(
            labels
                == label_fixture([LabelFixture {
                    name: "app",
                    value: "api"
                }])
        );
    }
    let mut existing = label_fixture([
        LabelFixture {
            name: "detected_level",
            value: "WrN",
        },
        LabelFixture {
            name: "level",
            value: "fatal",
        },
    ]);
    discover_detected_level_label(&labels, &mut existing, "level=debug", &Limits::default());
    assert!(existing["detected_level"] == "warn");
    let stream = label_fixture([LabelFixture {
        name: "level",
        value: "custom",
    }]);
    let mut metadata = label_fixture([LabelFixture {
        name: "level",
        value: "fatal",
    }]);
    discover_detected_level_label(&stream, &mut metadata, "level=debug", &Limits::default());
    assert!(metadata["detected_level"] == "custom");
}

#[test]
fn discovery_honors_disabled_custom_fields_json_depth_and_otlp_severity() {
    let labels = label_fixture([LabelFixture {
        name: "app",
        value: "api",
    }]);
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
    for case in [
        FieldDiscoveryCase {
            fields: vec!["priority".into()],
            depth: 2,
            line: "priority=DBG error=none",
            expected: "debug",
        },
        FieldDiscoveryCase {
            fields: vec!["priority".into()],
            depth: 2,
            line: r#"{"outer":{"inner":{"priority":"INF"}}}"#,
            expected: "unknown",
        },
        FieldDiscoveryCase {
            fields: vec!["priority".into()],
            depth: 0,
            line: r#"{"outer":{"inner":{"priority":"INF"}}}"#,
            expected: "info",
        },
        FieldDiscoveryCase {
            fields: vec!["priority".into()],
            depth: -1,
            line: r#"{"outer":{"inner":{"priority":"INF"}}}"#,
            expected: "info",
        },
        FieldDiscoveryCase {
            fields: vec![],
            depth: 2,
            line: "level=DBG error=none",
            expected: "debug",
        },
    ] {
        let fields = case.fields;
        let depth = case.depth;
        let line = case.line;
        let expected = case.expected;
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
    for case in [
        SeverityNumberCase {
            number: "0",
            expected: "unknown",
        },
        SeverityNumberCase {
            number: "4",
            expected: "trace",
        },
        SeverityNumberCase {
            number: "8",
            expected: "debug",
        },
        SeverityNumberCase {
            number: "12",
            expected: "info",
        },
        SeverityNumberCase {
            number: "16",
            expected: "warn",
        },
        SeverityNumberCase {
            number: "20",
            expected: "error",
        },
        SeverityNumberCase {
            number: "24",
            expected: "fatal",
        },
        SeverityNumberCase {
            number: "25",
            expected: "unknown",
        },
        SeverityNumberCase {
            number: "bad",
            expected: "info",
        },
    ] {
        let number = case.number;
        let expected = case.expected;
        let mut metadata = label_fixture([LabelFixture {
            name: "severity_number",
            value: number,
        }]);
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
                ProtobufEntryCase {
                    line: "level=debug error=none",
                    metadata: vec![],
                },
                ProtobufEntryCase {
                    line: "level=info",
                    metadata: vec![
                        LokiProtoLabelPair {
                            name: "detected_level".into(),
                            value: "WRN".into(),
                        },
                        LokiProtoLabelPair {
                            name: "trace_id".into(),
                            value: "abc".into(),
                        },
                    ],
                },
                ProtobufEntryCase {
                    line: "plain",
                    metadata: vec![],
                },
            ]
            .into_iter()
            .scan(0_i64, |offset, case| {
                let time = time + *offset;
                *offset += 1;
                Some(LokiProtoEntry {
                    timestamp: Some(LokiProtoTimestamp {
                        seconds: time / 1_000_000_000,
                        nanos: i32::try_from(time % 1_000_000_000).unwrap(),
                    }),
                    line: case.line.into(),
                    structured_metadata: case.metadata,
                    parsed: vec![],
                })
            })
            .collect(),
        }],
    };
    let protobuf = normalize_loki_proto_push(&tenant, proto, &Limits::default()).unwrap();
    assert!(protobuf == json);
    for pair in json.iter().zip(["debug", "warn", "unknown"]) {
        let row = pair.0;
        let level = pair.1;
        assert!(
            row.labels
                == label_fixture([
                    LabelFixture {
                        name: "app",
                        value: "api"
                    },
                    LabelFixture {
                        name: "service_name",
                        value: "api"
                    },
                ])
        );
        assert!(row.structured_metadata["detected_level"] == level);
    }
    assert!(json[1].structured_metadata["trace_id"] == "abc");
}

#[test]
fn queries_do_not_recreate_missing_or_dropped_discovery_metadata() {
    let labels = label_fixture([LabelFixture {
        name: "app",
        value: "api",
    }]);
    let detected = label_fixture([LabelFixture {
        name: "detected_level",
        value: "unknown",
    }]);
    for case in [
        QueryMetadataCase {
            pipeline: "",
            metadata: Labels::new(),
            expected: None,
        },
        QueryMetadataCase {
            pipeline: "",
            metadata: detected.clone(),
            expected: Some("unknown"),
        },
        QueryMetadataCase {
            pipeline: " | drop detected_level",
            metadata: detected.clone(),
            expected: None,
        },
        QueryMetadataCase {
            pipeline: " | keep app",
            metadata: detected,
            expected: None,
        },
    ] {
        let pipeline = case.pipeline;
        let metadata = case.metadata;
        let expected = case.expected;
        let stream = crate::parse_query(&format!(r#"{{app="api"}}{pipeline}"#)).unwrap();
        let matching =
            crate::matching_loki_stream_entry(&stream, &labels, "plain", &metadata, 1, false)
                .unwrap();
        let fields = matching.labels;
        let entry = matching.entry;
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
        let sample = crate::querier::aggregate::record_matching::matching_loki_metric_sample(
            &metric, &labels, "plain", &metadata, 1,
        )
        .unwrap()
        .unwrap();
        let fields = sample.labels;
        assert!(fields.get("detected_level").map(String::as_str) == expected);
    }
}
