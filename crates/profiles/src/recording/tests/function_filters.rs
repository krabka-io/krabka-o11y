use assert2::assert;
use krabka_observability::server_security::ServerSecurity;

use super::*;
use crate::{
    blockbuilder::{PositionedProfileRecord, build_block_with_positions},
    query::{QuerierState, serve},
    wal::{WalLine, WalPosition},
};

fn function_filter(name: &str) -> pb::settings::v1::StacktraceFilter {
    pb::settings::v1::StacktraceFilter {
        function_name: Some(pb::settings::v1::StacktraceFilterFunctionName {
            function_name: name.into(),
            metric_type: 0,
        }),
    }
}

struct RecordingResponse {
    status: reqwest::StatusCode,
    body: serde_json::Value,
}

struct GenerationFilterCase {
    generation: i32,
    filter: serde_json::Value,
}

struct FilterTotalCase {
    filter: Option<pb::settings::v1::StacktraceFilter>,
    expected_total: f64,
}

struct FilterTotalsCase {
    filter: Option<pb::settings::v1::StacktraceFilter>,
    expected_api: f64,
    expected_idle: f64,
}

async fn request(
    bound: std::net::SocketAddr,
    tenant: &str,
    method: &str,
    body: serde_json::Value,
) -> RecordingResponse {
    let response = reqwest::Client::new()
        .post(format!(
            "http://{bound}/settings.v1.RecordingRulesService/{method}"
        ))
        .header("content-type", "application/json")
        .header("connect-protocol-version", "1")
        .header("x-scope-orgid", tenant)
        .json(&body)
        .send()
        .await
        .unwrap();
    RecordingResponse {
        status: response.status(),
        body: response.json().await.unwrap(),
    }
}

async fn assert_stored_rule(bound: std::net::SocketAddr, expected: &serde_json::Value) {
    struct StoredRuleCase {
        method: &'static str,
        request_body: serde_json::Value,
        expected_response: serde_json::Value,
    }
    for case in [
        StoredRuleCase {
            method: "GetRecordingRule",
            request_body: serde_json::json!({"id": "filtered"}),
            expected_response: serde_json::json!({"rule": expected.clone()}),
        },
        StoredRuleCase {
            method: "ListRecordingRules",
            request_body: serde_json::json!({}),
            expected_response: serde_json::json!({"rules": [expected.clone()]}),
        },
    ] {
        let response = request(bound, "tenant-a", case.method, case.request_body).await;
        assert!(
            response.status == reqwest::StatusCode::OK && response.body == case.expected_response
        );
    }
}

#[tokio::test]
async fn recording_function_filters_round_trip_updates_conflicts_and_tenants() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = Arc::new(QuerierState::empty().with_admin_store(store));
    let channel = tokio::sync::oneshot::channel();
    let bound = serve(
        "127.0.0.1:0".parse().unwrap(),
        state,
        &ServerSecurity::default(),
        async move {
            let _ = channel.1.await;
        },
    )
    .await
    .unwrap();
    let mut body = serde_json::json!({
        "id": "filtered", "metricName": "profiles_recorded_filtered_total",
        "matchers": [format!(r#"{{__profile_type__="{PROFILE_TYPE}"}}"#)],
        "groupBy": ["service_name"],
        "stacktraceFilter": {"functionName": {"functionName": "target", "metricType": "TOTAL"}}
    });
    let created = request(bound, "tenant-a", "UpsertRecordingRule", body.clone()).await;
    assert!(created.status == reqwest::StatusCode::OK);
    let mut expected = body.clone();
    expected
        .as_object_mut()
        .unwrap()
        .insert("profileType".into(), PROFILE_TYPE.into());
    expected
        .as_object_mut()
        .unwrap()
        .insert("generation".into(), "1".into());
    // Proto JSON omits the zero-valued TOTAL enum, but preserves the filter.
    expected["stacktraceFilter"]["functionName"]
        .as_object_mut()
        .unwrap()
        .remove("metricType");
    assert!(created.body == serde_json::json!({"rule": expected}));
    assert_stored_rule(bound, &expected).await;
    body["generation"] = "1".into();
    body["stacktraceFilter"]["functionName"]["functionName"] = "other".into();
    expected["generation"] = "2".into();
    expected["stacktraceFilter"]["functionName"]["functionName"] = "other".into();
    assert!(
        request(bound, "tenant-a", "UpsertRecordingRule", body.clone())
            .await
            .body
            == serde_json::json!({"rule": expected})
    );
    assert!(
        request(bound, "tenant-a", "UpsertRecordingRule", body.clone())
            .await
            .status
            == reqwest::StatusCode::CONFLICT
    );
    assert!(
        request(
            bound,
            "tenant-b",
            "GetRecordingRule",
            serde_json::json!({"id": "filtered"})
        )
        .await
        .status
            == reqwest::StatusCode::NOT_FOUND
    );
    assert!(
        request(
            bound,
            "tenant-b",
            "ListRecordingRules",
            serde_json::json!({})
        )
        .await
        .body
            == serde_json::json!({})
    );
    // Both an absent function and an empty function name are accepted filters.
    for case in [
        GenerationFilterCase {
            generation: 2,
            filter: serde_json::json!({}),
        },
        GenerationFilterCase {
            generation: 3,
            filter: serde_json::json!({"functionName": {}}),
        },
    ] {
        body["generation"] = case.generation.to_string().into();
        body["stacktraceFilter"] = case.filter.clone();
        let updated = request(bound, "tenant-a", "UpsertRecordingRule", body.clone()).await;
        assert!(updated.status == reqwest::StatusCode::OK);
        assert!(updated.body["rule"]["stacktraceFilter"] == case.filter);
    }
    channel.0.send(()).unwrap();
}

fn stack_fixture() -> ProfileRecord {
    let mut record = profile_record(7);
    record.symbols.strings.extend([
        "target".into(),
        "leaf".into(),
        "target[go.shape.int]".into(),
    ]);
    for name in [2, 3, 4] {
        record.symbols.functions.push(WalFunction {
            name,
            system_name: name,
            filename: 0,
            start_line: 0,
        });
    }
    record.symbols.locations.extend([
        WalLocation {
            address: 1,
            mapping_id: 0,
            lines: vec![WalLine {
                function_id: 1,
                line: 1,
            }],
        },
        WalLocation {
            address: 2,
            mapping_id: 0,
            lines: vec![
                WalLine {
                    function_id: 2,
                    line: 2,
                },
                WalLine {
                    function_id: 1,
                    line: 3,
                },
            ],
        },
        WalLocation {
            address: 3,
            mapping_id: 0,
            lines: vec![WalLine {
                function_id: 3,
                line: 4,
            }],
        },
    ]);
    // target is not the root; recursion repeats it twice in the first stack.
    record.samples[0].stacktrace_location_refs = vec![1, 1, 0];
    record.samples.extend([
        WalSample {
            stacktrace_location_refs: vec![2, 0],
            value: 11,
            ..record.samples[0].clone()
        },
        WalSample {
            stacktrace_location_refs: vec![0],
            value: 13,
            ..record.samples[0].clone()
        },
        WalSample {
            stacktrace_location_refs: vec![3, 0],
            value: 17,
            ..record.samples[0].clone()
        },
    ]);
    record
}

fn expected_series(id: &str, service: &str, value: f64) -> TimeSeries {
    TimeSeries {
        labels: [
            krabka_blockstore::LabelPair::new("__name__", "profiles_recorded_function_total"),
            krabka_blockstore::LabelPair::new("profiles_rule_id", id),
            krabka_blockstore::LabelPair::new("service_name", service),
            krabka_blockstore::LabelPair::new("team", "profiles"),
        ]
        .into_iter()
        .map(|label| Label {
            name: label.name,
            value: label.value,
        })
        .collect(),
        samples: vec![Sample {
            value,
            timestamp: 1,
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn distinct_profiles_at_one_timestamp_add_to_unfiltered_and_function_totals() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let mut recursive = stack_fixture();
    recursive.samples.truncate(1);
    let mut inline = stack_fixture();
    inline.samples = vec![inline.samples[1].clone()];
    assert!(recursive.labels == inline.labels);
    assert!(recursive.samples[0].timestamp_ns == inline.samples[0].timestamp_ns);
    let positions = [7, 11].map(|offset| WalPosition {
        partition: 0,
        offset,
        record_hash: [0x42; 32],
    });
    // Profiles have independent durable WAL identities, even when all series
    // labels and their timestamps coincide. Values are 7 and 11; the latter
    // has an inline target frame and the former has two recursive occurrences.
    assert!(positions[0].sample_identity(0) != positions[1].sample_identity(0));
    let labels = Labels::from_pairs(recursive.labels.iter().cloned());
    let mut index = ProfileIndex::new();
    index
        .add_series("tenant-a", labels.fingerprint(), &labels)
        .unwrap();
    let block = build_block_with_positions(
        &store,
        "tenant-a",
        0,
        &[
            PositionedProfileRecord::new(Some(positions[0]), recursive),
            PositionedProfileRecord::new(Some(positions[1]), inline),
        ],
        krabka_blockstore::OffsetRange::new(7, 11),
        &krabka_blockstore::ObjectStoreMetrics::unregistered(),
    )
    .await
    .unwrap()
    .remove(0);
    let rule = function_total_rule("same-time");
    for case in [
        FilterTotalCase {
            filter: None,
            expected_total: 18.0,
        },
        FilterTotalCase {
            filter: Some(function_filter("main")),
            expected_total: 18.0,
        },
        FilterTotalCase {
            filter: Some(function_filter("target")),
            expected_total: 18.0,
        },
        FilterTotalCase {
            filter: Some(function_filter("leaf")),
            expected_total: 11.0,
        },
        FilterTotalCase {
            filter: Some(function_filter("missing")),
            expected_total: 0.0,
        },
    ] {
        let rule = pb::settings::v1::RecordingRule {
            stacktrace_filter: case.filter,
            ..rule.clone()
        };
        assert!(
            evaluate_block(&store, &index, &block, &[rule])
                .await
                .unwrap()
                == vec![expected_series("same-time", "api", case.expected_total)]
        );
    }
}

#[tokio::test]
async fn recording_functions_match_any_inline_frame_once_and_preserve_zero_groups() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = stack_fixture();
    let mut idle = profile_record(5);
    idle.labels.last_mut().unwrap().value = "idle".into();
    let records = [first, idle];
    let block = build_block(
        &store,
        "tenant-a",
        0,
        &records,
        krabka_blockstore::OffsetRange::new(0, 0),
        &krabka_blockstore::ObjectStoreMetrics::unregistered(),
    )
    .await
    .unwrap()
    .remove(0);
    let mut index = ProfileIndex::new();
    for record in &records {
        let labels = Labels::from_pairs(record.labels.iter().cloned());
        index
            .add_series("tenant-a", labels.fingerprint(), &labels)
            .unwrap();
    }
    let mut secret = profile_record(23);
    secret.labels.last_mut().unwrap().value = "secret".into();
    let labels = Labels::from_pairs(secret.labels);
    index
        .add_series("tenant-b", labels.fingerprint(), &labels)
        .unwrap();
    let rule = pb::settings::v1::RecordingRule {
        profile_type: PROFILE_TYPE.into(),
        generation: 1,
        ..function_total_rule("filtered")
    };
    // Independently counted sample ledger: target 7+11; leaf 11; root 48;
    // missing 0. Repeated target and inline frames never multiply the value.
    for case in [
        FilterTotalsCase {
            filter: Some(function_filter("target")),
            expected_api: 18.0,
            expected_idle: 0.0,
        },
        FilterTotalsCase {
            filter: Some(function_filter("leaf")),
            expected_api: 11.0,
            expected_idle: 0.0,
        },
        FilterTotalsCase {
            filter: Some(function_filter("target[go.shape.int]")),
            expected_api: 17.0,
            expected_idle: 0.0,
        },
        FilterTotalsCase {
            filter: Some(function_filter("main")),
            expected_api: 48.0,
            expected_idle: 5.0,
        },
        FilterTotalsCase {
            filter: Some(function_filter("missing")),
            expected_api: 0.0,
            expected_idle: 0.0,
        },
        FilterTotalsCase {
            filter: Some(function_filter("Target")),
            expected_api: 0.0,
            expected_idle: 0.0,
        },
        FilterTotalsCase {
            filter: Some(function_filter("")),
            expected_api: 48.0,
            expected_idle: 5.0,
        },
        FilterTotalsCase {
            filter: Some(pb::settings::v1::StacktraceFilter::default()),
            expected_api: 48.0,
            expected_idle: 5.0,
        },
        FilterTotalsCase {
            filter: None,
            expected_api: 48.0,
            expected_idle: 5.0,
        },
    ] {
        let rule = pb::settings::v1::RecordingRule {
            stacktrace_filter: case.filter,
            ..rule.clone()
        };
        let actual = evaluate_block(&store, &index, &block, &[rule])
            .await
            .unwrap();
        assert!(
            actual
                == vec![
                    expected_series("filtered", "api", case.expected_api),
                    expected_series("filtered", "idle", case.expected_idle)
                ]
        );
    }
    let missing = pb::settings::v1::RecordingRule {
        matchers: vec![format!(
            r#"{{__profile_type__="{PROFILE_TYPE}",service_name="secret"}}"#
        )],
        stacktrace_filter: Some(function_filter("target")),
        ..rule
    };
    assert!(
        evaluate_block(&store, &index, &block, &[missing])
            .await
            .unwrap()
            .is_empty()
    );
}

/// A recording rule `id` that records `profiles_recorded_function_total` for
/// every `PROFILE_TYPE` series, grouped by `service_name`, with the external
/// label `team="profiles"`.
fn function_total_rule(id: &str) -> pb::settings::v1::RecordingRule {
    pb::settings::v1::RecordingRule {
        id: id.into(),
        metric_name: "profiles_recorded_function_total".into(),
        matchers: vec![format!(r#"{{__profile_type__="{PROFILE_TYPE}"}}"#)],
        group_by: vec!["service_name".into()],
        external_labels: vec![pb::types::v1::LabelPair {
            name: "team".into(),
            value: "profiles".into(),
        }],
        ..Default::default()
    }
}
