use assert2::assert;
use krabka_observability::server_security::ServerSecurity;

use super::*;
use crate::{
    blockbuilder::build_block_with_positions,
    query::{QuerierState, serve},
    wal::WalPosition,
};

fn function_filter(name: &str) -> pb::settings::v1::StacktraceFilter {
    pb::settings::v1::StacktraceFilter {
        function_name: Some(pb::settings::v1::StacktraceFilterFunctionName {
            function_name: name.into(),
            metric_type: 0,
        }),
    }
}

async fn request(
    bound: std::net::SocketAddr,
    tenant: &str,
    method: &str,
    body: serde_json::Value,
) -> (reqwest::StatusCode, serde_json::Value) {
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
    (response.status(), response.json().await.unwrap())
}

async fn assert_stored_rule(bound: std::net::SocketAddr, expected: &serde_json::Value) {
    for (method, expected_response) in [
        (
            "GetRecordingRule",
            serde_json::json!({"rule": expected.clone()}),
        ),
        (
            "ListRecordingRules",
            serde_json::json!({"rules": [expected.clone()]}),
        ),
    ] {
        assert!(
            request(
                bound,
                "tenant-a",
                method,
                if method == "GetRecordingRule" {
                    serde_json::json!({"id": "filtered"})
                } else {
                    serde_json::json!({})
                }
            )
            .await
                == (reqwest::StatusCode::OK, expected_response)
        );
    }
}

#[tokio::test]
async fn recording_function_filters_round_trip_updates_conflicts_and_tenants() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let state = Arc::new(QuerierState::empty().with_admin_store(store));
    let (shutdown, stopped) = tokio::sync::oneshot::channel();
    let bound = serve(
        "127.0.0.1:0".parse().unwrap(),
        state,
        &ServerSecurity::default(),
        async move {
            let _ = stopped.await;
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
    let (status, created) = request(bound, "tenant-a", "UpsertRecordingRule", body.clone()).await;
    assert!(status == reqwest::StatusCode::OK);
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
    assert!(created == serde_json::json!({"rule": expected}));
    assert_stored_rule(bound, &expected).await;
    body["generation"] = "1".into();
    body["stacktraceFilter"]["functionName"]["functionName"] = "other".into();
    expected["generation"] = "2".into();
    expected["stacktraceFilter"]["functionName"]["functionName"] = "other".into();
    assert!(
        request(bound, "tenant-a", "UpsertRecordingRule", body.clone()).await
            == (
                reqwest::StatusCode::OK,
                serde_json::json!({"rule": expected})
            )
    );
    assert!(
        request(bound, "tenant-a", "UpsertRecordingRule", body.clone())
            .await
            .0
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
        .0 == reqwest::StatusCode::NOT_FOUND
    );
    assert!(
        request(
            bound,
            "tenant-b",
            "ListRecordingRules",
            serde_json::json!({})
        )
        .await
            == (reqwest::StatusCode::OK, serde_json::json!({}))
    );
    // Both an absent function and an empty function name are accepted filters.
    for (generation, filter) in [
        (2, serde_json::json!({})),
        (3, serde_json::json!({"functionName": {}})),
    ] {
        body["generation"] = generation.to_string().into();
        body["stacktraceFilter"] = filter.clone();
        let (status, updated) =
            request(bound, "tenant-a", "UpsertRecordingRule", body.clone()).await;
        assert!(status == reqwest::StatusCode::OK);
        assert!(updated["rule"]["stacktraceFilter"] == filter);
    }
    shutdown.send(()).unwrap();
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
            lines: vec![(1, 1)],
        },
        WalLocation {
            address: 2,
            mapping_id: 0,
            lines: vec![(2, 2), (1, 3)],
        },
        WalLocation {
            address: 3,
            mapping_id: 0,
            lines: vec![(3, 4)],
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
            ("__name__", "profiles_recorded_function_total"),
            ("profiles_rule_id", id),
            ("service_name", service),
            ("team", "profiles"),
        ]
        .into_iter()
        .map(|(name, value)| Label {
            name: name.into(),
            value: value.into(),
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
        &[(positions[0], recursive), (positions[1], inline)],
        (7, 11),
        &krabka_blockstore::ObjectStoreMetrics::unregistered(),
    )
    .await
    .unwrap()
    .remove(0);
    let rule = function_total_rule("same-time");
    for (filter, total) in [
        (None, 18.0),
        (Some(function_filter("main")), 18.0),
        (Some(function_filter("target")), 18.0),
        (Some(function_filter("leaf")), 11.0),
        (Some(function_filter("missing")), 0.0),
    ] {
        let rule = pb::settings::v1::RecordingRule {
            stacktrace_filter: filter,
            ..rule.clone()
        };
        assert!(
            evaluate_block(&store, &index, &block, &[rule])
                .await
                .unwrap()
                == vec![expected_series("same-time", "api", total)]
        );
    }
}

#[tokio::test]
async fn recording_functions_match_any_inline_frame_once_and_preserve_zero_groups() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let first = stack_fixture();
    let mut idle = profile_record(5);
    idle.labels.last_mut().unwrap().1 = "idle".into();
    let records = [first, idle];
    let block = build_block(
        &store,
        "tenant-a",
        0,
        &records,
        (0, 0),
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
    secret.labels.last_mut().unwrap().1 = "secret".into();
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
    for (filter, expected_api, expected_idle) in [
        (Some(function_filter("target")), 18.0, 0.0),
        (Some(function_filter("leaf")), 11.0, 0.0),
        (Some(function_filter("target[go.shape.int]")), 17.0, 0.0),
        (Some(function_filter("main")), 48.0, 5.0),
        (Some(function_filter("missing")), 0.0, 0.0),
        (Some(function_filter("Target")), 0.0, 0.0),
        (Some(function_filter("")), 48.0, 5.0),
        (
            Some(pb::settings::v1::StacktraceFilter::default()),
            48.0,
            5.0,
        ),
        (None, 48.0, 5.0),
    ] {
        let rule = pb::settings::v1::RecordingRule {
            stacktrace_filter: filter,
            ..rule.clone()
        };
        let actual = evaluate_block(&store, &index, &block, &[rule])
            .await
            .unwrap();
        assert!(
            actual
                == vec![
                    expected_series("filtered", "api", expected_api),
                    expected_series("filtered", "idle", expected_idle)
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
