use super::*;

mod recording_rule_append_writes_materialized_records_to_sink {
    use super::*;

    #[tokio::test]
    pub(crate) async fn recording_rule_append_writes_materialized_records_to_sink() {
        let engine = up_api_engine(&[60_000]);
        let sink = RecordingSink::default();

        let appended = super::super::super::evaluate_and_append_recording_rule(
            &engine,
            &sink,
            &tenant_id("tenant-a"),
            "job:up:current",
            "up",
            &BTreeMap::new(),
            60_000,
        )
        .await
        .expect("recording rule append");

        assert2::assert!(appended == 1);
        assert2::assert!(sink.records() == vec![job_up_current_record(60_000)]);
    }
}

mod recording_rule_evaluation_materializes_float_samples_as_wal_records {
    use super::*;

    fn assert_recorded_job_sample(records: &[WalRecord], job: &str, value: f64) {
        check!(records.iter().any(|record| record.labels
            == vec![
                krabka_metrics::MetricLabelPair::new(
                    "__name__",
                    "job:http_requests:sum".to_string().into(),
                ),
                krabka_metrics::MetricLabelPair::new("job", job.to_string().into()),
            ]
            && matches!(
                record.payload,
                SamplePayload::Float {
                    timestamp_ms: 60_000,
                    value: actual,
                    start_timestamp_ms: None,
                } if actual == value
            )));
    }

    #[tokio::test]
    pub(crate) async fn recording_rule_evaluation_materializes_float_samples_as_wal_records() {
        let mut store = InMemoryMetricStore::new();
        store.push_float(
            "tenant-a",
            labels("http_requests_total", "api"),
            60_000,
            7.0,
        );
        store.push_float(
            "tenant-a",
            labels("http_requests_total", "web"),
            60_000,
            11.0,
        );
        let store = Arc::new(store);
        let engine = PromqlEngine::new(store, EngineOpts::default());

        let records = super::super::super::evaluate_recording_rule(
            &engine,
            &tenant_id("tenant-a"),
            "job:http_requests:sum",
            "sum by (job) (http_requests_total)",
            &BTreeMap::new(),
            60_000,
        )
        .await
        .expect("recording rule evaluation");

        check!(records.len() == 2);
        check!(records.iter().all(|record| record.tenant == "tenant-a"));
        assert_recorded_job_sample(&records, "api", 7.0);
        assert_recorded_job_sample(&records, "web", 11.0);
    }
}

mod recording_rule_fails_on_labelset_collision_after_rule_labels {
    use super::*;

    #[tokio::test]
    pub(crate) async fn recording_rule_fails_on_labelset_collision_after_rule_labels() {
        // Two series differ only by `job`; a rule label overwriting `job` to a
        // constant collapses them to the same labelset, which Prometheus rejects.
        let engine = two_job_up_engine();
        let mut rule_labels = BTreeMap::new();
        rule_labels.insert("job".to_string(), "merged".to_string());

        let result = super::super::super::evaluate_recording_rule(
            &engine,
            &tenant_id("tenant-a"),
            "job:up:current",
            "up",
            &rule_labels,
            60_000,
        )
        .await;

        assert2::assert!(let Err(super::super::super::PromqlError::Exec(_)) = &result);
        if let Err(super::super::super::PromqlError::Exec(message)) = result {
            assert2::assert!(message.contains("same labelset after applying rule labels"));
        }
    }
}

mod recording_rule_group_append_runs_recording_rules_and_skips_alerts {
    use super::*;

    #[tokio::test]
    pub(crate) async fn recording_rule_group_append_runs_recording_rules_and_skips_alerts() {
        let group: serde_yaml::Value = serde_yaml::from_str(
            r"
name: availability
interval: 30s
rules:
  - record: job:up:current
    expr: up
  - alert: InstanceDown
    expr: up == 0
",
        )
        .expect("rule group yaml");
        let engine = up_samples_engine(&[
            UpSample {
                job: "api",
                ts_ms: 60_000,
                value: 1.0,
            },
            UpSample {
                job: "web",
                ts_ms: 60_000,
                value: 0.0,
            },
        ]);
        let sink = RecordingSink::default();

        let appended = super::super::super::evaluate_and_append_recording_rule_group(
            &engine,
            &sink,
            &tenant_id("tenant-a"),
            &group,
            60_000,
        )
        .await
        .expect("recording rule group append");

        let records = sink.records();
        check!(appended == 2);
        check!(records.len() == 2);
        check!(records.iter().all(|record| record.labels[0]
            == krabka_metrics::MetricLabelPair::new(
                "__name__",
                "job:up:current".to_string().into()
            )));
    }
}

mod recording_rule_merges_rule_level_labels_into_every_series {
    use super::*;

    struct ExpectedLabel {
        name: &'static str,
        value: &'static str,
    }

    #[tokio::test]
    pub(crate) async fn recording_rule_merges_rule_level_labels_into_every_series() {
        let engine = two_job_up_engine();
        let mut rule_labels = BTreeMap::new();
        rule_labels.insert("env".to_string(), "prod".to_string());
        rule_labels.insert("team".to_string(), "sre".to_string());

        let records = super::super::super::evaluate_recording_rule(
            &engine,
            &tenant_id("tenant-a"),
            "job:up:current",
            "up",
            &rule_labels,
            60_000,
        )
        .await
        .expect("recording rule evaluation");

        assert2::assert!(records.len() == 2);
        for record in &records {
            for label in [
                ExpectedLabel {
                    name: "env",
                    value: "prod",
                },
                ExpectedLabel {
                    name: "team",
                    value: "sre",
                },
                ExpectedLabel {
                    name: "__name__",
                    value: "job:up:current",
                },
            ] {
                assert2::assert!(
                    record
                        .labels
                        .contains(&krabka_metrics::MetricLabelPair::new(
                            label.name,
                            label.value.to_string().into()
                        ))
                );
            }
        }
    }
}
