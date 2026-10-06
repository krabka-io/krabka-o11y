use super::*;

#[tokio::test]
async fn byte_alert_templates_and_replayed_state_preserve_series_identity() {
    let values = [
        vec![0xff],
        vec![0xfe],
        "�".as_bytes().to_vec(),
        b"__krabka_bytes_ff".to_vec(),
    ];
    let mut store = InMemoryMetricStore::new();
    for bytes in &values {
        let mut labels = crate::PromqlLabels::from_pairs([("__name__", "byte_input")]);
        labels.insert("raw", crate::PromqlString::from(bytes.clone()));
        store.push_float("tenant-a", labels, 60_000, 2.0);
    }
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let rule: serde_yaml::Value = serde_yaml::from_str(
        r#"
alert: ByteAlert
expr: byte_input > 0
for: 1m
labels:
  copied: '{{ $labels.raw }}'
  from_query: '{{ query (printf "byte_input{raw=%q}" $labels.raw) | first | label "raw" }}'
annotations:
  failed_query: '{{ query `absent(` }}'
  dependent: '{{ if eq (query (printf "byte_input{raw=%q}" $labels.raw) | first | value) 2.0 }}{{ query (printf "byte_input{raw=%q}" (query (printf "byte_input{raw=%q}" $labels.raw) | first | label "raw")) | first | value }}{{ else }}wrong{{ end }}'
  skipped: '{{ if false }}{{ query `absent(` }}{{ else }}skipped{{ end }}'
  clock: '{{ now }}'
  fresh_samples: '{{ eq (query (printf "byte_input{raw=%q}" $labels.raw) | first) (query (printf "byte_input{raw=%q}" $labels.raw) | first) }}'
  same_sample: '{{ $sample := query (printf "byte_input{raw=%q}" $labels.raw) | first }}{{ eq $sample $sample }}'
  scalar_query: '{{ query "7" | first | value }}'

"#,
    )
    .unwrap();
    let wal = RecordingSink::default();
    let alerts = RecordingAlertmanagerSink::default();
    let states = RecordingRulerStateSink::default();
    let mut state = super::super::RulerAlertState::default();
    let pending = super::super::alerting::evaluate_and_persist_alerting_rule_with_state_and_wal(
        &engine,
        (&wal, &alerts, &states),
        &mut state,
        &tenant_id("tenant-a"),
        &rule,
        60_000,
    )
    .await
    .unwrap();
    assert2::assert!(pending == 0 && alerts.alerts().is_empty());
    let persisted = states.alert_records();
    assert2::assert!(persisted.len() == 4);
    let mut expected = values
        .iter()
        .map(|bytes| {
            let mut labels = crate::PromqlLabels::from_pairs([("alertname", "ByteAlert")]);
            labels.insert("raw", crate::PromqlString::from(bytes.clone()));
            labels.insert("copied", crate::PromqlString::from(bytes.clone()));
            labels.insert("from_query", crate::PromqlString::from(bytes.clone()));
            super::super::RulerAlertStateRecord {
                tenant: "tenant-a".to_owned(),
                rule_id: "ByteAlert\nbyte_input > 0".to_owned(),
                labels,
                active_since_ms: Some(60_000),
                keep_firing_until_ms: None,
            }
        })
        .collect::<Vec<_>>();
    expected.sort_by(|left, right| left.labels.cmp(&right.labels));
    let mut actual = persisted.clone();
    actual.sort_by(|left, right| left.labels.cmp(&right.labels));
    assert2::assert!(actual == expected);
    assert2::assert!(wal.records().len() == 8);
    let restored = persisted
        .into_iter()
        .map(|record| {
            serde_json::from_slice::<super::super::RulerAlertStateRecord>(
                &serde_json::to_vec(&record).unwrap(),
            )
            .unwrap()
        })
        .collect::<Vec<_>>();
    let mut restarted = super::super::RulerAlertState::default();
    restarted.apply_records(restored);
    let firing = super::super::alerting::evaluate_and_persist_alerting_rule_with_state_and_wal(
        &engine,
        (&wal, &alerts, &states),
        &mut restarted,
        &tenant_id("tenant-a"),
        &rule,
        120_000,
    )
    .await
    .unwrap();
    assert2::assert!(firing == 4 && alerts.alerts().len() == 4);
    assert2::assert!(
        alerts
            .alerts()
            .iter()
            .all(|alert| alert.starts_at_ms == 60_000 && !alert.labels.contains_key("__name__"))
    );
    for alert in alerts.alerts() {
        let failed = &alert.annotations["failed_query"];
        assert2::assert!(
            failed.starts_with("<error expanding template: ") && failed.ends_with('>'),
            "{failed}"
        );
        for (key, expected) in [
            ("dependent", "2"),
            ("skipped", "skipped"),
            ("clock", "120"),
            ("fresh_samples", "false"),
            ("same_sample", "true"),
            ("scalar_query", "7"),
        ] {
            assert2::assert!(
                alert.annotations[key] == expected,
                "{key}: {:?}",
                alert.annotations[key]
            );
        }
    }
    let mut collision_rule = rule;
    collision_rule["labels"] = serde_yaml::from_str::<serde_yaml::Value>(
        "raw: collapsed\ncopied: collapsed\nfrom_query: collapsed",
    )
    .unwrap();
    let prior_wal = wal.records();
    // Stale NaNs are not reflexively equal under f64::PartialEq. Compare the
    // actual WAL wire bytes to retain exact payload bits, labels and ordering.
    let encoded = |records: &[WalRecord]| {
        records
            .iter()
            .map(|record| record.encode().unwrap())
            .collect::<Vec<_>>()
    };
    let prior_wal_bytes = encoded(&prior_wal);
    let mut drifted = prior_wal.clone();
    let stale = drifted
        .iter_mut()
        .find_map(|record| match &mut record.payload {
            SamplePayload::Float { value, .. } if value.is_nan() => Some(value),
            _ => None,
        })
        .expect("pending-to-firing transition must write a stale NaN");
    *stale = f64::from_bits(stale.to_bits() ^ 1);
    assert2::assert!(encoded(&drifted) != prior_wal_bytes);
    let prior_states = states.alert_records();
    let prior_alerts = alerts.alerts();
    let before = restarted.active_since_ms.clone();
    let before_keep_firing = restarted.keep_firing_until_ms.clone();
    let error = super::super::alerting::evaluate_and_persist_alerting_rule_with_state_and_wal(
        &engine,
        (&wal, &alerts, &states),
        &mut restarted,
        &tenant_id("tenant-a"),
        &collision_rule,
        120_000,
    )
    .await
    .unwrap_err();
    assert2::assert!(error.to_string().contains("same labelset"));
    assert2::assert!(restarted.active_since_ms == before);
    assert2::assert!(restarted.keep_firing_until_ms == before_keep_firing);
    assert2::assert!(
        encoded(&wal.records()) == prior_wal_bytes
            && states.alert_records() == prior_states
            && alerts.alerts() == prior_alerts
    );
}

#[tokio::test]
async fn histogram_alerts_expand_typed_values_and_queries_without_dropping_series() {
    let mut store = InMemoryMetricStore::new();
    store.push_histogram(
        "tenant-a",
        crate::PromqlLabels::from_pairs([("__name__", "native_input"), ("job", "api")]),
        60_000,
        krabka_metrics::NativeHistogram {
            schema: -53,
            is_float: true,
            reset_hint: krabka_metrics::ResetHint::Gauge,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 5.0,
            sum: 9.0,
            positive_spans: vec![krabka_metrics::BucketSpan {
                offset: 0,
                length: 3,
            }],
            positive_counts: vec![2.0, 0.0, 3.0],
            negative_spans: vec![],
            negative_counts: vec![],
            custom_values: Some(vec![1.0, 2.0]),
            start_timestamp_ms: None,
        },
    );
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let rule = serde_yaml::from_str(
        r#"
alert: NativeAlert
expr: native_input
labels:
  count: '{{ $value.Count }}'
annotations:
  typed: '{{ $value.Count }}/{{ $value.Sum }}/{{ $value.Schema }}/{{ $value.UsesCustomBuckets }}'
  buckets: '{{ $value.String }}'
  queried: '{{ query "native_input" | first | value }}'
  invalid_numeric: '{{ $value | humanize }}'
"#,
    )
    .unwrap();
    let alerts = RecordingAlertmanagerSink::default();
    let fired = super::super::evaluate_and_dispatch_alerting_rule(
        &engine,
        &alerts,
        &tenant_id("tenant-a"),
        &rule,
        60_000,
    )
    .await
    .unwrap();
    assert2::assert!(fired == 1 && alerts.alerts().len() == 1);
    let alert = alerts.alerts().pop().unwrap();
    assert2::assert!(alert.labels["count"] == "5" && alert.labels["job"] == "api");
    assert2::assert!(!alert.labels.contains_key("__name__"));
    assert2::assert!(alert.annotations["typed"] == "5/9/-53/true");
    assert2::assert!(alert.annotations["buckets"] == "{count:5, sum:9, [-Inf,1]:2, (2,+Inf]:3}");
    assert2::assert!(alert.annotations["queried"] == alert.annotations["buckets"]);
    assert2::assert!(
        alert.annotations["invalid_numeric"].starts_with("<error expanding template: ")
    );
}
