use super::*;

#[tokio::test]
async fn a_bad_rule_does_not_skip_the_next_rule() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let tenant = tenant_id("tenant-a");
    let group: serde_yaml::Value = serde_yaml::from_str(
        r#"
name: mixed
rules:
  - record: broken
  - record: good
    expr: up
"#,
    )
    .unwrap();
    let rules = BTreeMap::from([(
        "ns".to_string(),
        BTreeMap::from([("mixed".to_string(), group)]),
    )]);
    let wal = RecordingSink::default();
    let alerts = RecordingAlertmanagerSink::default();
    let states = RecordingRulerStateSink::default();
    let report = super::super::evaluate_and_persist_ruler_rule_set_with_report(
        &engine,
        (&wal, &alerts, &states),
        &mut super::super::RulerAlertState::default(),
        &tenant,
        &rules,
        60_000,
    )
    .await
    .unwrap();

    check!(report.evaluation.recording_records == 1);
    check!(wal.records().len() == 1);
    check!(report.rules.len() == 2);
    check!(!report.rules[0].last_error.is_empty());
    check!(report.rules[1].last_error.is_empty());
    check!(!report.groups[0].last_error.is_empty());
}

#[tokio::test]
async fn alert_rules_write_pending_firing_and_stale_synthetic_series() {
    let mut store = InMemoryMetricStore::new();
    store.push_float("tenant-a", labels("up", "api"), 60_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 120_000, 1.0);
    store.push_float("tenant-a", labels("up", "api"), 180_000, 0.0);
    let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
    let tenant = tenant_id("tenant-a");
    let group: serde_yaml::Value = serde_yaml::from_str(
        r#"
name: alerts
rules:
  - alert: InstanceUp
    expr: up == 1
    for: 1m
"#,
    )
    .unwrap();
    let wal = RecordingSink::default();
    let alerts = RecordingAlertmanagerSink::default();
    let states = RecordingRulerStateSink::default();
    let mut state = super::super::RulerAlertState::default();

    super::super::evaluate_and_persist_ruler_rule_group(
        &engine,
        (&wal, &alerts, &states),
        &mut state,
        &tenant,
        &group,
        60_000,
    )
    .await
    .unwrap();
    let active = wal.records();
    check!(active.len() == 2);
    check!(metric(&active[0]) == "ALERTS");
    check!(label(&active[0], "alertstate") == Some("pending"));
    check!(metric(&active[1]) == "ALERTS_FOR_STATE");
    check!(float_value(&active[0]).to_bits() == 1.0_f64.to_bits());
    check!(float_value(&active[1]).to_bits() == 60.0_f64.to_bits());

    super::super::evaluate_and_persist_ruler_rule_group(
        &engine,
        (&wal, &alerts, &states),
        &mut state,
        &tenant,
        &group,
        120_000,
    )
    .await
    .unwrap();
    let firing = wal.records();
    check!(firing.len() == 5);
    check!(label(&firing[3], "alertstate") == Some("firing"));
    check!(float_value(&firing[3]).to_bits() == 1.0_f64.to_bits());

    super::super::evaluate_and_persist_ruler_rule_group(
        &engine,
        (&wal, &alerts, &states),
        &mut state,
        &tenant,
        &group,
        180_000,
    )
    .await
    .unwrap();
    let records = wal.records();
    check!(records.len() == 7);
    for record in &records[5..] {
        check!(float_value(record).to_bits() == 0x7ff0_0000_0000_0002);
    }
}

fn metric(record: &WalRecord) -> &str {
    label(record, "__name__").unwrap_or("")
}

fn label<'a>(record: &'a WalRecord, name: &str) -> Option<&'a str> {
    record
        .labels
        .iter()
        .find_map(|(key, value)| (key == name).then_some(value.as_str()))
}

fn float_value(record: &WalRecord) -> f64 {
    let SamplePayload::Float { value, .. } = record.payload else {
        panic!("expected a float record")
    };
    value
}
