use super::prelude::*;

/// The patterns scan drops a row outside the query window, and the window
/// is half-open: a row exactly on the start counts, one exactly on the end
/// does not. Nothing had scanned a block through this endpoint, so the two
/// edges and the `||` joining them to the fingerprint test were all free.
#[tokio::test]
pub(crate) async fn a_patterns_scan_keeps_the_window_half_open() {
    use krabka_blockstore::{BlockKey, LogRow, TimeRange, series_fingerprint, write_log_block};

    let dir = tempfile::tempdir().expect("a temp dir");
    let mut labels = Labels::new();
    labels.insert("app".to_string(), "web".to_string());
    let fingerprint = series_fingerprint(&labels);

    let row = |timestamp_ns, line: &str| LogRow {
        series_fingerprint: fingerprint,
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
    };
    // Before the window, on its start, inside, on its end, and past it.
    // The rows outside carry a different line shape, so including one
    // shows up as a second pattern rather than merely a larger count --
    // swapping which rows are kept leaves the count alone.
    let key = BlockKey::new(
        "tenant-a",
        0,
        0,
        0,
        TimeRange::new(0, 100).expect("a valid range"),
    );
    let descriptor = write_log_block(
        dir.path(),
        &key,
        vec![
            row(5, "cache warmed"),
            row(10, "request served"),
            row(20, "request served"),
            row(30, "cache warmed"),
            row(40, "cache warmed"),
        ],
    )
    .expect("the block writes");

    let mut index = BlockIndex::default();
    index.insert(descriptor);
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels);
    let state = QuerierState::new(dir.path(), label_index, index);

    let mut headers = HeaderMap::new();
    headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));
    let value = super::execute_patterns_query(
        &state,
        &headers,
        Some("query=%7Bapp%3D%22web%22%7D&start=10&end=30&step=1h"),
    )
    .await
    .expect("the patterns query runs");

    // One line shape, one bucket, and only the two rows inside the window.
    let data = value["data"].as_array().expect("a data array");
    check!(data.len() == 1, "one pattern: {value}");
    let samples = data[0]["samples"].as_array().expect("a samples array");
    check!(samples.len() == 1, "one bucket: {value}");
    check!(
        samples[0][1] == 2,
        "the row on the start counts and the one on the end does not: {value}"
    );
}

#[test]
pub(crate) fn prometheus_rules_filters_parse_all_supported_axes() {
    let filters = PrometheusRulesFilters::parse(Some(
            "type=alert&exclude_alerts=true&time=10&rule_name=HighError&rule_group=api&file=rules.yaml&group_limit=2&group_next_token=next&match=%7Bapp%3D%22api%22%7D",
        ))
        .unwrap();
    assert_eq!(filters.rule_kind, Some("alerting"));
    check!(filters.exclude_alerts);
    check!(filters.evaluation_time.is_some());
    check!(filters.rule_names.contains("HighError"));
    check!(filters.rule_groups.contains("api"));
    check!(filters.files.contains("rules.yaml"));
    assert_eq!(filters.group_limit, Some(2));
    assert_eq!(filters.group_next_token.as_deref(), Some("next"));
    assert_eq!(filters.label_selectors.len(), 1);
    assert!(filters.has_rule_filter());

    let recording = PrometheusRulesFilters::parse(Some("type=record")).unwrap();
    assert_eq!(recording.rule_kind, Some("recording"));
    assert!(PrometheusRulesFilters::parse(Some("group_next_token=next")).is_err());
    assert!(
        !PrometheusRulesFilters::parse(Some(""))
            .unwrap()
            .has_rule_filter()
    );
}

#[test]
pub(crate) fn json_log_lines_collapse_to_a_single_templated_pattern() {
    // Two Krabka-shaped JSON log lines differing only by timestamp must mine
    // to one pattern with the timestamp templatized and every constant kept.
    let first = r#"{"timestamp":"2026-07-01T04:19:26.1238077Z","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}"#;
    let second = r#"{"timestamp":"2026-07-01T04:19:27.9981001Z","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}"#;
    assert_eq!(log_line_pattern(first), log_line_pattern(second));
    assert_eq!(
        log_line_pattern(first),
        r#"{"timestamp":"<_>","severity":"INFO","target":"krabka_broker::network::dispatch","message":"connection opened"}"#
    );
}

#[test]
pub(crate) fn json_log_pattern_templatizes_ids_and_numbers_but_keeps_constants() {
    let pattern = log_line_pattern(
        r#"{"severity":"INFO","request_id":"550e8400-e29b-41d4-a716-446655440000","trace":"4f3a9c2be18d4f6a5b7c9e0f1a2d3e4b","offset":12345,"sasl":false,"listener":"PLAIN"}"#,
    );
    assert_eq!(
        pattern,
        r#"{"severity":"INFO","request_id":"<_>","trace":"<_>","offset":"<_>","sasl":false,"listener":"PLAIN"}"#
    );
}

#[test]
pub(crate) fn json_message_field_templatizes_embedded_variables() {
    assert_eq!(
        log_line_pattern(r#"{"message":"processed request 550e8400e29b41d4a716 in 42ms"}"#),
        r#"{"message":"processed request <_> in <_>"}"#
    );
}

#[test]
pub(crate) fn non_json_lines_still_use_logfmt_mining() {
    assert_eq!(
        log_line_pattern("status=500 user=100 route=/checkout"),
        "status=<_> user=<_> route=/checkout"
    );
    // A line that merely starts with `{` but is not valid JSON falls back.
    assert_eq!(log_line_pattern("{not json ts=1"), "{not json ts=<_>");
}

#[test]
pub(crate) fn pattern_value_variable_classification() {
    // Variable: timestamps, floats, UUIDs, long hex ids, opaque tokens.
    assert!(pattern_value_is_variable("2026-07-01T04:19:26.1238077Z"));
    assert!(pattern_value_is_variable("42.5"));
    assert!(pattern_value_is_variable(
        "550e8400-e29b-41d4-a716-446655440000"
    ));
    assert!(pattern_value_is_variable(
        "4f3a9c2be18d4f6a5b7c9e0f1a2d3e4b"
    ));
    assert!(pattern_value_is_variable("AKIAIOSFODNN7EXAMPLE"));
    assert!(pattern_value_is_variable("\"2026-07-01T04:19:26Z\""));
    // Sole-reason coverage: each value below is variable via exactly one
    // classifier, so every branch of the `||` chain (and the shape checks
    // inside `is_uuid`/`is_hex_id`) is independently exercised.
    assert!(pattern_value_is_variable("-42.5")); // negative float: only the f64 parse
    assert!(pattern_value_is_variable(
        "f47ac10b-58cc-4372-a567-0e02b2c3d479" // letter-led UUID: only is_uuid
    ));
    assert!(pattern_value_is_variable("abcdefabcdefabcd")); // 16 hex letters, no digit: only is_hex_id
    // UUID *layout* but non-hex groups must not be accepted as a UUID (guards
    // the `len == n && all-hex` check inside is_uuid).
    assert!(!pattern_value_is_variable(
        "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
    ));
    // Constant: levels, module paths, file:line callers, short words.
    assert!(!pattern_value_is_variable("INFO"));
    assert!(!pattern_value_is_variable(
        "krabka_broker::network::dispatch"
    ));
    assert!(!pattern_value_is_variable("grpc_logging.go:66"));
    assert!(!pattern_value_is_variable("/cortex.Ingester/Push"));
    assert!(!pattern_value_is_variable("cafe"));
    assert!(!pattern_value_is_variable("authenticationToken"));
}

/// A negative range offset MUST render with a leading `-` sign, and a positive
/// offset MUST NOT. This pins the `offset_ns.0 < 0` sign branch in
/// `format_metric_range_selector`. A replacement of `<` with `==` would
/// drop the sign and emit a positive offset for a query that asked to look
/// *forward* in time. That `==` is never true here, because the outer guard
/// handles the `== 0` case.
#[test]
pub(crate) fn format_metric_range_selector_signs_negative_offset() {
    let negative = parse_metric_query("count_over_time({app=\"x\"}[5m] offset -3m)").unwrap();
    let positive = parse_metric_query("count_over_time({app=\"x\"}[5m] offset 3m)").unwrap();

    let negative_selector =
        format_metric_range_selector(&negative).expect("negative offset selector");
    let positive_selector =
        format_metric_range_selector(&positive).expect("positive offset selector");

    // The negative offset carries the sign; the positive one does not.
    check!(negative_selector.contains(" offset -"));
    check!(!positive_selector.contains(" offset -"));
    // The two differ ONLY by the sign character.
    check!(negative_selector == positive_selector.replace(" offset ", " offset -"));
}

/// `count_loki_metric_result_hot_tail_samples` counts matched ingester samples and
/// returns 0 when there is nothing to match: an `absent_over_time` query short-
/// circuits to 0, and a query whose response JSON has no `data.result`
/// array also yields 0. A replacement of the whole body with a constant
/// `1`, the mutant, would report a phantom ingester sample and skew the
/// store/ingester scan-stat split.
#[test]
pub(crate) fn count_loki_metric_result_hot_tail_samples_returns_zero_when_nothing_matches() {
    let plan = StreamPlan {
        tenant: "tenant".to_string(),
        time_range: TimeRange::new(0, 300_000_000_000).unwrap(),
        query: StreamQuery {
            matchers: Vec::new(),
            pipeline: Vec::new(),
        },
        fingerprints: BTreeSet::new(),
        blocks: Vec::new(),
    };
    let frontier = CompactionFrontier::new(0);
    let eval_range = TimeRange::new(0, 300_000_000_000).unwrap();
    let step_ns = 60_000_000_000;

    // `absent_over_time` short-circuits to 0 regardless of the response body.
    let absent_query = parse_metric_query("absent_over_time({app=\"x\"}[5m])").unwrap();
    let absent = count_loki_metric_result_hot_tail_samples(
        &json!({ "data": { "result": [] } }),
        &plan,
        &absent_query,
        &[],
        &frontier,
        (eval_range, step_ns),
        &[],
    );
    check!(absent == 0);

    // A non-absent query with an empty hot tail and a response lacking any
    // `data.result` array matches nothing and returns 0.
    let count_query = parse_metric_query("count_over_time({app=\"x\"}[5m])").unwrap();
    let none = count_loki_metric_result_hot_tail_samples(
        &json!({}),
        &plan,
        &count_query,
        &[],
        &frontier,
        (eval_range, step_ns),
        &[],
    );
    check!(none == 0);
}
