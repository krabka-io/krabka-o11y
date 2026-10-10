#[path = "support/prometheus_template_functions.rs"]
mod prometheus_template_functions;

#[path = "support/template_functions.rs"]
mod template_functions;

use std::{
    collections::BTreeMap,
    time::{SystemTime, UNIX_EPOCH},
};

use assert2::check;
use krabka_logql::{
    ComparisonOp, DestinationLabel, DurationNanos, FieldFilter, FieldValue, JsonExpressionPath,
    JsonExtraction, JsonParserConfig, LabelFormat, LabelFormatAssignment, LabelMatcher,
    LabelSelection, LabelSelectionSet, LineFilter, LineFilterOp, LineFormat, LogfmtExtraction,
    LogfmtParserConfig, MatchOp, MetricQuery, OffsetNanos, ParserStage, PatternParser,
    PipelineEvaluation, PipelineStage, Quantile, QuantileDenominator, QuantileNumerator,
    RangeAggregation, RegexpParser, SourceLabel, StreamQuery, TemplateData, UnwrapExpression,
    VectorAggregation, VectorAggregationOp, VectorGrouping, parse_logql_expr,
    parse_metric_binary_arithmetic_query, parse_metric_binary_comparison_query,
    parse_metric_binary_set_query, parse_metric_label_join_query, parse_metric_label_replace_query,
    parse_metric_query, parse_metric_scalar_arithmetic_query, parse_metric_scalar_comparison_query,
    parse_query,
};

#[test]
fn parses_selector_with_all_matcher_ops() {
    let query =
        parse_query(r#"{app="api", env!="dev", pod=~"api-[0-9]+", zone!~"test|stage"}"#).unwrap();

    check!(
        query
            == StreamQuery {
                matchers: vec![
                    LabelMatcher::new("app", MatchOp::Equal, "api").unwrap(),
                    LabelMatcher::new("env", MatchOp::NotEqual, "dev").unwrap(),
                    LabelMatcher::new("pod", MatchOp::RegexEqual, "api-[0-9]+").unwrap(),
                    LabelMatcher::new("zone", MatchOp::RegexNotEqual, "test|stage").unwrap(),
                ],
                pipeline: vec![],
            }
    );
}

#[test]
fn empty_pipeline_preserves_complete_metadata_fields_and_matches_original_stream_labels() {
    let labels = BTreeMap::from([
        ("app".into(), "api".into()),
        ("collision".into(), "stream".into()),
        ("collision_extracted".into(), "base-suffix".into()),
        ("empty".into(), String::new()),
        ("__error__".into(), "StreamError".into()),
        ("__error_details__".into(), "stream-details".into()),
    ]);
    let metadata = BTreeMap::from([
        ("app".into(), "shadow".into()),
        ("collision".into(), "metadata".into()),
        ("collision_extracted".into(), "direct".into()),
        ("empty".into(), "metadata-empty".into()),
        ("only_metadata".into(), "present".into()),
        ("__error__".into(), String::new()),
        ("__error_details__".into(), "metadata-details".into()),
    ]);
    let expected_fields = BTreeMap::from([
        ("app".into(), "api".into()),
        ("app_extracted".into(), "shadow".into()),
        ("collision".into(), "stream".into()),
        ("collision_extracted".into(), "metadata".into()),
        ("collision_extracted_extracted".into(), "direct".into()),
        ("empty".into(), "metadata-empty".into()),
        ("only_metadata".into(), "present".into()),
        ("__error__".into(), String::new()),
        ("__error_details__".into(), "metadata-details".into()),
    ]);
    let line = "raw \0\u{1b}[31m\n東京";
    for (selector, matches) in [
        (r#"{app="api"}"#, true),
        (r#"{app="shadow"}"#, false),
        (r#"{only_metadata="present"}"#, false),
        (r#"{app="api",empty=""}"#, true),
        (r#"{app="api",empty="metadata-empty"}"#, false),
        (r#"{app="api",collision_extracted="base-suffix"}"#, true),
    ] {
        let query = parse_query(selector).unwrap();
        let expected = matches.then(|| PipelineEvaluation {
            fields: expected_fields.clone(),
            line: line.into(),
        });
        assert2::assert!(
            [
                query.evaluate_with_fields(&labels, line, &metadata),
                query.evaluate_with_fields_at(&labels, line, &metadata, 1_234_567_890),
            ] == [expected.clone(), expected],
            "{selector}"
        );
    }
}

#[test]
fn rejects_selectors_without_a_non_empty_compatible_matcher() {
    for query in [
        r"{}",
        r#"{env=""}"#,
        r#"{env!="prod"}"#,
        r#"{env=~".*"}"#,
        r#"{env!~"prod"}"#,
        r#"{env=~".*", zone!="prod"}"#,
    ] {
        check!(
            parse_query(query).is_err(),
            "query should be rejected: {query}"
        );
    }
}

#[test]
fn accepts_selectors_with_at_least_one_non_empty_compatible_matcher() {
    for query in [
        r#"{env="prod"}"#,
        r#"{env!=""}"#,
        r#"{env=~".+"}"#,
        r#"{env!~".*"}"#,
        r#"{app="api", env=~".*"}"#,
        r#"{app=~"api|worker", env!="prod"}"#,
    ] {
        check!(
            parse_query(query).is_ok(),
            "query should be accepted: {query}"
        );
    }
}

#[test]
fn parses_multiple_line_filters_in_order() {
    let query =
        parse_query(r#"{app="api"} |= "error" != "debug" |~ "status=[45][0-9][0-9]""#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::LineFilter(
                    LineFilter::new(LineFilterOp::Contains, "error").unwrap()
                ),
                PipelineStage::LineFilter(
                    LineFilter::new(LineFilterOp::NotContains, "debug").unwrap()
                ),
                PipelineStage::LineFilter(
                    LineFilter::new(LineFilterOp::Regex, "status=[45][0-9][0-9]").unwrap()
                ),
            ]
    );
}

#[test]
fn parses_decolorize_stage() {
    let query = parse_query(r#"{app="api"} | decolorize |= "status=500""#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Decolorize,
                PipelineStage::LineFilter(
                    LineFilter::new(LineFilterOp::Contains, "status=500").unwrap()
                ),
            ]
    );
}

#[test]
fn query_evaluator_applies_decolorize_before_later_line_filters() {
    let query = parse_query(r#"{app="api"} | decolorize |= "status=500" !~ `\x1b\[`"#).unwrap();
    let labels = app_api_labels();
    let evaluation = query
        .evaluate_with_fields(
            &labels,
            "\u{1b}[31mlevel=error status=500\u{1b}[0m",
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.line == "level=error status=500");
}

#[test]
fn query_evaluator_applies_pattern_line_filters() {
    let labels = app_api_labels();
    let line = r#"ts=2024-04-05T08:40:13Z caller=http.go:194 level=debug traceID=abc msg="POST /push.v1.PusherService/Push (200) 12ms""#;
    let query = parse_query(
        r#"{app="api"} |> `<_> caller=http.go:194 level=debug <_> msg="POST /push.v1.PusherService/Push <_>`"#,
    )
    .unwrap();

    check!(query.matches(&labels, line));
    check!(!query.matches(
        &labels,
        r#"ts=2024-04-05T08:40:13Z caller=http.go:194 level=info msg="POST /push.v1.PusherService/Push (200) 12ms""#
    ));

    let query = parse_query(
        r#"{app="api"} !> `<_> caller=http.go:194 level=debug <_> msg="POST /push.v1.PusherService/Push <_>`"#,
    )
    .unwrap();

    check!(!query.matches(&labels, line));
    check!(query.matches(
        &labels,
        r#"ts=2024-04-05T08:40:13Z caller=http.go:194 level=info msg="POST /push.v1.PusherService/Push (200) 12ms""#
    ));
}

#[test]
fn query_evaluator_ignores_logql_comments_outside_strings() {
    let query = parse_query(
        r#"
            {app="api"} # selector comment
            |= "error # literal"
            # disabled stage: != "error"
            | logfmt # parser comment
            | status >= 500 # filter comment
        "#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, "status=500 msg=\"error # literal\""));
    check!(!query.matches(&labels, "status=500 msg=\"error\""));
    check!(!query.matches(&labels, "status=200 msg=\"error # literal\""));
}

#[test]
fn decodes_common_escapes_in_quoted_strings() {
    let query =
        parse_query(r#"{app="api\nprod"} |= "line\tone" | logfmt | msg = "hello\"there""#).unwrap();
    let labels = BTreeMap::from([("app".to_string(), "api\nprod".to_string())]);

    check!(query.matches(&labels, "line\tone msg=\"hello\\\"there\""));
    check!(!query.matches(
        &BTreeMap::from([("app".to_string(), "api\\nprod".to_string())]),
        "line\tone msg=\"hello\\\"there\""
    ));
    check!(!query.matches(&labels, "line\\tone msg=\"hello\\\"there\""));
}

#[test]
fn query_evaluator_applies_matchers_and_pipeline() {
    let query = parse_query(r#"{app="api", env!="dev"} |= "error" !~ "debug""#).unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    check!(query.matches(&labels, "error status=500"));
    check!(!query.matches(&labels, "debug error status=500"));
    check!(!query.matches(
        &BTreeMap::from([("app".to_string(), "worker".to_string())]),
        "error"
    ));
}

#[test]
fn pipeline_stage_public_bool_helpers_reflect_stage_behavior() {
    let filter =
        PipelineStage::LineFilter(LineFilter::new(LineFilterOp::Contains, "error").unwrap());
    check!(filter.matches("level=error status=500"));
    check!(!filter.matches("level=info status=200"));

    let mut matching_line = "level=error status=500".to_string();
    let mut matching_fields = BTreeMap::new();
    check!(filter.apply(&mut matching_line, &mut matching_fields));

    let mut filtered_line = "level=info status=200".to_string();
    let mut filtered_fields = BTreeMap::new();
    check!(!filter.apply(&mut filtered_line, &mut filtered_fields));

    let parser = PipelineStage::Parser(ParserStage::Logfmt);
    let mut parsed_line = "status=500".to_string();
    let mut parsed_fields = BTreeMap::new();
    check!(parser.apply(&mut parsed_line, &mut parsed_fields));
    check!(parsed_fields.get("status") == Some(&"500".to_string()));

    check!(PipelineStage::Decolorize.mutates_line());
    check!(PipelineStage::Parser(ParserStage::Unpack).mutates_line());
    check!(PipelineStage::LineFormat(LineFormat::new("{{.msg}}").unwrap()).mutates_line());
    check!(!parser.mutates_line());
    check!(!filter.mutates_line());
}

#[test]
fn query_evaluator_treats_empty_compatible_regex_matcher_as_matching_absent_label() {
    let query = parse_query(r#"{app="api", env=~".*"}"#).unwrap();

    check!(query.matches(&app_api_labels(), "api line"));
    check!(query.matches(
        &BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("env".to_string(), "prod".to_string()),
        ]),
        "api line"
    ));
    check!(!query.matches(
        &BTreeMap::from([("app".to_string(), "worker".to_string())]),
        "worker line"
    ));
}

#[test]
fn query_evaluator_applies_negative_regex_label_matchers_to_present_labels() {
    let query = parse_query(r#"{app="api", zone!~"test|stage"}"#).unwrap();

    check!(query.matches(
        &BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("zone".to_string(), "prod".to_string()),
        ]),
        "api line"
    ));
    check!(!query.matches(
        &BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("zone".to_string(), "stage".to_string()),
        ]),
        "api line"
    ));
}

#[test]
fn query_evaluator_anchors_regex_label_matchers() {
    let query = parse_query(r#"{app=~"api|worker"}"#).unwrap();

    check!(query.matches(&app_api_labels(), "api line"));
    check!(query.matches(
        &BTreeMap::from([("app".to_string(), "worker".to_string())]),
        "worker line"
    ));
    check!(!query.matches(
        &BTreeMap::from([("app".to_string(), "myapi".to_string())]),
        "prefixed api line"
    ));
    check!(!query.matches(
        &BTreeMap::from([("app".to_string(), "api-v2".to_string())]),
        "suffixed api line"
    ));
}

#[test]
fn query_evaluator_applies_field_filter_to_original_labels() {
    let query = parse_query(r#"{app="api"} | env = "prod""#).unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    check!(query.matches(&labels, "api error"));
    check!(!query.matches(
        &BTreeMap::from([
            ("app".to_string(), "api".to_string()),
            ("env".to_string(), "dev".to_string()),
        ]),
        "api error"
    ));
}

#[test]
fn query_evaluator_suffixes_json_fields_that_collide_with_original_labels() {
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    let query =
        parse_query(r#"{app="api"} | json | env = "prod" | env_extracted = "dev""#).unwrap();
    check!(query.matches(&labels, r#"{"env":"dev"}"#));

    let query = parse_query(r#"{app="api"} | json | env = "dev""#).unwrap();
    check!(!query.matches(&labels, r#"{"env":"dev"}"#));
}

#[test]
fn query_evaluator_suffixes_logfmt_fields_that_collide_with_original_labels() {
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    let query =
        parse_query(r#"{app="api"} | logfmt | env = "prod" | env_extracted = "dev""#).unwrap();
    check!(query.matches(&labels, "env=dev"));

    let query = parse_query(r#"{app="api"} | logfmt | env = "dev""#).unwrap();
    check!(!query.matches(&labels, "env=dev"));
}

#[test]
fn parses_json_parser_stage_and_numeric_field_filter() {
    let query = parse_query(r#"{app="api"} | json | status >= 500"#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Json),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "status",
                    ComparisonOp::GreaterEqual,
                    FieldValue::Number(500.0)
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_json_parser_stage_and_field_filter() {
    let query = parse_query(r#"{app="api"} | json | status >= 500"#).unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r#"{"status":500,"message":"boom"}"#));
    check!(!query.matches(&labels, r#"{"status":200,"message":"ok"}"#));
    check!(!query.matches(&labels, "not json"));
}

#[test]
fn query_evaluator_exposes_json_parser_error_fields() {
    let labels = app_api_labels();

    let query = parse_query(r#"{app="api"} | json | __error__ = "JSONParserErr""#).unwrap();
    check!(query.matches(&labels, "not json"));
    check!(!query.matches(&labels, r#"{"status":500}"#));

    let query = parse_query(r#"{app="api"} | json | __error__ = """#).unwrap();
    check!(!query.matches(&labels, "not json"));
    check!(query.matches(&labels, r#"{"status":500}"#));
}

#[test]
fn parses_selected_json_parser_stage() {
    let query = parse_query(
        r#"{app="api"} | json first_server="servers[0]", ua="request.headers[\"User-Agent\"]" | ua = "Agent/1""#,
    )
    .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::JsonSelected(
                    JsonParserConfig::new(vec![
                        JsonExtraction::new(
                            DestinationLabel("first_server".into()),
                            JsonExpressionPath("servers[0]".into())
                        )
                        .unwrap(),
                        JsonExtraction::new(
                            DestinationLabel("ua".into()),
                            JsonExpressionPath(r#"request.headers["User-Agent"]"#.into())
                        )
                        .unwrap(),
                    ])
                    .unwrap()
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "ua",
                    ComparisonOp::Equal,
                    FieldValue::String("Agent/1".to_string())
                )),
            ]
    );
}

#[test]
fn query_evaluator_selected_json_extracts_paths_and_arrays() {
    let query = parse_query(
        r#"{app="api"} | json first_server="servers[0]", ua="request.headers[\"User-Agent\"]" | ua = "Agent/1""#,
    )
    .unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r#"{"servers":["10.0.0.1"],"request":{"headers":{"User-Agent":"Agent/1"},"method":"GET"},"status":500}"#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("first_server") == Some(&"10.0.0.1".to_string()));
    check!(evaluation.fields.get("ua") == Some(&"Agent/1".to_string()));
    check!(!evaluation.fields.contains_key("request_method"));
    check!(!evaluation.fields.contains_key("status"));
}

#[test]
fn parses_unpack_parser_stage() {
    let query = parse_query(r#"{app="api"} | unpack | pod = "pod-3223f""#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Unpack),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "pod",
                    ComparisonOp::Equal,
                    FieldValue::String("pod-3223f".to_string())
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_unpack_parser_and_replaced_line_filters() {
    let query = parse_query(
        r#"{app="api"} | unpack |= "original log message" != "container" | pod = "pod-3223f""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(
        &labels,
        r#"{"container":"myapp","pod":"pod-3223f","_entry":"original log message"}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"container":"myapp","pod":"pod-3223f","_entry":"container original log message"}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"container":"myapp","pod":"pod-3223f","_entry":"other log message"}"#
    ));
}

#[test]
fn parses_line_format_stage() {
    let query =
        parse_query(r#"{app="api"} | logfmt | line_format `{{.msg}} {{.status}}`"#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::LineFormat(LineFormat::new("{{.msg}} {{.status}}").unwrap()),
            ]
    );
}

#[test]
fn query_evaluator_applies_line_format_before_later_line_filters() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{.msg}} {{.status}}` |= "api error 500" != "status=""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r#"status=500 msg="api error""#));
    check!(!query.matches(&labels, r#"status=200 msg="api error""#));
}

#[test]
fn query_evaluator_line_format_can_reference_current_line() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{__line__}} method={{.method}}` |= "raw method=GET""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r"raw method=GET"));
    check!(!query.matches(&labels, r"raw method=POST"));
}

#[test]
fn query_evaluator_line_format_can_reference_current_timestamp() {
    let query = parse_query(
        r#"{app="api"} | line_format `{{ __timestamp__ | unixEpochNanos }} {{ __timestamp__ | unixEpochMillis }} {{ __timestamp__ | unixEpoch }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields_at(&labels, "raw", &BTreeMap::new(), 1_234_567_890)
        .unwrap();

    check!(evaluation.line == "1234567890 1234 1");
}

#[test]
fn query_evaluator_line_format_exposes_line_and_timestamp_aliases() {
    let query =
        parse_query(r#"{app="api"} | line_format `{{ line }} {{ timestamp | unixEpochNanos }}`"#)
            .unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields_at(&labels, "raw line", &BTreeMap::new(), 1_234_567_890)
        .unwrap();

    check!(evaluation.line == "raw line 1234567890");
}

#[test]
fn query_evaluator_matches_with_fields_at_uses_timestamped_pipeline_result() {
    let query = parse_query(
        r#"{app="api"} | line_format `{{ __timestamp__ | unixEpochMillis }}` |= "1234""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches_with_fields_at(&labels, "raw", &BTreeMap::new(), 1_234_000_000));
    check!(!query.matches_with_fields_at(&labels, "raw", &BTreeMap::new(), 9_999_000_000));
}

#[test]
fn query_evaluator_line_format_formats_current_timestamp_with_date_helper() {
    let query = parse_query(
        r#"{app="api"} | line_format `{{ __timestamp__ | date "2006-01-02T15:04:05.00Z-07:00" }} {{ __timestamp__ | date "2006-01-02" }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields_at(&labels, "raw", &BTreeMap::new(), 1_234_567_890)
        .unwrap();

    check!(evaluation.line == "1970-01-01T00:00:01.23Z+00:00 1970-01-01");
}

#[test]
fn query_evaluator_line_format_converts_epoch_strings_with_unix_to_time_helper() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .day | unixToTime | date "2006-01-02" }} {{ .seconds | unixToTime | date "2006-01-02T15:04:05" }} {{ .millis | unixToTime | unixEpoch }} {{ .micros | unixToTime | unixEpochMillis }} {{ .nanos | unixToTime | unixEpochNanos }}` |= "2023-01-16 2023-03-23T13:13:35 1679577215 1679577215000 1679577215000000000""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(
        &labels,
        r"day=19373 seconds=1679577215 millis=1679577215000 micros=1679577215000000 nanos=1679577215000000000 invalid=soon"
    ));
    check!(!query.matches(
        &labels,
        r"day=19373 seconds=1679587215 millis=1679577215000 micros=1679577215000000 nanos=1679577215000000000 invalid=soon"
    ));
}

#[test]
fn query_evaluator_line_format_parses_dates_with_to_date_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .day | toDate "2006-01-02" | unixEpoch }} {{ .stamp | toDateInZone "2006-01-02T15:04:05.999999999Z" "UTC" | unixEpochNanos }} {{ .day | toDateInZone "2006-01-02" "America/New_York" | unixEpoch }} {{ .bad | toDateInZone "2006-01-02" "UTC" | unixEpoch }}` |= "1635811200 1635867930123456789 1635825600 -62135596800""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(
        &labels,
        r"day=2021-11-02 stamp=2021-11-02T15:45:30.123456789Z bad=soon"
    ));
    check!(!query.matches(
        &labels,
        r"day=2021-11-03 stamp=2021-11-02T15:45:30.123456789Z bad=soon"
    ));
}

#[test]
fn query_evaluator_line_format_exposes_now_template_helper() {
    let query = parse_query(
        r#"{app="api"} | line_format `{{ now | unixEpochNanos }} {{ now | unixEpochMillis }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();
    let before = current_unix_epoch_nanos();

    let evaluation = query
        .evaluate_with_fields(&labels, "raw", &BTreeMap::new())
        .unwrap();

    let after = current_unix_epoch_nanos();
    let parts = evaluation.line.split_whitespace().collect::<Vec<_>>();
    check!(parts.len() == 2);
    let nanos = parts[0].parse::<u128>().unwrap();
    let millis = parts[1].parse::<u128>().unwrap();
    check!(nanos >= before);
    check!(nanos <= after);
    check!(millis >= before / 1_000_000);
    check!(millis <= after / 1_000_000);
}

#[test]
fn query_evaluator_line_format_ranges_over_from_json_arrays() {
    check_matches_only_rate_30_queries(
        r#"{app="api"} | json queries="queries" | line_format `{{ range $q := fromJson .queries }}{{ $q.query }}={{ $q.duration }};{{ end }}` |= "rate=30;sum=15;""#,
    );
}

#[test]
fn query_evaluator_line_format_ranges_with_current_dot_over_from_json_arrays() {
    check_matches_only_rate_30_queries(
        r#"{app="api"} | json queries="queries" | line_format `{{ range fromJson .queries }}{{ .query }}={{ .duration }};{{ end }}` |= "rate=30;sum=15;""#,
    );
}

#[test]
fn query_evaluator_line_format_ranges_can_reference_root_fields() {
    let output = ApiLine { query: r#"{app="api"} | logfmt | line_format `{{ range fromJson "[{\"method\":\"POST\"}]" }}inner={{ .method }} root={{ $.method }}{{ end }}`"#, line: r#"method=GET msg="request ok""# }.evaluate();

    check!(output.line == "inner=POST root=GET");
}

#[test]
fn query_evaluator_line_format_ranges_with_index_and_value_variables() {
    check_matches_only_rate_30_queries(
        r#"{app="api"} | json queries="queries" | line_format `{{ range $i, $q := fromJson .queries }}{{ $i }}:{{ $q.query }}={{ $q.duration }};{{ end }}` |= "0:rate=30;1:sum=15;""#,
    );
}

#[test]
fn query_evaluator_line_format_ranges_over_from_json_objects() {
    let query = parse_query(
        r#"{app="api"} | json durations="durations" | line_format `{{ range $name, $duration := fromJson .durations }}{{ $name }}={{ $duration }};{{ end }}` |= "rate=30;sum=15;""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r#"{"durations":{"rate":30,"sum":15}}"#));
    check!(!query.matches(&labels, r#"{"durations":{"rate":20,"sum":15}}"#));
}

#[test]
fn query_evaluator_line_format_uses_range_else_for_empty_from_json_arrays() {
    let query = parse_query(
        r#"{app="api"} | json queries="queries" | line_format `{{ range $q := fromJson .queries }}{{ $q.query }};{{ else }}none{{ end }}` |= "none""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r#"{"queries":[]}"#));
    check!(!query.matches(&labels, r#"{"queries":[{"query":"rate","duration":30}]}"#));
}

#[test]
fn query_evaluator_line_format_finds_outer_else_after_nested_control_blocks() {
    let format = LineFormat::new(
        r#"{{ if .outer }}{{ if .inner }}I{{ else }}i{{ end }}{{ range fromJson "[1]" }}R{{ else }}r{{ end }}{{ with .inner }}W{{ else }}w{{ end }}{{ else }}O{{ end }}"#,
    )
    .unwrap();
    let fields = BTreeMap::from([("outer".to_string(), "yes".to_string())]);

    check!(format.render("raw", &fields) == "iRw");
    check!(format.render("raw", &BTreeMap::new()) == "O");
}

#[test]
fn query_evaluator_line_format_applies_go_template_index_and_slice_helpers() {
    let query = parse_query(
        r#"{app="api"} | json payload="payload" | line_format `{{ index (fromJson .payload) "servers" 1 "name" }}|{{ index (fromJson .payload) "status" }}|{{ slice "abcdef" 1 4 }}|{{ slice (index (fromJson .payload) "servers") 0 1 }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let result = query
        .evaluate_with_fields(
            &labels,
            r#"{"payload":{"servers":[{"name":"api"},{"name":"worker"}],"status":200}}"#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(result.line == r"worker|200|bcd|[map[name:api]]");
}

#[test]
fn query_evaluator_line_format_applies_integer_math_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ add 3 2 5 }} {{ sub 5 2 }} {{ mul 5 2 3 }} {{ div 10 2 }} {{ mod 10 3 }} {{ max 1 2 3 }} {{ min 1 2 3 }} {{ .count | int | add 2 }} {{ .bad | int }}` |= "10 3 30 5 1 3 1 10 0""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r"count=8 bad=soon"));
    check!(!query.matches(&labels, r"count=7 bad=soon"));
}

#[test]
fn query_evaluator_line_format_applies_float_math_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ addf 3.5 2 5 }} {{ subf 5.5 2 1.5 }} {{ mulf 5.5 2 2.5 }} {{ divf 10 2 4 }} {{ maxf 1 2.5 3 }} {{ minf 1.5 2.5 3 }} {{ ceil 123.001 }} {{ floor 123.9999 }} {{ round 123.555555 3 }} {{ .ratio | float64 | addf 1.25 }} {{ .bad | float64 }}` |= "10.5 2 27.5 1.25 3 1.5 124 123 123.556 4.75 0""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r"ratio=3.5 bad=soon"));
    check!(!query.matches(&labels, r"ratio=2.5 bad=soon"));
}

#[test]
fn query_evaluator_line_format_applies_template_string_pipelines() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .path | replace "/" "_" | upper | trunc 6 }} {{ __line__ | lower }}` |= "_CHECK status=500 path=/checkout msg=api_error""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r"status=500 path=/checkout msg=API_ERROR"));
    check!(!query.matches(&labels, r"status=500 path=/health msg=API_ERROR"));
}

#[test]
fn query_evaluator_line_format_does_not_split_pipeline_inside_backtick_strings() {
    let format = LineFormat::new(r"{{ .missing | default `fallback|value` }}").unwrap();

    check!(format.render("raw", &BTreeMap::new()) == "fallback|value");
}

#[test]
fn query_evaluator_line_format_applies_additional_template_string_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .raw | trim | trimPrefix "/" | trimSuffix "/" | title }} {{ .raw | trimAll " /" }} {{ .path | substr 1 10 }} {{ .path | substr 5 -1 }} {{ .path | substr -1 4 }} {{ .query | urlencode }} {{ .encoded | urldecode }}` |= "Checkout checkout api/items items /api a%3D1+b%3Dtwo a=1 b=two""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(
        &labels,
        r#"raw=" /checkout/ " path=/api/items query="a=1 b=two" encoded="a%3D1+b%3Dtwo""#
    ));
    check!(!query.matches(
        &labels,
        r#"raw=" /health/ " path=/api/items query="a=1 b=two" encoded="a%3D1+b%3Dtwo""#
    ));
}

#[test]
fn query_evaluator_line_format_applies_base64_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .raw | b64enc }} {{ .encoded | b64dec }} {{ .invalid | b64dec }}` |= "aGVsbG8= hello ""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(
        &labels,
        r#"raw=hello encoded="aGVsbG8=" invalid="not-base64!""#
    ));
    check!(!query.matches(
        &labels,
        r#"raw=hello encoded="d29ybGQ=" invalid="not-base64!""#
    ));
}

#[test]
fn query_evaluator_line_format_applies_measurement_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ .latency | duration }} {{ .latency | duration_seconds }} {{ .size | bytes }}` |= "90 90 1.572864e+06""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r"latency=1m30s size=1.5MiB"));
    check!(!query.matches(&labels, r"latency=250ms size=1.5MiB"));
}

#[test]
fn query_evaluator_line_format_applies_printf_template_helper() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ printf "The IP address was %s" .remote_addr }}|{{ printf "%-5.5s" .request_method }}|{{ printf "%15.15s" .client_host }}|{{ .route | printf "[%s]" }}` |= "The IP address was 192.168.1.1|GET  |long-example.in|[/checkout]""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(
        &labels,
        r"remote_addr=192.168.1.1 request_method=GET client_host=long-example.internal route=/checkout"
    ));
    check!(!query.matches(
        &labels,
        r"remote_addr=192.168.1.1 request_method=POST client_host=long-example.internal route=/checkout"
    ));
}

#[test]
fn query_evaluator_line_format_applies_go_template_print_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ print "status=" 500 " method=" .method }}|{{ println "status" 500 }}|{{ urlquery "a=1 b=two&x=/api" }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let result = query
        .evaluate_with_fields(&labels, r"method=GET", &BTreeMap::new())
        .unwrap();

    check!(result.line == "status=500 method=GET|status 500\n|a%3D1+b%3Dtwo%26x%3D%2Fapi");
}

#[test]
fn query_evaluator_line_format_tokenizes_commands_with_spaced_arguments() {
    let format = LineFormat::new(r#"{{ printf "%s:%s"   "GET"   "200" }}"#).unwrap();

    check!(format.render("raw", &BTreeMap::new()) == "GET:200");
}

#[test]
fn query_evaluator_line_format_prints_spaces_between_adjacent_non_strings() {
    let format = LineFormat::new(
        r#"{{ print 1 2 }}|{{ print "a" 1 }}|{{ print (fromJson "1") (fromJson "2") }}|{{ println 1 2 }}"#,
    )
    .unwrap();

    check!(format.render("raw", &BTreeMap::new()) == "1 2|a1|1 2|1 2\n");
}

#[test]
fn query_evaluator_line_format_applies_go_template_escape_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ html .html }}|{{ js .script }}|{{ "<x>" | html }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let result = query
        .evaluate_with_fields(
            &labels,
            r#"html="<a&b>\"'" script="line\n\"quote\" <tag> &=""#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(
        result.line
            == r#"&lt;a&amp;b&gt;&#34;&#39;|line\u000A\"quote\" \u003Ctag\u003E \u0026\u003D|&lt;x&gt;"#
    );
}

#[test]
fn query_evaluator_line_format_applies_logical_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ contains "timeout" .msg }} {{ .path | hasPrefix "/api" }} {{ .path | hasSuffix "items" }} {{ .method | eq "GET" }}` |= "true true true true""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(
        &labels,
        r#"method=GET path=/api/items msg="request timeout""#
    ));
    check!(!query.matches(
        &labels,
        r#"method=POST path=/api/items msg="request timeout""#
    ));
    check!(!query.matches(&labels, r#"method=GET path=/health msg="request timeout""#));
}

#[test]
fn query_evaluator_line_format_applies_ne_template_helper() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ ne .method "POST" }} {{ .status | ne "500" }}` |= "true true""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r"method=GET status=200"));
    check!(!query.matches(&labels, r"method=POST status=200"));
    check!(!query.matches(&labels, r"method=GET status=500"));
}

#[test]
fn query_evaluator_line_format_applies_ordering_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ if gt (int .status) 499 }}server{{ else }}ok{{ end }} {{ ge (int .status) 500 }} {{ lt 2 10 }} {{ le (int .status) 500 }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let server = query
        .evaluate_with_fields(&labels, r"status=500", &BTreeMap::new())
        .unwrap();
    let ok = query
        .evaluate_with_fields(&labels, r"status=200", &BTreeMap::new())
        .unwrap();

    check!(server.line == "server true true true");
    check!(ok.line == "ok false true true");
}

#[test]
fn query_evaluator_line_format_applies_len_template_helper() {
    let query =
        parse_query(r#"{app="api"} | logfmt | line_format `len={{ len .msg }}` |= "len=15""#)
            .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r#"msg="template helper""#));
    check!(!query.matches(&labels, r#"msg="tiny""#));
}

#[test]
fn query_evaluator_line_format_applies_conditional_template_blocks() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ if contains "timeout" .msg }}timeout{{ else if eq "GET" .method }}read{{ else }}other{{ end }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let timeout = query
        .evaluate_with_fields(
            &labels,
            r#"method=POST msg="request timeout""#,
            &BTreeMap::new(),
        )
        .unwrap();
    let read = query
        .evaluate_with_fields(&labels, r#"method=GET msg="request ok""#, &BTreeMap::new())
        .unwrap();
    let other = query
        .evaluate_with_fields(&labels, r#"method=POST msg="request ok""#, &BTreeMap::new())
        .unwrap();

    check!(timeout.line == "timeout");
    check!(read.line == "read");
    check!(other.line == "other");
}

#[test]
fn query_evaluator_line_format_applies_if_template_variable_declarations() {
    let query = r#"{app="api"} | logfmt | line_format `{{ if $method := .method }}method={{ $method }}{{ else }}missing={{ $method }}{{ end }}`"#;
    let present = ApiLine {
        query,
        line: r#"method=GET msg="request ok""#,
    }
    .evaluate();
    let absent = ApiLine {
        query,
        line: r#"msg="request ok""#,
    }
    .evaluate();

    check!(present.line == "method=GET");
    check!(absent.line == "missing=");
}

#[test]
fn query_evaluator_line_format_applies_json_template_truthiness() {
    let query = parse_query(
        r#"{app="api"} | line_format `{{ if fromJson "[]" }}array{{ else }}empty-array{{ end }}|{{ if fromJson "{}" }}object{{ else }}empty-object{{ end }}|{{ if fromJson "null" }}null{{ else }}empty-null{{ end }}|{{ if fromJson "false" }}bool{{ else }}empty-bool{{ end }}|{{ if fromJson "0" }}number{{ else }}empty-number{{ end }}|{{ with fromJson "{\"method\":\"GET\"}" }}{{ .method }}{{ else }}missing{{ end }}|{{ not (fromJson "[]") }}|{{ or (fromJson "[]") (fromJson "{\"x\":1}") }}|{{ and (fromJson "{\"x\":1}") (fromJson "0") }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let result = query
        .evaluate_with_fields(&labels, "raw", &BTreeMap::new())
        .unwrap();

    check!(
        result.line
            == "empty-array|empty-object|empty-null|empty-bool|empty-number|GET|true|map[x:1]|0"
    );
}

#[test]
fn query_evaluator_line_format_applies_integer_and_json_scalar_truthiness() {
    let format = LineFormat::new(
        r#"{{ if 1 }}int-one{{ else }}int-zero{{ end }}|{{ if 0 }}bad{{ else }}int-zero{{ end }}|{{ if fromJson "1" }}json-one{{ else }}bad{{ end }}|{{ if fromJson "0" }}bad{{ else }}json-zero{{ end }}|{{ if fromJson "9223372036854775808" }}json-big{{ else }}bad{{ end }}|{{ if fromJson "0.25" }}json-fraction{{ else }}bad{{ end }}|{{ if fromJson "0.0" }}bad{{ else }}json-zero-fraction{{ end }}|{{ if fromJson "\"\"" }}bad{{ else }}json-empty-string{{ end }}|{{ if fromJson "\"ok\"" }}json-string{{ else }}bad{{ end }}|{{ if fromJson "[0]" }}json-array{{ else }}bad{{ end }}"#,
    )
    .unwrap();

    check!(
        format.render("raw", &BTreeMap::new())
            == "int-one|int-zero|json-one|json-zero|json-big|json-fraction|json-zero-fraction|json-empty-string|json-string|json-array"
    );
}

#[test]
fn query_evaluator_line_format_applies_with_template_blocks() {
    let query = r#"{app="api"} | logfmt | line_format `{{ with .method }}method={{ . }}{{ else }}missing{{ end }}`"#;
    let present = ApiLine {
        query,
        line: r#"method=GET msg="request ok""#,
    }
    .evaluate();
    let absent = ApiLine {
        query,
        line: r#"msg="request ok""#,
    }
    .evaluate();

    check!(present.line == "method=GET");
    check!(absent.line == "missing");
}

#[test]
fn query_evaluator_line_format_with_can_reference_root_fields() {
    let output = ApiLine { query: r#"{app="api"} | logfmt | line_format `{{ with fromJson "{\"method\":\"POST\"}" }}inner={{ .method }} root={{ $.method }}{{ end }}`"#, line: r#"method=GET msg="request ok""# }.evaluate();

    check!(output.line == "inner=POST root=GET");
}

#[test]
fn query_evaluator_line_format_applies_with_template_variable_declarations() {
    let query = r#"{app="api"} | logfmt | line_format `{{ with $method := .method }}dot={{ . }} var={{ $method }}{{ else }}missing={{ $method }}{{ end }}`"#;
    let present = ApiLine {
        query,
        line: r#"method=GET msg="request ok""#,
    }
    .evaluate();
    let absent = ApiLine {
        query,
        line: r#"msg="request ok""#,
    }
    .evaluate();

    check!(present.line == "dot=GET var=GET");
    check!(absent.line == "missing=");
}

#[test]
fn query_evaluator_line_format_applies_else_with_template_blocks() {
    let query = parse_query(
        r#"{app="api"} | line_format `{{ with .missing }}primary={{ . }}{{ else with fromJson "{\"fallback\":\"worker\"}" }}fallback={{ .fallback }}{{ else }}none{{ end }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let output = query
        .evaluate_with_fields(&labels, "raw", &BTreeMap::new())
        .unwrap();

    check!(output.line == "fallback=worker");
}

#[test]
fn query_evaluator_line_format_applies_template_variable_assignments() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ $method := .method }}{{ $status := .status }}{{ $method }} {{ $status | printf "status=%s" }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let output = query
        .evaluate_with_fields(&labels, r"method=GET status=500", &BTreeMap::new())
        .unwrap();

    check!(output.line == "GET status=500");
}

#[test]
fn query_evaluator_line_format_reassigns_template_variables() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ $status := .status }}{{ $status = printf "status=%s" $status }}{{ $status }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let output = query
        .evaluate_with_fields(&labels, r"status=500", &BTreeMap::new())
        .unwrap();

    check!(output.line == "status=500");
}

#[test]
fn query_evaluator_line_format_preserves_bare_values_ending_with_parenthesis() {
    check!(LineFormat::new("{{ status) }}").is_err());
    let format = LineFormat::new(r#"{{ "status)" }}"#).unwrap();
    check!(format.render("raw", &BTreeMap::new()) == "status)");
}

#[test]
fn query_evaluator_line_format_applies_template_trim_markers() {
    let query =
        parse_query(r#"{app="api"} | logfmt | line_format `left {{- .method -}} right`"#).unwrap();
    let labels = app_api_labels();

    let output = query
        .evaluate_with_fields(&labels, r#"method=GET msg="request ok""#, &BTreeMap::new())
        .unwrap();

    check!(output.line == "leftGETright");
}

#[test]
fn query_evaluator_line_format_does_not_trim_right_without_dash_marker() {
    let format = LineFormat::new("left {{ .method  }} right").unwrap();
    let fields = BTreeMap::from([("method".to_string(), "GET".to_string())]);

    check!(format.render("raw", &fields) == "left GET right");
}

#[test]
fn query_evaluator_line_format_skips_trailing_whitespace_after_right_trim_marker() {
    let format = LineFormat::new("{{ .method -}}   ").unwrap();
    let fields = BTreeMap::from([("method".to_string(), "GET".to_string())]);

    check!(format.render("raw", &fields) == "GET");
}

#[test]
fn query_evaluator_line_format_trims_control_body_before_left_trimmed_else() {
    let format = LineFormat::new("{{ if .method }}hit \n\t {{- else }}miss{{ end }}").unwrap();
    let fields = BTreeMap::from([("method".to_string(), "GET".to_string())]);

    check!(format.render("raw", &fields) == "hit");
    check!(format.render("raw", &BTreeMap::new()) == "miss");
}

#[test]
fn query_evaluator_line_format_ignores_template_comments() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `before{{/* hidden */}}after {{ .method }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let output = query
        .evaluate_with_fields(&labels, r#"method=GET msg="request ok""#, &BTreeMap::new())
        .unwrap();

    check!(output.line == "beforeafter GET");
}

#[test]
fn rejects_unclosed_or_unopened_template_comments() {
    check!(LineFormat::new("before{{/* hidden }}after").is_err());
    check!(LineFormat::new("before{{ hidden */}}after").is_err());
}

#[test]
fn rejects_top_level_template_control_actions() {
    for template in [
        "{{ else }}",
        "{{ else if .method }}",
        "{{ else with .method }}",
        "{{ end }}",
    ] {
        check!(
            LineFormat::new(template).is_err(),
            "template should be rejected: {template}"
        );
    }
}

#[test]
fn rejects_invalid_template_control_assignment_variables() {
    for template in [
        "{{ if $bad name := .method }}x{{ end }}",
        "{{ if $bad|name := .method }}x{{ end }}",
        "{{ with $bad.name := .method }}x{{ end }}",
        "{{ range $ := fromJson \"[]\" }}x{{ end }}",
        "{{ range $bad.name := fromJson \"[]\" }}x{{ end }}",
        "{{ $bad.name := .method }}",
    ] {
        check!(
            LineFormat::new(template).is_err(),
            "template should be rejected: {template}"
        );
    }
}

#[test]
fn query_evaluator_line_format_applies_boolean_template_combinators() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ if and (contains "timeout" .msg) (hasPrefix "/api" .path) }}route-timeout{{ else if or (eq "POST" .method) (not (hasSuffix "ok" .msg)) }}attention{{ else }}other{{ end }}`"#,
    )
    .unwrap();
    let labels = app_api_labels();

    let route_timeout = query
        .evaluate_with_fields(
            &labels,
            r"method=GET path=/api/items msg=request_timeout",
            &BTreeMap::new(),
        )
        .unwrap();
    let attention = query
        .evaluate_with_fields(
            &labels,
            r"method=POST path=/health msg=ok",
            &BTreeMap::new(),
        )
        .unwrap();
    let other = query
        .evaluate_with_fields(&labels, r"method=GET path=/health msg=ok", &BTreeMap::new())
        .unwrap();

    check!(route_timeout.line == "route-timeout");
    check!(attention.line == "attention");
    check!(other.line == "other");
}

#[test]
fn query_evaluator_line_format_applies_spacing_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ alignLeft 5 .short }}|{{ alignLeft 5 .long }}|{{ alignRight 5 .short }}|{{ alignRight 5 .long }}|{{ repeat 3 .mark }}|{{ .multi | indent 2 }}|{{ .multi | nindent 2 }}` |= "hi   |hello|   hi|world|xxx|  alpha\n  beta|\n  alpha\n  beta""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(
        &labels,
        r#"short=hi long=hello-world mark=x multi="alpha\nbeta""#
    ));
    check!(!query.matches(
        &labels,
        r#"short=hi long=hello-world mark=y multi="alpha\nbeta""#
    ));
}

#[test]
fn query_evaluator_line_format_applies_regex_template_helpers() {
    let query = parse_query(
        r#"{app="api"} | logfmt | line_format `{{ count "o" .word }}|{{ .word | count "o" }}|{{ regexReplaceAll "(f)(o+)" .word "${1}a" }}|{{ .word | regexReplaceAllLiteral "(f)(o+)" "${1}a" }}` |= "2|2|fa|${1}a""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r"word=foo"));
    check!(!query.matches(&labels, r"word=bar"));
}

#[test]
fn public_template_renderer_supports_prometheus_alert_variables_and_functions() {
    let template = LineFormat::new_prometheus(
        r#"{{ if $labels.job }}{{ title $labels.job }} {{ printf "%.1f" $value }} {{ humanize 1500 }} {{ humanizeDuration 90 }} {{ humanizePercentage $value }} {{ humanize1024 2048 }} {{ reReplaceAll "a.+" "service" $externalLabels.cluster }} {{ $externalURL }} {{ label "job" (first $samples) }}={{ value (first $samples) }}{{ end }}"#,
    )
    .unwrap();
    let sample = TemplateData::Sample(std::sync::Arc::new(BTreeMap::from([
        (
            "Labels".into(),
            TemplateData::Labels(BTreeMap::from([("job".into(), "worker".into())])),
        ),
        ("Value".into(), TemplateData::Float(7.0)),
    ])));
    let variables = BTreeMap::from([
        ("value".to_string(), TemplateData::Float(0.125)),
        (
            "labels".to_string(),
            TemplateData::Labels(BTreeMap::from([("job".into(), "api".into())])),
        ),
        (
            "externalLabels".to_string(),
            TemplateData::Labels(BTreeMap::from([("cluster".into(), "alpha".into())])),
        ),
        (
            "externalURL".to_string(),
            TemplateData::String("https://prom.example".into()),
        ),
        (
            "samples".to_string(),
            TemplateData::QueryResult(vec![sample].into()),
        ),
    ]);

    assert2::assert!(
        template
            .render_bytes_with_variables_and_queries(&variables, &BTreeMap::new())
            .unwrap()
            == b"Api 0.1 1.5k 1m 30s 12.5% 2ki service https://prom.example worker=7"
    );
}

#[test]
fn prometheus_humanizers_preserve_integer_trailing_zeros() {
    let template =
        LineFormat::new_prometheus("{{ humanize1024 1000 }} {{ humanizePercentage 12 }}").unwrap();

    assert2::assert!(template.render("", &BTreeMap::new()) == "1000 1200%");
}

#[test]
fn parses_label_format_stage_with_rename_and_template_assignments() {
    let query = parse_query(
        r#"{app="api"} | logfmt | label_format route=path, summary="{{.method}} {{.status}}""#,
    )
    .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::LabelFormat(
                    LabelFormat::new(vec![
                        LabelFormatAssignment::rename("route", "path").unwrap(),
                        LabelFormatAssignment::template("summary", "{{.method}} {{.status}}")
                            .unwrap(),
                    ])
                    .unwrap()
                ),
            ]
    );
}

#[test]
fn query_evaluator_applies_label_format_to_later_filters_and_labels() {
    let query = parse_query(
        r#"{app="api",env="prod"} | logfmt | label_format namespace=env, summary="{{.method}} {{.status}}" | namespace = "prod" | summary = "GET 500""#,
    )
    .unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
    ]);

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r"method=GET status=500 path=/api",
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("namespace") == Some(&"prod".to_string()));
    check!(evaluation.fields.get("summary") == Some(&"GET 500".to_string()));
    check!(!evaluation.fields.contains_key("env"));
}

#[test]
fn query_evaluator_label_format_applies_template_default_and_upper() {
    let query = parse_query(
        r#"{app="api"} | logfmt | label_format method=`{{ .method | default "get" | upper }}` | method = "GET""#,
    )
    .unwrap();
    let labels = app_api_labels();

    let missing_method = query
        .evaluate_with_fields(&labels, r"status=500 path=/checkout", &BTreeMap::new())
        .unwrap();
    let present_method = query.evaluate_with_fields(
        &labels,
        r"method=post status=500 path=/checkout",
        &BTreeMap::new(),
    );

    check!(missing_method.fields.get("method") == Some(&"GET".to_string()));
    check!(present_method.is_none());
}

#[test]
fn parses_parameterized_logfmt_parser_stage() {
    let query =
        parse_query(r#"{app="api"} | logfmt host, fwd_ip="fwd" | fwd_ip = "124.133.124.161""#)
            .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::LogfmtSelected(
                    LogfmtParserConfig::new(vec![
                        LogfmtExtraction::same("host").unwrap(),
                        LogfmtExtraction::rename(
                            DestinationLabel("fwd_ip".into()),
                            SourceLabel("fwd".into())
                        )
                        .unwrap(),
                    ])
                    .unwrap()
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "fwd_ip",
                    ComparisonOp::Equal,
                    FieldValue::String("124.133.124.161".to_string())
                )),
            ]
    );
}

#[test]
fn query_evaluator_parameterized_logfmt_extracts_only_requested_fields() {
    let query = parse_query(
        r#"{app="api"} | logfmt host, fwd_ip="fwd" | host = "grafana.net" | fwd_ip = "124.133.124.161""#,
    )
    .unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r#"at=info method=GET path=/ host=grafana.net fwd="124.133.124.161" status=200"#,
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("host") == Some(&"grafana.net".to_string()));
    check!(evaluation.fields.get("fwd_ip") == Some(&"124.133.124.161".to_string()));
    check!(!evaluation.fields.contains_key("method"));
    check!(!evaluation.fields.contains_key("status"));
}

#[test]
fn query_evaluator_parameterized_logfmt_keeps_missing_requested_fields_as_empty() {
    let evaluation = ApiLine {
        query: r#"{app="api"} | logfmt status, message="msg""#,
        line: r#"duration=25ms msg="api typed parser ok""#,
    }
    .evaluate();

    check!(evaluation.fields.get("status") == Some(&String::new()));
    check!(evaluation.fields.get("message") == Some(&"api typed parser ok".to_string()));
}

#[test]
fn query_evaluator_numeric_field_filter_keeps_invalid_present_values_as_label_filter_errors() {
    let evaluation = ApiLine {
        query: r#"{app="api"} | logfmt status | status >= 500"#,
        line: r#"duration=25ms msg="api typed parser ok""#,
    }
    .evaluate();

    check!(evaluation.fields.get("status") == Some(&String::new()));
    check!(evaluation.fields.get("__error__") == Some(&"LabelFilterErr".to_string()));
    check!(
        evaluation.fields.get("__error_details__")
            == Some(&r#"strconv.ParseFloat: parsing "": invalid syntax"#.to_string())
    );
}

#[test]
fn query_evaluator_logfmt_keep_empty_keeps_standalone_keys() {
    let query =
        parse_query(r#"{app="api"} | logfmt --keep-empty | empty = "" | host = "grafana.net""#)
            .unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, r"host=grafana.net empty", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("host") == Some(&"grafana.net".to_string()));
    check!(evaluation.fields.get("empty") == Some(&String::new()));
}

#[test]
fn query_evaluator_logfmt_skips_leading_whitespace() {
    let query = parse_query(r#"{app="api"} | logfmt | status = "204""#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, " \tstatus=204", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("status") == Some(&"204".to_string()));
}

#[test]
fn query_evaluator_logfmt_non_strict_skips_malformed_tokens() {
    let query = parse_query(r#"{app="api"} | logfmt | status = "204" | __error__ = """#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, r"=broken status=204", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("status") == Some(&"204".to_string()));
    check!(!evaluation.fields.contains_key("__error__"));
}

#[test]
fn query_evaluator_logfmt_decodes_quoted_value_escapes() {
    let query = parse_query(r#"{app="api"} | logfmt | msg = "hello \"api\"""#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, r#"msg="hello \"api\"""#, &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("msg") == Some(&r#"hello "api""#.to_string()));
}

#[test]
fn query_evaluator_field_filter_matches_missing_string_label_as_empty() {
    let query = parse_query(r#"{app="api"} | logfmt | empty = """#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, r"host=grafana.net", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("host") == Some(&"grafana.net".to_string()));
    check!(!evaluation.fields.contains_key("empty"));

    let non_empty_query = parse_query(r#"{app="api"} | logfmt | empty != """#).unwrap();
    check!(
        non_empty_query
            .evaluate_with_fields(&labels, r"host=grafana.net", &BTreeMap::new())
            .is_none()
    );
}

#[test]
fn query_evaluator_logfmt_strict_marks_malformed_tokens_as_errors() {
    let query =
        parse_query(r#"{app="api"} | logfmt --strict | __error__ = "LogfmtParserErr""#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, "host=grafana.net =broken", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("host") == Some(&"grafana.net".to_string()));
    check!(evaluation.fields.get("__error__") == Some(&"LogfmtParserErr".to_string()));
}

#[test]
fn query_evaluator_logfmt_strict_ignores_standalone_keys_without_keep_empty() {
    let query = parse_query(r#"{app="api"} | logfmt --strict | __error__ = """#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r"host=grafana.net empty status=204",
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("host") == Some(&"grafana.net".to_string()));
    check!(evaluation.fields.get("status") == Some(&"204".to_string()));
    check!(!evaluation.fields.contains_key("empty"));
    check!(!evaluation.fields.contains_key("__error__"));
}

#[test]
fn query_evaluator_logfmt_sanitizes_ansi_prefixed_field_names() {
    let query = parse_query(r#"{app="api"} | logfmt"#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            "\u{1b}[31mstatus=503 msg=\"colored parser error\"\u{1b}[0m",
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("_31mstatus") == Some(&"503".to_string()));
    check!(!evaluation.fields.contains_key("\u{1b}[31mstatus"));
}

#[test]
fn query_evaluator_logfmt_sanitizes_field_names_without_losing_valid_characters() {
    let evaluation = ApiLine {
        query: r#"{app="api"} | logfmt"#,
        line: "trace.id=abc span:id=def already_ok=ghi 9lives=cat a--b=two",
    }
    .evaluate();

    check_sanitized_field_names(&evaluation);
    check!(evaluation.fields.get("a_b") == Some(&"two".to_string()));
}

#[test]
fn query_evaluator_logfmt_strict_reports_loki_syntax_error_details() {
    let query =
        parse_query(r#"{app="api"} | logfmt --strict | __error__ = "LogfmtParserErr""#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, r#"status=500 msg="unterminated"#, &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("__error__") == Some(&"LogfmtParserErr".to_string()));
    check!(
        evaluation.fields.get("__error_details__")
            == Some(&"logfmt syntax error at pos 29 : unterminated quoted value".to_string())
    );
}

#[test]
fn query_evaluator_logfmt_non_strict_keep_empty_skips_malformed_quoted_values() {
    let query = parse_query(r#"{app="api"} | logfmt --keep-empty | __error__ = """#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, r#"status=500 msg="unterminated"#, &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("status") == Some(&"500".to_string()));
    check!(!evaluation.fields.contains_key("__error__"));
}

#[test]
fn parses_drop_and_keep_label_expression_stages() {
    let query = parse_query(
        r#"{app="api"} | logfmt | drop level, app=~"debug-.*" | keep method, status="500""#,
    )
    .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::DropLabels(
                    LabelSelectionSet::new(vec![
                        LabelSelection::name("level").unwrap(),
                        LabelSelection::regex("app", "debug-.*").unwrap(),
                    ])
                    .unwrap()
                ),
                PipelineStage::KeepLabels(
                    LabelSelectionSet::new(vec![
                        LabelSelection::name("method").unwrap(),
                        LabelSelection::equal("status", "500").unwrap(),
                    ])
                    .unwrap()
                ),
            ]
    );
}

#[test]
fn query_evaluator_applies_drop_and_keep_to_later_filters_and_labels() {
    let query = parse_query(
        r#"{app="api",env="prod"} | logfmt | drop env, level="debug" | keep app, method, status="500" | method = "GET""#,
    )
    .unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("env".to_string(), "prod".to_string()),
        ("__error__".to_string(), "ParserErr".to_string()),
    ]);

    let evaluation = query
        .evaluate_with_fields(
            &labels,
            r"method=GET status=500 level=debug path=/api",
            &BTreeMap::new(),
        )
        .unwrap();

    check!(evaluation.fields.get("app") == Some(&"api".to_string()));
    check!(evaluation.fields.get("method") == Some(&"GET".to_string()));
    check!(evaluation.fields.get("status") == Some(&"500".to_string()));
    check!(evaluation.fields.get("__error__") == Some(&"ParserErr".to_string()));
    check!(!evaluation.fields.contains_key("env"));
    check!(!evaluation.fields.contains_key("level"));
    check!(!evaluation.fields.contains_key("path"));
}

#[test]
fn query_evaluator_accepts_decimal_unwrap_samples() {
    let query = parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, "cost=1.5", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("__krabka_unwrap_sample_value__") == Some(&"1.5".to_string()));
    check!(!evaluation.fields.contains_key("__error__"));
    check!(!evaluation.fields.contains_key("__error_details__"));
}

#[test]
fn query_evaluator_accepts_signed_decimal_unwrap_samples() {
    let query = parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap();
    let labels = app_api_labels();

    let positive = query
        .evaluate_with_fields(&labels, "cost=+1.5", &BTreeMap::new())
        .unwrap();
    let evaluation = query
        .evaluate_with_fields(&labels, "cost=-1.5", &BTreeMap::new())
        .unwrap();

    check!(positive.fields.get("__krabka_unwrap_sample_value__") == Some(&"1.5".to_string()));
    check!(!positive.fields.contains_key("__error__"));
    check!(!positive.fields.contains_key("__error_details__"));
    check!(evaluation.fields.get("__krabka_unwrap_sample_value__") == Some(&"-1.5".to_string()));
    check!(!evaluation.fields.contains_key("__error__"));
    check!(!evaluation.fields.contains_key("__error_details__"));
}

#[test]
fn query_evaluator_rejects_repeated_sample_signs() {
    let query = parse_query(r#"{app="api"} | logfmt | unwrap cost"#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, "cost=++1.5", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("__error__") == Some(&"SampleExtractionErr".to_string()));
    check!(
        evaluation.fields.get("__error_details__")
            == Some(&"unwrap label `cost` cannot be converted".to_string())
    );
    check!(
        !evaluation
            .fields
            .contains_key("__krabka_unwrap_sample_value__")
    );
}

#[test]
fn query_evaluator_accepts_scientific_unwrap_samples() {
    let query = parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, "cost=-2.5e-1", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("__krabka_unwrap_sample_value__") == Some(&"-0.25".to_string()));
    check!(!evaluation.fields.contains_key("__error__"));
    check!(!evaluation.fields.contains_key("__error_details__"));
}

#[test]
fn query_evaluator_flattens_nested_json_fields() {
    let query =
        parse_query(r#"{app="api"} | json | request_method = "GET" | response_status >= 500"#)
            .unwrap();
    let labels = app_api_labels();

    check!(query.matches(
        &labels,
        r#"{"request":{"method":"GET"},"response":{"status":500}}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"request":{"method":"POST"},"response":{"status":500}}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"request":{"method":"GET"},"response":{"status":200}}"#
    ));
}

#[test]
fn query_evaluator_sanitizes_json_field_names_and_skips_arrays() {
    let query = parse_query(
        r#"{app="api"} | json | request_headers_User_Agent = "curl/7.68.0" | servers = "ignored""#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(!query.matches(
        &labels,
        r#"{"request":{"headers":{"User-Agent":"curl/7.68.0"}},"servers":["10.0.0.1"]}"#
    ));

    let query =
        parse_query(r#"{app="api"} | json | request_headers_User_Agent = "curl/7.68.0""#).unwrap();
    check!(query.matches(
        &labels,
        r#"{"request":{"headers":{"User-Agent":"curl/7.68.0"}},"servers":["10.0.0.1"]}"#
    ));
}

#[test]
fn query_evaluator_json_parser_exposes_sanitized_scalar_fields_only() {
    let evaluation = ApiLine { query: r#"{app="api"} | json"#, line: r#"{"trace.id":"abc","span:id":"def","already_ok":"ghi","9lives":"cat","servers":["10.0.0.1"]}"# }.evaluate();

    check_sanitized_field_names(&evaluation);
    check!(!evaluation.fields.contains_key("servers"));
}

#[test]
fn parses_pattern_parser_stage_and_field_filter() {
    let query = parse_query(
        r#"{app="api"} | pattern `<method> <path> (<status>) <duration>` | status >= 500"#,
    )
    .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Pattern(
                    PatternParser::new("<method> <path> (<status>) <duration>").unwrap()
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "status",
                    ComparisonOp::GreaterEqual,
                    FieldValue::Number(500.0)
                )),
            ]
    );
}

#[test]
fn pattern_regexp_and_line_format_accessors_return_source_text() {
    let pattern = PatternParser::new("prefix <method> suffix").unwrap();
    let regexp = RegexpParser::new(r"(?P<method>\w+) (?P<status>\d+)").unwrap();
    let format = LineFormat::new("{{.method}} {{.status}}").unwrap();
    let fields = BTreeMap::from([
        ("method".to_string(), "GET".to_string()),
        ("status".to_string(), "500".to_string()),
    ]);

    check!(pattern.pattern() == "prefix <method> suffix");
    check!(regexp.pattern() == r"(?P<method>\w+) (?P<status>\d+)");
    check!(format.template() == "{{.method}} {{.status}}");
    check!(format.render("raw", &fields) == "GET 500");
}

#[test]
fn query_evaluator_applies_pattern_parser_stage_and_field_filter() {
    check_matches_only_post_500(
        r#"{app="api"} | pattern `<method> <path> (<status>) <duration>` | method = "POST" | status >= 500"#,
    );
}

#[test]
fn query_evaluator_pattern_parser_captures_after_leading_literals() {
    let query = parse_query(
        r#"{app="api"} | pattern `prefix method=<method> status=<status>` | method = "POST" | status = "500""#,
    )
    .unwrap();
    let labels = app_api_labels();

    let evaluation = query
        .evaluate_with_fields(&labels, "prefix method=POST status=500", &BTreeMap::new())
        .unwrap();

    check!(evaluation.fields.get("method") == Some(&"POST".to_string()));
    check!(evaluation.fields.get("status") == Some(&"500".to_string()));
    check!(!query.matches(&labels, "prefix method=GET status=500"));
}

#[test]
fn query_evaluator_applies_unanchored_pattern_parser_and_collision_suffixes() {
    let query =
        parse_query(r#"{app="api",method="GET"} | pattern `<_> method=<method> status=<status>` | method = "GET" | method_extracted = "POST" | status = "500""#)
            .unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("method".to_string(), "GET".to_string()),
    ]);

    check!(query.matches(&labels, "prefix method=POST status=500"));
    check!(!query.matches(&labels, "prefix method=POST status=200"));
}

#[test]
fn query_evaluator_accepts_the_remainder_for_the_final_pattern_capture() {
    let query =
        parse_query(r#"{app="api"} | pattern `<method> <path>` | __error__ = "PatternParserErr""#)
            .unwrap();
    let labels = app_api_labels();

    check!(!query.matches(&labels, "too-few"));
    check!(!query.matches(&labels, "GET /ready"));

    let query = parse_query(r#"{app="api"} | pattern `<method> <path>` | __error__ = """#).unwrap();
    check!(query.matches(&labels, "too-few"));
    check!(query.matches(&labels, "GET /ready"));
}

#[test]
fn parses_regexp_parser_stage_and_field_filter() {
    let query = parse_query(
        r#"{app="api"} | regexp `(?P<method>\w+) (?P<path>[\w/]+) \((?P<status>\d+)\) (?P<duration>.*)` | status >= 500"#,
    )
    .unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Regexp(
                    RegexpParser::new(
                        r"(?P<method>\w+) (?P<path>[\w/]+) \((?P<status>\d+)\) (?P<duration>.*)"
                    )
                    .unwrap()
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "status",
                    ComparisonOp::GreaterEqual,
                    FieldValue::Number(500.0)
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_regexp_parser_stage_and_field_filter() {
    check_matches_only_post_500(
        r#"{app="api"} | regexp `(?P<method>\w+) (?P<path>[\w/]+) \((?P<status>\d+)\) (?P<duration>.*)` | method = "POST" | status >= 500"#,
    );
}

#[test]
fn query_evaluator_suffixes_regexp_captures_that_collide_with_original_labels() {
    let query =
        parse_query(r#"{app="api",method="GET"} | regexp `method=(?P<method>\w+) status=(?P<status>\d+)` | method = "GET" | method_extracted = "POST" | status = "500""#)
            .unwrap();
    let labels = BTreeMap::from([
        ("app".to_string(), "api".to_string()),
        ("method".to_string(), "GET".to_string()),
    ]);

    check!(query.matches(&labels, "prefix method=POST status=500 suffix"));
    check!(!query.matches(&labels, "prefix method=POST status=200 suffix"));
}

#[test]
fn regexp_parser_equality_requires_pattern_and_capture_names() {
    let word = RegexpParser::new(r"(?P<value>\w+)").unwrap();
    let word_again = RegexpParser::new(r"(?P<value>\w+)").unwrap();
    let digits = RegexpParser::new(r"(?P<value>\d+)").unwrap();
    let renamed = RegexpParser::new(r"(?P<other>\w+)").unwrap();

    check!(word == word_again);
    check!(word != digits);
    check!(word != renamed);
}

#[test]
fn query_evaluator_exposes_regexp_parser_error_fields() {
    let query =
        parse_query(r#"{app="api"} | regexp `(?P<method>\w+) (?P<path>[\w/]+)` | __error__ = "RegexpParserErr""#)
            .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, "too-few"));
    check!(!query.matches(&labels, "GET /ready"));

    let query =
        parse_query(r#"{app="api"} | regexp `(?P<method>\w+) (?P<path>[\w/]+)` | __error__ = """#)
            .unwrap();
    check!(!query.matches(&labels, "too-few"));
    check!(query.matches(&labels, "GET /ready"));
}

#[test]
fn rejects_regexp_parser_without_named_capture() {
    check!(parse_query(r#"{app="api"} | regexp `\w+ \S+`"#).is_err());
}

#[test]
fn parses_logfmt_parser_stage_and_string_field_filter() {
    let query = parse_query(r#"{app="api"} | logfmt | msg = "api error""#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "msg",
                    ComparisonOp::Equal,
                    FieldValue::String("api error".to_string())
                )),
            ]
    );
}

#[test]
fn parses_backtick_string_field_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | msg = `api error` | path =~ `/api/.+`"#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "msg",
                    ComparisonOp::Equal,
                    FieldValue::String("api error".to_string())
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "path",
                    ComparisonOp::RegexEqual,
                    FieldValue::String("/api/.+".to_string())
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_backtick_string_field_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | msg = `api error` | path =~ `/api/.+`"#).unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r#"msg="api error" path=/api/search"#));
    check!(!query.matches(&labels, r#"msg="api ok" path=/api/search"#));
    check!(!query.matches(&labels, r#"msg="api error" path=/ready"#));
}

#[test]
fn query_evaluator_applies_ip_line_filters_to_complete_ip_tokens() {
    let query = parse_query(r#"{app="api"} |= ip("192.168.4.0/24")"#).unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, "client=192.168.4.20 status=200"));
    check!(!query.matches(&labels, "client=192.168.5.20 status=200"));

    let query = parse_query(r#"{app="api"} != ip("3.180.71.3")"#).unwrap();

    check!(query.matches(&labels, "client=93.180.71.3 status=200"));
    check!(!query.matches(&labels, "client=3.180.71.3 status=200"));
}

#[test]
fn query_evaluator_applies_ip_label_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | remote_addr = ip("192.168.4.5-192.168.4.20")"#)
            .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, "remote_addr=192.168.4.12"));
    check!(!query.matches(&labels, "remote_addr=192.168.4.21"));

    let query = parse_query(r#"{app="api"} | logfmt | remote_addr != ip("192.168.4.2")"#).unwrap();

    check!(query.matches(&labels, "remote_addr=192.168.4.12"));
    check!(!query.matches(&labels, "remote_addr=192.168.4.2"));
}

#[test]
fn parses_regex_field_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | method=~"GET|POST" | path!~"/health.*""#).unwrap();

    check!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "method",
                    ComparisonOp::RegexEqual,
                    FieldValue::String("GET|POST".to_string())
                )),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "path",
                    ComparisonOp::RegexNotEqual,
                    FieldValue::String("/health.*".to_string())
                )),
            ]
    );
}

#[test]
fn query_evaluator_applies_logfmt_parser_stage_and_field_filters() {
    let query = parse_query(r#"{app="api"} | logfmt | status >= 500 | msg = "api error""#).unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, r#"status=500 msg="api error" trace=abc"#));
    check!(!query.matches(&labels, r#"status=200 msg="api ok" trace=abc"#));
    check!(!query.matches(&labels, "plain line"));
}

#[test]
fn parses_duration_and_bytes_field_filters() {
    check!(
        parse_query(r#"{app="api"} | logfmt | duration >= 20ms | bytes_consumed > 1.5MiB"#).is_ok()
    );
}

#[test]
fn query_evaluator_applies_duration_and_bytes_field_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | duration >= 20ms | bytes_consumed > 20MB"#).unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, "duration=25ms bytes_consumed=21MB"));
    check!(!query.matches(&labels, "duration=10ms bytes_consumed=21MB"));
    check!(!query.matches(&labels, "duration=25ms bytes_consumed=19MB"));
    check!(!query.matches(&labels, "duration=oops bytes_consumed=21MB"));
}

#[test]
fn query_evaluator_applies_and_or_field_filter_chains() {
    let query = parse_query(r#"{app="api"} | logfmt | status >= 500 or level = "warn""#).unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, "status=500 level=info"));
    check!(query.matches(&labels, "status=200 level=warn"));
    check!(!query.matches(&labels, "status=200 level=info"));

    let query =
        parse_query(r#"{app="api"} | logfmt | status >= 500 and path !~ "/health.*""#).unwrap();

    check!(query.matches(&labels, "status=500 path=/checkout"));
    check!(!query.matches(&labels, "status=500 path=/healthz"));
    check!(!query.matches(&labels, "status=200 path=/checkout"));
}

#[test]
fn query_evaluator_applies_parenthesized_field_filter_chains() {
    let query = parse_query(
        r#"{app="api"} | logfmt | duration >= 20ms or (method = "GET" and size <= 20KB)"#,
    )
    .unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, "duration=10ms method=GET size=10KB"));
    check!(query.matches(&labels, "duration=25ms method=POST size=40KB"));
    check!(!query.matches(&labels, "duration=10ms method=GET size=30KB"));

    let flat_query = parse_query(
        r#"{app="api"} | logfmt | duration >= 20ms or method = "GET" and size <= 20KB"#,
    )
    .unwrap();

    check!(!flat_query.matches(&labels, "duration=25ms method=POST size=40KB"));
}

#[test]
fn query_evaluator_treats_comma_and_adjacent_field_filters_as_and() {
    let labels = app_api_labels();

    let query =
        parse_query(r#"{app="api"} | logfmt | status >= 500, path !~ "/health.*""#).unwrap();
    check!(query.matches(&labels, "status=500 path=/checkout"));
    check!(!query.matches(&labels, "status=500 path=/healthz"));
    check!(!query.matches(&labels, "status=200 path=/checkout"));

    let query = parse_query(r#"{app="api"} | logfmt | status >= 500 path !~ "/health.*""#).unwrap();
    check!(query.matches(&labels, "status=500 path=/checkout"));
    check!(!query.matches(&labels, "status=500 path=/healthz"));
    check!(!query.matches(&labels, "status=200 path=/checkout"));
}

#[test]
fn query_evaluator_skips_unterminated_logfmt_quoted_field() {
    let labels = app_api_labels();

    let query = parse_query(r#"{app="api"} | logfmt | msg = "unterminated""#).unwrap();
    check!(!query.matches(&labels, r#"status=500 msg="unterminated"#));

    let query = parse_query(r#"{app="api"} | logfmt | status >= 500"#).unwrap();
    check!(query.matches(&labels, r#"status=500 msg="unterminated"#));
}

#[test]
fn query_evaluator_applies_regex_field_filters() {
    let query =
        parse_query(r#"{app="api"} | logfmt | method=~"GET|POST" | path!~"/health.*""#).unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, "method=GET path=/checkout"));
    check!(query.matches(&labels, "method=POST path=/checkout"));
    check!(!query.matches(&labels, "method=DELETE path=/checkout"));
    check!(!query.matches(&labels, "method=GET path=/healthz"));
}

#[test]
fn parses_count_over_time_metric_query() {
    let query = parse_metric_query(r#"count_over_time({app="api"} |= "error" [30s])"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_and_formats_recursive_logql_expressions() {
    let expression = krabka_logql::parse_logql_expr(
        r#"label_replace((count_over_time({app="api"}[30s]) + on(app) group_left(env) count_over_time({app="worker"}[30s])) * 2, "service", "$1", "app", "(.*)")"#,
    )
    .unwrap();

    check!(
        expression.to_string()
            == r#"label_replace((count_over_time({app="api"}[30s]) + on(app) group_left(env) count_over_time({app="worker"}[30s])) * 2,"service","$1","app","(.*)")"#
    );
}

#[test]
fn logql_expression_parser_obeys_operator_precedence() {
    let expression = krabka_logql::parse_logql_expr("vector(1) + 2 * 3 ^ 4 ^ 5").unwrap();
    check!(expression.to_string() == "vector(1) + 2 * 3 ^ 4 ^ 5");

    let left_associative = krabka_logql::parse_logql_expr("vector(1) - 2 - 3").unwrap();
    check!(left_associative.to_string() == "vector(1) - 2 - 3");

    let grouped = krabka_logql::parse_logql_expr("(vector(1) + 2) * 3").unwrap();
    check!(grouped.to_string() == "(vector(1) + 2) * 3");

    let set = krabka_logql::parse_logql_expr("vector(1) or vector(2) and vector(3)").unwrap();
    check!(set.to_string() == "vector(1) or vector(2) and vector(3)");
    let krabka_logql::LogqlExpr::Set { right, .. } = set else {
        panic!("the root expression should be a set operation");
    };
    check!(matches!(*right, krabka_logql::LogqlExpr::Set { .. }));
}

#[test]
fn recursive_logql_parser_handles_stream_filters_and_signed_scalars() {
    for query in [
        r#"{app="web"} != "debug""#,
        r#"{app="web"} | json | status >= 500"#,
    ] {
        let expression = krabka_logql::parse_logql_expr(query).unwrap();
        check!(matches!(expression, krabka_logql::LogqlExpr::Stream { .. }));
    }

    for query in ["vector(1) * -2", "vector(1) * 1e-3"] {
        let expression = krabka_logql::parse_logql_expr(query).unwrap();
        check!(expression.to_string() == query);
    }
}

#[test]
fn recursive_logql_parser_rejects_non_scalar_vector_arguments() {
    for query in ["vector(vector(1))", r#"vector(rate({app="web"}[5m]))"#] {
        check!(krabka_logql::parse_logql_expr(query).is_err());
    }
}

#[test]
fn parses_metric_range_selector_before_pipeline() {
    let query = parse_metric_query(
        r#"count_over_time({app="api"}[30s] |= "error" | logfmt | status >= 500)"#,
    )
    .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error" | logfmt | status >= 500"#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_label_replace_metric_query() {
    let query = parse_metric_label_replace_query(
        r#"label_replace(count_over_time({app="api"} |= "error" [30s]), "service", "$1-api", "app", "(.*)")"#,
    )
    .unwrap();

    check!(query.destination_label == "service");
    check!(query.replacement == "$1-api");
    check!(query.source_label == "app");
    check!(query.pattern == "(.*)");
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_label_join_metric_query() {
    let query = parse_metric_label_join_query(
        r#"label_join(count_over_time({app="api"} |= "error" [30s]), "joined", "/", "app", "env", "missing")"#,
    )
    .unwrap();

    check!(query.destination_label == "joined");
    check!(query.separator == "/");
    check!(query.source_labels == vec!["app", "env", "missing"]);
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_metric_scalar_comparison_query() {
    let query = parse_metric_scalar_comparison_query(
        r#"count_over_time({app="api"} |= "error" [30s]) > bool 1.5e0"#,
    )
    .unwrap();

    check!(query.op == ComparisonOp::Greater);
    check!(query.bool_modifier);
    check!(query.scalar == "1.5e0");
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_scalar_metric_comparison_query() {
    let query = parse_metric_scalar_comparison_query(
        r#"2 > bool count_over_time({app="api"} |= "error" [30s])"#,
    )
    .unwrap();

    check!(query.op == ComparisonOp::Greater);
    check!(query.bool_modifier);
    check!(query.scalar == "2");
    check!(query.scalar_on_left);
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_metric_scalar_arithmetic_query() {
    let query = parse_metric_scalar_arithmetic_query(
        r#"count_over_time({app="api"} |= "error" [30s]) * 2.5"#,
    )
    .unwrap();

    check!(query.op == krabka_logql::MetricScalarArithmeticOp::Multiply);
    check!(query.scalar == "2.5");
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_scalar_metric_arithmetic_query() {
    let query = parse_metric_scalar_arithmetic_query(
        r#"2 - count_over_time({app="api"} |= "error" [30s])"#,
    )
    .unwrap();

    check!(query.op == krabka_logql::MetricScalarArithmeticOp::Subtract);
    check!(query.scalar == "2");
    check!(query.scalar_on_left);
    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(query.query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_parenthesized_metric_expression_operands() {
    let metric_scalar =
        parse_metric_scalar_arithmetic_query(r#"(count_over_time({app="api"}[30s])) * 2"#).unwrap();
    check!(metric_scalar.op == krabka_logql::MetricScalarArithmeticOp::Multiply);
    check!(metric_scalar.query.aggregation == RangeAggregation::CountOverTime);

    let scalar_metric =
        parse_metric_scalar_comparison_query(r#"2 > bool ((count_over_time({app="api"}[30s])))"#)
            .unwrap();
    check!(scalar_metric.scalar_on_left);
    check!(scalar_metric.query.range_ns == DurationNanos(30_000_000_000));

    let binary = parse_metric_binary_arithmetic_query(
        r#"(count_over_time({app="api"}[30s])) / (count_over_time({app="worker"}[15s]))"#,
    )
    .unwrap();
    check!(binary.left.range_ns == DurationNanos(30_000_000_000));
    check!(binary.right.range_ns == DurationNanos(15_000_000_000));

    let set = parse_metric_binary_set_query(
        r#"(count_over_time({app="api"}[30s])) or (count_over_time({app="worker"}[15s]))"#,
    )
    .unwrap();
    check!(set.op == krabka_logql::MetricBinarySetOp::Or);
    check!(set.left.range_ns == DurationNanos(30_000_000_000));
    check!(set.right.range_ns == DurationNanos(15_000_000_000));

    let label_replace = parse_metric_label_replace_query(
        r#"label_replace((count_over_time({app="api"}[30s])), "service", "$1", "app", "(.*)")"#,
    )
    .unwrap();
    check!(label_replace.query.range_ns == DurationNanos(30_000_000_000));

    let label_join = parse_metric_label_join_query(
        r#"label_join((count_over_time({app="api"}[30s])), "service", "-", "app")"#,
    )
    .unwrap();
    check!(label_join.query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_parenthesized_metric_query_with_quoted_close_parenthesis() {
    let query =
        parse_metric_scalar_arithmetic_query(r#"(count_over_time({app="api"} |= ")" [30s])) * 2"#)
            .unwrap();

    check!(query.query.aggregation == RangeAggregation::CountOverTime);
    check!(
        query.query.stream.pipeline
            == vec![PipelineStage::LineFilter(
                LineFilter::new(LineFilterOp::Contains, ")").unwrap()
            )]
    );
}

#[test]
fn rejects_parenthesized_metric_operands_with_trailing_text() {
    check!(
        parse_metric_scalar_comparison_query(
            r#"2 > bool (count_over_time({app="api"}[30s])) trailing"#,
        )
        .is_err()
    );
}

#[test]
fn parses_metric_function_arguments_with_nested_commas_and_quotes() {
    let label_replace = parse_metric_label_replace_query(
        r#"label_replace(sum by (app) (count_over_time({app="api"} |= "contains,comma" [30s])), "service", "$1", "app", "api,(.*)")"#,
    )
    .unwrap();

    check!(label_replace.query.vector_aggregation.is_some());
    check!(label_replace.pattern == "api,(.*)");

    let label_join = parse_metric_label_join_query(
        r#"label_join(sum by (app) (count_over_time({app="api"}[30s])), "joined", ",", "app", "env")"#,
    )
    .unwrap();

    check!(label_join.query.vector_aggregation.is_some());
    check!(label_join.separator == ",");

    let label_replace = parse_metric_label_replace_query(
        r#"label_replace(sum by (app, env) (count_over_time({app="api"} |= ")" [30s])), "service", "$1", "app", "(.*)")"#,
    )
    .unwrap();

    check!(
        label_replace.query.vector_aggregation
            == Some(VectorAggregation {
                op: VectorAggregationOp::Sum,
                grouping: Some(VectorGrouping::By(vec![
                    "app".to_string(),
                    "env".to_string()
                ])),
            })
    );
}

#[test]
fn rejects_empty_metric_function_arguments() {
    check!(
        parse_metric_label_replace_query(r#"label_replace(, "service", "$1", "app", "(.*)")"#)
            .is_err()
    );
    check!(parse_metric_label_join_query(r#"label_join(, "joined", ",", "app")"#).is_err());
}

#[test]
fn parses_metric_binary_arithmetic_query() {
    let query = parse_metric_binary_arithmetic_query(
        r#"count_over_time({app="api"}[30s]) / count_over_time({app="api"} |= "error" [30s])"#,
    )
    .unwrap();

    check!(query.op == krabka_logql::MetricScalarArithmeticOp::Divide);
    check!(query.left.aggregation == RangeAggregation::CountOverTime);
    check!(query.left.range_ns == DurationNanos(30_000_000_000));
    check!(query.right.aggregation == RangeAggregation::CountOverTime);
    check!(query.right.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_metric_arithmetic_modulo_and_power_ops() {
    let modulo = parse_metric_scalar_arithmetic_query(
        r#"count_over_time({app="api"} |= "error" [30s]) % 2"#,
    )
    .unwrap();
    check!(modulo.op == krabka_logql::MetricScalarArithmeticOp::Modulo);

    let power = parse_metric_scalar_arithmetic_query(
        r#"count_over_time({app="api"} |= "error" [30s]) ^ 2"#,
    )
    .unwrap();
    check!(power.op == krabka_logql::MetricScalarArithmeticOp::Power);
}

#[test]
fn parses_metric_binary_arithmetic_matching_modifier() {
    let query = parse_metric_binary_arithmetic_query(
        r#"count_over_time({app="api"}[30s]) / ignoring(app) count_over_time({app="worker"}[30s])"#,
    )
    .unwrap();

    check!(
        query.matching
            == Some(krabka_logql::MetricVectorMatching::Ignoring {
                labels: vec!["app".to_string()],
                group: None,
            })
    );
    check!(query.op == krabka_logql::MetricScalarArithmeticOp::Divide);
    check!(query.left.aggregation == RangeAggregation::CountOverTime);
    check!(query.right.aggregation == RangeAggregation::CountOverTime);
}

#[test]
fn parses_metric_binary_arguments_with_nested_operator_characters() {
    let comparison = parse_metric_binary_comparison_query(
        r#"count_over_time({app="api"} | line_format `literal > inside` [30s]) > bool count_over_time({app="api"}[30s])"#,
    )
    .unwrap();
    check!(comparison.op == ComparisonOp::Greater);
    check!(comparison.left.range_ns == DurationNanos(30_000_000_000));

    let arithmetic = parse_metric_binary_arithmetic_query(
        r#"count_over_time({app="api"} |= "+" [30s]) + count_over_time({app="worker"}[15s])"#,
    )
    .unwrap();
    check!(arithmetic.op == krabka_logql::MetricScalarArithmeticOp::Add);
    check!(arithmetic.right.range_ns == DurationNanos(15_000_000_000));

    let set = parse_metric_binary_set_query(
        r#"count_over_time({app="origin"} |= "or" [30s]) or count_over_time({app="worker"}[15s])"#,
    )
    .unwrap();
    check!(set.op == krabka_logql::MetricBinarySetOp::Or);
    check!(set.left.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_metric_binary_arguments_with_quoted_parentheses_and_nested_keywords() {
    let comparison = parse_metric_binary_comparison_query(
        r#"count_over_time({app="api"} |= ")" [30s]) > bool count_over_time({app="worker"}[15s])"#,
    )
    .unwrap();
    check!(comparison.left.range_ns == DurationNanos(30_000_000_000));
    check!(comparison.right.range_ns == DurationNanos(15_000_000_000));

    let arithmetic = parse_metric_binary_arithmetic_query(
        r#"count_over_time({app="api"} |= ")" [30s] offset -5m) + count_over_time({app="worker"}[15s])"#,
    )
    .unwrap();
    check!(arithmetic.op == krabka_logql::MetricScalarArithmeticOp::Add);
    check!(arithmetic.left.offset_ns == OffsetNanos(-300_000_000_000));

    let set = parse_metric_binary_set_query(
        r#"sum by (or) (count_over_time({app="api"} |= ")" [30s])) and count_over_time({app="worker"}[15s])"#,
    )
    .unwrap();
    check!(set.op == krabka_logql::MetricBinarySetOp::And);
    check!(
        set.left.vector_aggregation
            == Some(VectorAggregation {
                op: VectorAggregationOp::Sum,
                grouping: Some(VectorGrouping::By(vec!["or".to_string()])),
            })
    );
}

#[test]
fn parses_metric_binary_arithmetic_group_modifier() {
    let query = parse_metric_binary_arithmetic_query(
        r#"sum by (app, env) (count_over_time({env="prod"}[30s])) / on(env) group_left(status) sum by (env, status) (count_over_time({env="prod"}[30s]))"#,
    )
    .unwrap();

    check!(
        query.matching
            == Some(krabka_logql::MetricVectorMatching::On {
                labels: vec!["env".to_string()],
                group: Some(krabka_logql::MetricVectorGroupModifier::Left(vec![
                    "status".to_string()
                ])),
            })
    );
    check!(query.op == krabka_logql::MetricScalarArithmeticOp::Divide);
    check!(query.left.vector_aggregation.is_some());
    check!(query.right.vector_aggregation.is_some());
}

#[test]
fn parses_metric_binary_comparison_query() {
    let query = parse_metric_binary_comparison_query(
        r#"count_over_time({app="api"}[30s]) > bool count_over_time({app="api"} |= "error" [30s])"#,
    )
    .unwrap();

    check!(query.op == ComparisonOp::Greater);
    check!(query.bool_modifier);
    check!(query.left.aggregation == RangeAggregation::CountOverTime);
    check!(query.left.range_ns == DurationNanos(30_000_000_000));
    check!(query.right.aggregation == RangeAggregation::CountOverTime);
    check!(query.right.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_metric_binary_set_query() {
    let query = parse_metric_binary_set_query(
        r#"count_over_time({app="api"}[30s]) and count_over_time({app="api"} |= "error" [30s])"#,
    )
    .unwrap();

    check!(query.op == krabka_logql::MetricBinarySetOp::And);
    check!(query.left.aggregation == RangeAggregation::CountOverTime);
    check!(query.left.range_ns == DurationNanos(30_000_000_000));
    check!(query.right.aggregation == RangeAggregation::CountOverTime);
    check!(query.right.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn rejects_group_modifiers_on_metric_set_operators() {
    check!(
        parse_metric_binary_set_query(
            r#"count_over_time({app="api"}[30s]) and on(app) group_left(status) count_over_time({app="worker"}[30s])"#,
        )
        .is_err()
    );
}

#[test]
fn parses_rate_metric_query() {
    let query = parse_metric_query(r#"rate({app="api"} |= "error" [2m])"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::Rate,
                vector_aggregation: None,
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
                range_ns: DurationNanos(120_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_rate_counter_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"rate_counter({app="api"} | logfmt | unwrap requests | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::RateCounter);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap requests | __error__ = """#).unwrap()
    );
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_bytes_over_time_metric_query() {
    let query = parse_metric_query(r#"bytes_over_time({app="api"} |= "error" [1h])"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::BytesOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
                range_ns: DurationNanos(3_600_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_bytes_rate_metric_query() {
    let query = parse_metric_query(r#"bytes_rate({app="api"} |= "error" [2m])"#).unwrap();

    check!(format!("{:?}", query.aggregation) == "BytesRate");
    check!(query.stream == parse_query(r#"{app="api"} |= "error""#).unwrap());
    check!(query.range_ns == DurationNanos(120_000_000_000));
}

#[test]
fn parses_sum_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::SumOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: StreamQuery {
                    matchers: vec![LabelMatcher::new("app", MatchOp::Equal, "api").unwrap()],
                    pipeline: vec![
                        PipelineStage::Parser(ParserStage::Logfmt),
                        PipelineStage::Unwrap(UnwrapExpression::new("cost").unwrap()),
                        PipelineStage::FieldFilter(FieldFilter::new(
                            "__error__",
                            ComparisonOp::Equal,
                            FieldValue::String(String::new())
                        )),
                    ],
                },
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_sum_over_time_unwrap_bytes_metric_query() {
    let query = parse_metric_query(
        r#"sum_over_time({app="api"} | logfmt | unwrap bytes(size) | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::SumOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: StreamQuery {
                    matchers: vec![LabelMatcher::new("app", MatchOp::Equal, "api").unwrap()],
                    pipeline: vec![
                        PipelineStage::Parser(ParserStage::Logfmt),
                        PipelineStage::Unwrap(UnwrapExpression::bytes("size").unwrap()),
                        PipelineStage::FieldFilter(FieldFilter::new(
                            "__error__",
                            ComparisonOp::Equal,
                            FieldValue::String(String::new())
                        )),
                    ],
                },
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_sum_over_time_unwrap_duration_conversion_metric_queries() {
    let expected = MetricQuery {
        aggregation: RangeAggregation::SumOverTime,
        vector_aggregation: None,
        range_grouping: None,
        stream: StreamQuery {
            matchers: vec![LabelMatcher::new("app", MatchOp::Equal, "api").unwrap()],
            pipeline: vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::Unwrap(UnwrapExpression::duration("latency").unwrap()),
                PipelineStage::FieldFilter(FieldFilter::new(
                    "__error__",
                    ComparisonOp::Equal,
                    FieldValue::String(String::new()),
                )),
            ],
        },
        range_ns: DurationNanos(30_000_000_000),
        offset_ns: OffsetNanos(0),
    };

    for source in [
        r#"sum_over_time({app="api"} | logfmt | unwrap duration(latency) | __error__ = "" [30s])"#,
        r#"sum_over_time({app="api"} | logfmt | unwrap duration_seconds(latency) | __error__ = "" [30s])"#,
    ] {
        check!(parse_metric_query(source).unwrap() == expected, "{source}");
    }
}

#[test]
fn a_conversion_name_without_parentheses_is_an_unwrap_label() {
    let query =
        parse_metric_query(r#"sum_over_time({app="api"} | logfmt | unwrap duration [30s])"#)
            .unwrap();

    assert2::assert!(
        query.stream.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::Unwrap(UnwrapExpression::new("duration").unwrap()),
            ]
    );
}

#[test]
fn parses_distinct_labels() {
    let query = parse_query(r#"{app="api"} | logfmt | distinct method, status"#).unwrap();
    assert2::assert!(
        query.pipeline
            == vec![
                PipelineStage::Parser(ParserStage::Logfmt),
                PipelineStage::Distinct(vec!["method".into(), "status".into()]),
            ]
    );
}

#[test]
fn parses_avg_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"avg_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::AvgOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_avg_over_time_unwrap_metric_query_with_range_grouping() {
    let query = parse_metric_query(
        r#"avg_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s]) by (app)"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::AvgOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == DurationNanos(30_000_000_000));
    check!(query.range_grouping == Some(VectorGrouping::By(vec!["app".to_string()])));
}

#[test]
fn vector_aggregation_preserves_its_inner_range_grouping() {
    let inner = r#"avg_over_time({app="api"} | logfmt | duration != "" | unwrap duration_seconds(duration) [1m]) without (service_name)"#;
    let plain = parse_metric_query(inner).unwrap();
    check!(plain.vector_aggregation.is_none());
    check!(plain.range_grouping == Some(VectorGrouping::Without(vec!["service_name".to_string()])));
    for source in [
        format!("max by (level) ({inner})"),
        format!("max({inner}) by (level)"),
    ] {
        let nested = parse_metric_query(&source).unwrap();
        check!(nested.aggregation == plain.aggregation);
        check!(nested.stream == plain.stream);
        check!(nested.range_ns == plain.range_ns);
        check!(nested.range_grouping == plain.range_grouping);
        check!(
            nested.vector_aggregation
                == Some(VectorAggregation {
                    op: VectorAggregationOp::Max,
                    grouping: Some(VectorGrouping::By(vec!["level".to_string()])),
                })
        );
        check!(parse_logql_expr(&source).is_ok());
    }
    for source in [
        r#"sum_over_time({app="api"} | unwrap cost [1m]) without (service_name)"#,
        r#"max by (level) (sum_over_time({app="api"} | unwrap cost [1m]) without (service_name))"#,
    ] {
        check!(parse_metric_query(source).is_err());
    }
}

#[test]
fn parses_stdvar_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"stdvar_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::StdvarOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_stddev_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"stddev_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::StddevOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_quantile_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"quantile_over_time(0.75, {app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(
        query.aggregation
            == RangeAggregation::QuantileOverTime(Quantile {
                numerator: QuantileNumerator(3),
                denominator: QuantileDenominator(4),
            })
    );
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_fraction_only_quantile_over_time_metric_query() {
    let query = parse_metric_query(
        r#"quantile_over_time(.75, {app="api"} | logfmt | unwrap cost [30s]) by (app)"#,
    )
    .unwrap();

    check!(
        query.aggregation
            == RangeAggregation::QuantileOverTime(Quantile {
                numerator: QuantileNumerator(3),
                denominator: QuantileDenominator(4),
            })
    );
    check!(query.range_grouping == Some(VectorGrouping::By(vec!["app".to_string()])));
}

#[test]
fn rejects_invalid_quantile_scalars() {
    check!(parse_metric_query(r#"quantile_over_time(1.01, {app="api"}[30s])"#).is_err());
    check!(parse_metric_query(r#"quantile_over_time(., {app="api"}[30s])"#).is_err());
    check!(parse_metric_query(r#"quantile_over_time(0., {app="api"}[30s])"#).is_err());
}

#[test]
fn parses_absent_over_time_metric_query() {
    let query = parse_metric_query(r#"absent_over_time({app="api",env="prod"} [30s])"#).unwrap();

    check!(query.aggregation == RangeAggregation::AbsentOverTime);
    check!(query.stream == parse_query(r#"{app="api",env="prod"}"#).unwrap());
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_present_over_time_metric_query() {
    let query = parse_metric_query(r#"present_over_time({app="api"} |= "error" [30s])"#).unwrap();

    check!(query.aggregation == RangeAggregation::PresentOverTime);
    check!(query.stream == parse_query(r#"{app="api"} |= "error""#).unwrap());
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_min_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"min_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::MinOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_max_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"max_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::MaxOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_first_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"first_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::FirstOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_last_over_time_unwrap_metric_query() {
    let query = parse_metric_query(
        r#"last_over_time({app="api"} | logfmt | unwrap cost | __error__ = "" [30s])"#,
    )
    .unwrap();

    check!(query.aggregation == RangeAggregation::LastOverTime);
    check!(
        query.stream
            == parse_query(r#"{app="api"} | logfmt | unwrap cost | __error__ = """#).unwrap()
    );
    check!(query.range_ns == DurationNanos(30_000_000_000));
}

#[test]
fn parses_compound_prometheus_duration_metric_query() {
    let query =
        parse_metric_query(r#"count_over_time({app="api"} |= "error" [1h30m15s250ms])"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: None,
                range_grouping: None,
                stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
                range_ns: DurationNanos(5_415_250_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_metric_query_with_range_offset() {
    let query =
        parse_metric_query(r#"count_over_time({app="api"} |= "error" [10s] offset 5m)"#).unwrap();

    check!(query.aggregation == RangeAggregation::CountOverTime);
    check!(query.stream == parse_query(r#"{app="api"} |= "error""#).unwrap());
    check!(query.range_ns == DurationNanos(10_000_000_000));
    check!(query.offset_ns == OffsetNanos(300_000_000_000));
}

#[test]
fn parses_metric_query_with_negative_range_offset() {
    let query =
        parse_metric_query(r#"count_over_time({app="api"} |= "error" [10s] offset -5m)"#).unwrap();

    check!(query.aggregation == RangeAggregation::CountOverTime);
    check!(query.stream == parse_query(r#"{app="api"} |= "error""#).unwrap());
    check!(query.range_ns == DurationNanos(10_000_000_000));
    check!(query.offset_ns == OffsetNanos(-300_000_000_000));
}

#[test]
fn rejects_metric_query_with_offset_after_range_aggregation() {
    check!(parse_metric_query(r#"count_over_time({app="api"} [10s]) offset 5m"#).is_err());
}

#[test]
fn rejects_out_of_order_or_repeated_prometheus_duration_units() {
    check!(parse_metric_query(r#"count_over_time({app="api"} [30m1h])"#).is_err());
    check!(parse_metric_query(r#"count_over_time({app="api"} [1h30m15m])"#).is_err());
}

#[test]
fn rejects_range_aggregations_that_do_not_support_grouping() {
    check!(parse_metric_query(r#"count_over_time({app="api"}[30s]) by (app)"#).is_err());
    check!(parse_metric_query(r#"rate({app="api"}[30s]) without (app)"#).is_err());
}

#[test]
fn rejects_invalid_metric_scalar_literals() {
    check!(
        parse_metric_scalar_comparison_query(r#". > bool count_over_time({app="api"}[30s])"#)
            .is_err()
    );
    check!(
        parse_metric_scalar_comparison_query(r#"1e > bool count_over_time({app="api"}[30s])"#)
            .is_err()
    );
    check!(
        parse_metric_scalar_comparison_query(r#"+.5e+2 > bool count_over_time({app="api"}[30s])"#)
            .is_ok()
    );
}

#[test]
fn parses_and_rejects_field_value_literal_boundaries() {
    check!(parse_query(r#"{app="api"} | logfmt | status = -5"#).is_ok());
    let error = parse_query(r#"{app="api"} | logfmt | status = -"#).unwrap_err();
    check!(
        error
            .to_string()
            .contains("expected field comparison value")
    );
    check!(parse_query(r#"{app="api"} | logfmt | size = 1.5MiB"#).is_ok());
    check!(parse_query(r#"{app="api"} | logfmt | size = 1.5XYZ"#).is_err());
}

#[test]
fn parses_vector_aggregation_metric_query_with_leading_or_trailing_grouping() {
    let expected = MetricQuery {
        aggregation: RangeAggregation::Rate,
        vector_aggregation: Some(VectorAggregation {
            op: VectorAggregationOp::Sum,
            grouping: Some(VectorGrouping::By(vec![
                "env".to_string(),
                "status".to_string(),
            ])),
        }),
        range_grouping: None,
        stream: parse_query(r#"{app="api"} |= "error""#).unwrap(),
        range_ns: DurationNanos(300_000_000_000),
        offset_ns: OffsetNanos(0),
    };

    for source in [
        r#"sum by (env, status) (rate({app="api"} |= "error" [5m]))"#,
        r#"sum(rate({app="api"} |= "error" [5m])) by (env, status)"#,
    ] {
        check!(parse_metric_query(source).unwrap() == expected, "{source}");
    }
}

#[test]
fn parses_vector_aggregation_without_metric_query() {
    let query =
        parse_metric_query(r#"avg without (pod) (bytes_over_time({app="api", env="prod"} [30s]))"#)
            .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::BytesOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::Avg,
                    grouping: Some(VectorGrouping::Without(vec!["pod".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api", env="prod"}"#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_stddev_vector_aggregation_metric_query() {
    let query =
        parse_metric_query(r#"stddev by (env) (count_over_time({app="api"} [30s]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::Stddev,
                    grouping: Some(VectorGrouping::By(vec!["env".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_stdvar_vector_aggregation_metric_query() {
    let query =
        parse_metric_query(r#"stdvar(count_over_time({app="api"} [30s])) without (pod)"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::Stdvar,
                    grouping: Some(VectorGrouping::Without(vec!["pod".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_count_values_vector_aggregation_metric_query() {
    let query = parse_metric_query(
        r#"count_values by (env) ("events", count_over_time({app="api"} [30s]))"#,
    )
    .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::CountValues("events".to_string()),
                    grouping: Some(VectorGrouping::By(vec!["env".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_topk_vector_aggregation_metric_query() {
    let query =
        parse_metric_query(r#"topk by (env) (2, count_over_time({app="api"} [30s]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::TopK(2),
                    grouping: Some(VectorGrouping::By(vec!["env".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_approx_topk_metric_query() {
    let query =
        parse_metric_query(r#"approx_topk(2, count_over_time({app="api"} [30s]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::ApproxTopK(2),
                    grouping: None,
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn rejects_approx_topk_with_grouping() {
    check!(
        parse_metric_query(r#"approx_topk by (env) (2, count_over_time({app="api"} [30s]))"#)
            .is_err()
    );
}

#[test]
fn parses_bottomk_vector_aggregation_metric_query() {
    let query =
        parse_metric_query(r#"bottomk(2, count_over_time({app="api"} [30s])) without (pod)"#)
            .unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::BottomK(2),
                    grouping: Some(VectorGrouping::Without(vec!["pod".to_string()])),
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_sort_vector_aggregation_metric_query() {
    let query = parse_metric_query(r#"sort(count_over_time({app="api"} [30s]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::Sort,
                    grouping: None,
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_sort_desc_vector_aggregation_metric_query() {
    let query = parse_metric_query(r#"sort_desc(count_over_time({app="api"} [30s]))"#).unwrap();

    check!(
        query
            == MetricQuery {
                aggregation: RangeAggregation::CountOverTime,
                vector_aggregation: Some(VectorAggregation {
                    op: VectorAggregationOp::SortDesc,
                    grouping: None,
                }),
                range_grouping: None,
                stream: parse_query(r#"{app="api"}"#).unwrap(),
                range_ns: DurationNanos(30_000_000_000),
                offset_ns: OffsetNanos(0),
            }
    );
}

#[test]
fn parses_selection_over_nested_vector_aggregation() {
    let query = r#"topk(1, sum by (app) (count_over_time({app=~".+"}[1m])))"#;
    let expression = parse_logql_expr(query).unwrap();

    check!(expression.to_string() == query);
}

#[test]
fn invalid_regex_reports_parse_error() {
    let error = parse_query(r#"{app=~"["}"#).unwrap_err();

    check!(error.to_string().contains("invalid regex"));
}

#[test]
fn invalid_syntax_reports_expected_token() {
    let error = parse_query(r#"{app="api" |= "error""#).unwrap_err();

    check!(error.to_string().contains("expected"));
}

fn current_unix_epoch_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after unix epoch")
        .as_nanos()
}

#[test]
fn nested_vector_aggregations_preserve_metric_children_and_grouping() {
    use krabka_logql::{LogqlExpr, VectorAggregationOp, VectorGrouping, parse_logql_expr};
    let query = "max(avg by (level) (avg_over_time({app=\"checkout\"} | logfmt | unwrap duration(duration) [1m])))";
    let parsed = parse_logql_expr(query).unwrap();
    let LogqlExpr::Aggregation {
        expr, aggregation, ..
    } = &parsed
    else {
        panic!("expected nested aggregation");
    };
    assert2::assert!(aggregation.op == VectorAggregationOp::Max);
    let LogqlExpr::Metric { query, .. } = expr.as_ref() else {
        panic!("expected metric child");
    };
    assert2::assert!(query.vector_aggregation.as_ref().unwrap().op == VectorAggregationOp::Avg);
    assert2::assert!(
        query.vector_aggregation.as_ref().unwrap().grouping
            == Some(VectorGrouping::By(vec!["level".to_owned()]))
    );
    assert2::assert!(parse_logql_expr(&parsed.to_string()).unwrap() == parsed);
    for query in ["max(1)", "avg({app=\"checkout\"})", "sum by (app) (1)"] {
        assert2::assert!(parse_logql_expr(query).is_err());
    }
    assert2::assert!(
        parse_logql_expr(
            "sum by (app) (max(avg_over_time({app=\"checkout\"} | unwrap size [1m])))"
        )
        .is_ok()
    );
}

#[test]
fn template_logic_preserves_types_and_operand_values() {
    let cases = [
        (
            r#"{{ if "false" }}yes{{ else }}no{{ end }}|{{ if "0" }}yes{{ else }}no{{ end }}"#,
            "yes|yes",
        ),
        (
            r"{{ if false }}yes{{ else }}no{{ end }}|{{ if true }}yes{{ else }}no{{ end }}",
            "no|yes",
        ),
        (
            r#"{{ and "left" "right" }}|{{ and "left" "" }}|{{ or "" "fallback" }}"#,
            "right||fallback",
        ),
        (
            r#"{{ and 1 0 }}|{{ or 0 2 }}|{{ "last" | and "first" }}|{{ "last" | or "first" }}"#,
            "0|2|last|first",
        ),
        (
            r#"{{ if contains "x" "no" }}bad{{ else }}no{{ end }}|{{ if not false }}yes{{ end }}"#,
            "no|yes",
        ),
        (
            r#"{{ eq "b" "a" "b" }}|{{ if eq "b" "a" }}bad{{ else }}no{{ end }}"#,
            "true|no",
        ),
        (
            r#"{{ default "fallback" false }}|{{ default "fallback" 0 }}|{{ default "fallback" "0" }}"#,
            "fallback|fallback|0",
        ),
        (
            r"{{ if sub 2 2 }}bad{{ else }}zero{{ end }}|{{ if addf 1 -1 }}bad{{ else }}zero{{ end }}",
            "zero|zero",
        ),
        (
            r#"{{ lt "3" "10" }}|{{ lt 3 10 }}|{{ eq true false true }}"#,
            "false|true|true",
        ),
        (
            r#"{{ len (fromJson "[1,2]") }}|{{ len (fromJson "{\"a\":1}") }}"#,
            "2|1",
        ),
        (r#"{{ or (fromJson "[]") (fromJson "[1,2]") }}"#, "[1 2]"),
        (
            r#"{{ with or "" "chosen" }}{{ . | upper }}{{ else }}bad{{ end }}"#,
            "CHOSEN",
        ),
    ];
    for (template, expected) in cases {
        let format = LineFormat::new(template).unwrap();
        check!(
            format.render("raw", &BTreeMap::new()) == expected,
            "{template}"
        );
    }
}

#[test]
fn template_runtime_errors_keep_input_and_can_be_filtered() {
    let labels = app_api_labels();
    for action in [
        r#"regexReplaceAll "[" __line__ "x""#,
        r#"count "[" __line__"#,
        "div 1 0",
        r#"contains "x""#,
        "eq .app 1",
        "len 1",
        "call nil",
        r#"bytes "soon""#,
        r#"bytes "18446744073709551616""#,
        r#"duration "1d""#,
        r#"duration "9223372036854775808ns""#,
        r#"contains 1 "one""#,
        r#"indent (add 1 2) "x""#,
        r#"index "abc" 3"#,
        r#"index "abc" "1""#,
        r#"slice "abc" 2 1"#,
        r#"slice "abc" 0 1 3"#,
    ] {
        let expression = format!("{{app=\"api\"}} | line_format `prefix{{{{ {action} }}}}`");
        let query = parse_query(&expression).unwrap();
        let result = query
            .evaluate_with_fields(&labels, "raw", &BTreeMap::new())
            .unwrap();
        check!(result.line == "raw");
        check!(result.fields.get("__error__").map(String::as_str) == Some("TemplateFormatErr"));
        let filtered = parse_query(&format!(r#"{expression} | __error__ = """#)).unwrap();
        check!(!filtered.matches(&labels, "raw"));
    }
    let short_circuit = parse_query(
        r#"{app="api"} | line_format `{{ or "chosen" (div 1 0) }}|{{ and "" (div 1 0) }}`"#,
    )
    .unwrap();
    let result = short_circuit
        .evaluate_with_fields(&labels, "raw", &BTreeMap::new())
        .unwrap();
    check!(result.line == "chosen|");
    check!(!result.fields.contains_key("__error__"));
    let label_query = parse_query(r#"{app="api"} | label_format result=`{{ div 1 0 }}`"#).unwrap();
    let result = label_query
        .evaluate_with_fields(&labels, "raw", &BTreeMap::new())
        .unwrap();
    check!(!result.fields.contains_key("result"));
    check!(result.fields.get("__error__").map(String::as_str) == Some("TemplateFormatErr"));
}

#[test]
fn template_variables_reassign_outer_bindings_without_leaking_declarations() {
    let cases = [
        (
            r#"{{ $sum := 0 }}{{ range $n := fromJson "[1,2,3]" }}{{ $sum = add $sum $n }}{{ . }}{{ end }}|{{ $sum }}"#,
            "123|6",
        ),
        (
            r#"{{ $v := "outer" }}{{ if true }}{{ $v := "inner" }}{{ $v }}{{ end }}|{{ $v }}"#,
            "inner|outer",
        ),
        (
            r#"{{ $v := "outer" }}{{ with "inner" }}{{ $v = . }}{{ end }}|{{ $v }}"#,
            "|inner",
        ),
        (
            r#"{{ range $i, $v := fromJson "[\"a\",\"b\"]" }}{{ $i }}={{ . }}={{ $v }};{{ end }}"#,
            "0=a=a;1=b=b;",
        ),
    ];
    for (template, expected) in cases {
        let format = LineFormat::new(template).unwrap();
        check!(
            format.render("raw", &BTreeMap::new()) == expected,
            "{template}"
        );
    }
}

#[test]
fn template_range_supports_integer_counts_and_nested_flow() {
    let cases = [
        (
            r"{{ range 3 }}{{ . }}{{ end }}|{{ range 0 }}bad{{ else }}empty{{ end }}",
            "012|empty",
        ),
        (
            r"{{ range 5 }}{{ if eq . 2 }}{{ break }}{{ end }}{{ . }}{{ end }}done",
            "01done",
        ),
        (
            r"{{ range 4 }}{{ if eq . 1 }}{{ continue }}{{ end }}{{ . }}{{ end }}done",
            "023done",
        ),
        (
            r#"{{ range $v := fromJson "[\"a\",\"b\"]" }}{{ range 3 }}{{ if eq . 1 }}{{ break }}{{ end }}{{ . }}{{ end }}{{ $v }}{{ end }}"#,
            "0a0b",
        ),
        (
            r#"{{ range $i, $v := fromJson "{\"a\":1,\"b\":2,\"c\":3}" }}{{ if eq $i "b" }}{{ continue }}{{ end }}{{ $i }};{{ end }}"#,
            "a;c;",
        ),
    ];
    for (template, expected) in cases {
        let format = LineFormat::new(template).unwrap();
        check!(
            format.render("raw", &BTreeMap::new()) == expected,
            "{template}"
        );
    }
    for template in [
        "{{ break }}",
        "{{ continue }}",
        "{{ if true }}{{ break }}{{ end }}",
    ] {
        check!(LineFormat::new(template).is_err());
    }
}

#[test]
fn template_definitions_reset_dot_root_and_variable_scope() {
    let cases = [
        (
            r#"{{ template "suffix" "chosen" }}{{ define "suffix" }}{{ . | upper }}={{ $ }}{{ end }}"#,
            "CHOSEN=chosen",
        ),
        (
            r#"{{ block "body" "fallback" }}{{ . }}{{ end }}"#,
            "fallback",
        ),
        (
            r#"{{ define "item" }}{{ .name }}{{ end }}{{ range fromJson "[{\"name\":\"a\"},{\"name\":\"b\"}]" }}{{ template "item" . }};{{ end }}"#,
            "a;b;",
        ),
        (
            r#"{{ define "tree" }}{{ .name }}{{ range .children }}{{ template "tree" . }}{{ end }}{{ end }}{{ template "tree" (fromJson "{\"name\":\"a\",\"children\":[{\"name\":\"b\"}]}") }}"#,
            "ab",
        ),
        (
            r#"{{ with fromJson "{}" }}bad{{ else }}{{ template "item" "chosen" }}{{ end }}{{ define "item" }}{{ . }}{{ end }}"#,
            "chosen",
        ),
    ];
    for (template, expected) in cases {
        let format = LineFormat::new(template).unwrap();
        check!(
            format.render("raw", &BTreeMap::new()) == expected,
            "{template}"
        );
    }
    let query =
        parse_query(r#"{app="api"} | line_format `{{ template "missing" . }}` | __error__ = """#)
            .unwrap();
    check!(!query.matches(&app_api_labels(), "raw"));
    check!(LineFormat::new(r#"{{ define "a" }}x{{ end }}{{ define "a" }}y{{ end }}"#).is_err());
    check!(LineFormat::new(r#"{{ if true }}{{ define "a" }}x{{ end }}{{ end }}"#).is_err());
    let format = LineFormat::new(r#"{{ define "a" }} {{ end }}{{ define "a" }}x{{ end }}{{ define "a" }}{{/* empty */}}{{ end }}{{ template "a" }}"#).unwrap();
    check!(format.render("raw", &BTreeMap::new()) == "x");
}

#[test]
fn supported_template_function_catalog_matches_independent_values() {
    for (name, expression, expected) in template_functions::CASES {
        let format = LineFormat::new(format!("{{{{ {expression} }}}}")).unwrap();
        check!(
            format.render("raw", &BTreeMap::new()) == *expected,
            "{name}: {expression}"
        );
    }
}

#[test]
fn printf_keeps_go_types_through_nested_functions_pipelines_and_json() {
    let format = LineFormat::new(
        r#"{{ printf "%#08x/%d/%T/%T/%T" 42 (add 3 4) 1 (add 1 2) (index "abc" 1) }}|{{ printf "%[1]*.[2]*[3]f" 6 2 1.25 }}|{{ "λx" | printf "%.1s" }}|{{ printf "%.1x/%+q" "λx" "é" }}|{{ printf "%e/%.3g/%x/%b" 1.25 12345.0 1.25 1.25 }}|{{ printf "%#v" (fromJson "{\"a\":true,\"b\":[1,\"x\"]}") }}|{{ printf "%f" "1.25" }}|{{ printf "%d %d" 3 }}"#,
    )
    .unwrap();
    let rendered = format.render("raw", &BTreeMap::new());
    check!(
        rendered
            == "0x0000002a/7/int/int64/uint8|  1.25|λ|ce/\"\\u00e9\"|1.250000e+00/1.23e+04/0x1.4p+00/5629499534213120p-52|map[string]interface {}{\"a\":true, \"b\":[]interface {}{1, \"x\"}}|%!f(string=1.25)|3 %!d(MISSING)"
    );
    let query = parse_query(
        r#"{app="api"} | line_format `{{ .status | printf "%d" }}` |= "%!d(string=200)""#,
    )
    .unwrap();
    let labels = BTreeMap::from([
        ("app".into(), "api".into()),
        ("status".into(), "200".into()),
    ]);
    check!(query.matches(&labels, "raw"));
    let wrong = BTreeMap::from([
        ("app".into(), "api".into()),
        ("status".into(), "404".into()),
    ]);
    check!(!query.matches(&wrong, "raw"));
}

#[test]
fn template_action_delimiters_inside_strings_and_comments_are_literal() {
    for (template, expected) in [
        (r#"{{ printf "%s" "}}" }}"#, "}}"),
        (r"{{/* }} ignored */}}ok", "ok"),
        (r#"{{ if true }}{{ printf "%s" "}}" }}{{ end }}"#, "}}"),
    ] {
        let format = LineFormat::new(template).unwrap();
        check!(format.render("raw", &BTreeMap::new()) == expected);
    }
}

#[test]
fn typed_time_functions_compose_layouts_zones_printf_and_wrapping_epochs() {
    let labels = BTreeMap::from([("app".into(), "api".into())]);
    for (template, expected) in [
        (
            r#"{{ printf "%T" (__timestamp__) }}|{{ date "January Mon _2 3:04:05PM MST" (__timestamp__) }}"#,
            "time.Time|January Tue  2 12:04:05PM UTC",
        ),
        (
            r#"{{ toDateInZone "2006-01-02 15:04:05" "America/New_York" "2024-03-10 02:30:00" | printf "%s" }}"#,
            "2024-03-10 01:30:00 -0500 EST",
        ),
        (
            r#"{{ toDateInZone "2006-01-02 15:04:05" "Europe/Berlin" "2024-10-27 02:30:00" | date "15:04 MST" }}"#,
            "01:30 UTC",
        ),
        (
            r#"{{ toDate "2006-01-02" "invalid" | unixEpoch }}|{{ toDate "2006-01-02" "invalid" | printf "%#v" }}"#,
            "-62135596800|time.Date(1, time.January, 1, 0, 0, 0, 0, time.UTC)",
        ),
        (
            r#"{{ toDateInZone "15:04:05.000" "UTC" "12:34:56,123" | unixEpochNanos }}|{{ toDateInZone "15:04:05.000" "UTC" "12:34:56,123" | printf "%s" }}"#,
            "-6826941682748345152|0000-01-01 12:34:56.123 +0000 UTC",
        ),
        (
            r#"{{ unixToTime "9999999999999" | unixEpochNanos }}|{{ date "2006-01-02T15:04:05" 1 }}|{{ date "2006-01-02T15:04:05" (add 1 1) }}"#,
            "-8446744073710551616|1970-01-01T00:00:01|1970-01-01T00:00:02",
        ),
        (
            r#"{{ toDateInZone "15:04 MST" "America/New_York" "12:00 EST" | printf "%s" }}"#,
            "0000-01-01 12:03:58 -0456 LMT",
        ),
    ] {
        let query = parse_query(&format!(r#"{{app="api"}} | line_format `{template}`"#)).unwrap();
        let result = query
            .evaluate_with_fields_at(
                &labels,
                "original",
                &BTreeMap::new(),
                1_704_197_045_123_456_789,
            )
            .unwrap();
        check!(result.line == expected);
        check!(!result.fields.contains_key("__error__"));
    }
    let query = parse_query(r#"{app="api"} | line_format `{{ unixToTime "soon" | date "2006" }}`"#)
        .unwrap();
    let result = query
        .evaluate_with_fields(&labels, "original", &BTreeMap::new())
        .unwrap();
    check!(result.fields.get("__error__").map(String::as_str) == Some("TemplateFormatErr"));
    check!(result.line == "original");
}

#[test]
fn rule_template_bytes_survive_variables_queries_and_control_flow() {
    let variables = BTreeMap::from([(
        "labels".into(),
        TemplateData::ByteLabels(BTreeMap::from([
            ("a".into(), vec![0xff]),
            ("b".into(), vec![0xfe]),
        ])),
    )]);
    let queries = BTreeMap::from([(
        "up".into(),
        TemplateData::QueryResult(
            vec![TemplateData::Sample(std::sync::Arc::new(BTreeMap::from([
                (
                    "Labels".into(),
                    TemplateData::ByteLabels(BTreeMap::from([("raw".into(), vec![0xe2, 0x82])])),
                ),
                ("Value".into(), TemplateData::Float(7.0)),
            ])))]
            .into(),
        ),
    )]);
    let format = LineFormat::new_prometheus(r#"{{ $x := $labels.a }}{{ if ne $x $labels.b }}{{ $x }}{{ end }}{{ range $k,$v := $labels }}{{ $k }}={{ $v }};{{ end }}{{ label "raw" (first (query "up")) }}|{{ value (first (query "up")) }}|{{ printf "%x" (slice (label "raw" (first (query "up"))) 0 1) }}"#).unwrap();
    check!(
        format
            .render_bytes_with_variables_and_queries(&variables, &queries)
            .unwrap()
            == vec![
                0xff, b'a', b'=', 0xff, b';', b'b', b'=', 0xfe, b';', 0xe2, 0x82, b'|', b'7', b'|',
                b'e', b'2'
            ]
    );
    let invalid = LineFormat::new(r#"{{ index $labels "a" 3 }}"#).unwrap();
    check!(
        invalid
            .render_bytes_with_variables_and_queries(&variables, &queries)
            .is_err()
    );
}

#[test]
fn go_numeric_constants_keep_float_complex_and_execution_overflow_semantics() {
    let labels = BTreeMap::from([("app".into(), "api".into())]);
    for (template, expected) in [
        (
            r#"{{printf "%T|%v|%f|%x" .5 .5 .5 .5}}"#,
            "float64|0.5|0.500000|0x1p-01",
        ),
        (
            r#"{{printf "%T|%v|%f" 0x_1.fp2 0x_1.fp2 0x_1.fp2}}"#,
            "float64|7.75|7.750000",
        ),
        (
            r#"{{printf "%T|%v" 1e0 1e0}}|{{printf "%T|%v" 0755 0755}}"#,
            "float64|1|int|493",
        ),
        (
            r#"{{printf "%T|%v|%f|%x" 1+2i 1+2i 1+2i 1+2i}}"#,
            "complex128|(1+2i)|(1.000000+2.000000i)|(0x1p+00+0x1p+01i)",
        ),
        (
            r#"{{if 0i}}bad{{else}}zero{{end}}|{{eq 1+2i 1+2i}}|{{ne 1+2i 2i}}|{{printf "%d" 2i}}"#,
            "zero|true|true|%!d(complex128=(0+2i))",
        ),
        (
            r#"{{float64 "0x1.fp2"}}|{{addf "0x1p2" 1}}|{{float64 2i}}|{{int 2i}}|{{float64 "1e400"}}"#,
            "7.75|5|0|0|0",
        ),
        (r#"{{printf "%T" now}}|{{repeat 2.0 "x"}}"#, "time.Time|xx"),
    ] {
        let query = parse_query(&format!(r#"{{app="api"}} | line_format `{template}`"#)).unwrap();
        let result = query
            .evaluate_with_fields(&labels, "original", &BTreeMap::new())
            .unwrap();
        check!(result.line == expected);
        check!(!result.fields.contains_key("__error__"));
    }
    for template in [
        "{{unknown}}",
        "{{08}}",
        "{{1e309}}",
        "{{18446744073709551616}}",
        "{{1__2}}",
    ] {
        check!(parse_query(&format!(r#"{{app="api"}} | line_format `{template}`"#)).is_err());
    }
    for template in [
        "{{9223372036854775808}}",
        "{{printf \"%v\" 18446744073709551615}}",
        "{{lt 2i 3i}}",
    ] {
        let query = parse_query(&format!(r#"{{app="api"}} | line_format `{template}`"#)).unwrap();
        let result = query
            .evaluate_with_fields(&labels, "original", &BTreeMap::new())
            .unwrap();
        check!(result.fields.get("__error__").map(String::as_str) == Some("TemplateFormatErr"));
        check!(result.line == "original");
    }
}

#[test]
fn template_execution_uses_go_depth_and_range_assignment_semantics() {
    let recursive = LineFormat::new(r#"{{ define "down" }}{{ if gt . 0 }}{{ range 1 }}{{ template "down" (sub $ 1) }}{{ end }}{{ else }}done{{ end }}{{ end }}{{ template "down" 2000 }}"#).unwrap();
    check!(
        recursive
            .render_bytes_with_variables_and_queries(&BTreeMap::new(), &BTreeMap::new())
            .unwrap()
            == b"done"
    );
    let overflow = LineFormat::new(
        r#"{{ define "loop" }}{{ template "loop" . }}{{ end }}{{ template "loop" . }}"#,
    )
    .unwrap();
    check!(
        overflow
            .render_bytes_with_variables_and_queries(&BTreeMap::new(), &BTreeMap::new())
            .unwrap_err()
            .contains("depth")
    );
    for (template, expected) in [
        (
            r#"{{$i := 9}}{{$v := "before"}}{{range $i, $v = fromJson "[10,20]"}}{{$i}}={{$v}};{{end}}|{{$i}}:{{$v}}"#,
            "0=10;1=20;|1:20",
        ),
        (r"{{$v := 9}}{{range $v = 3}}{{.}}{{end}}|{{$v}}", "012|2"),
        (
            r#"{{range $v := fromJson "[]"}}bad{{else}}{{len $v}}{{end}}|{{range (fromJson "null")}}bad{{else}}nil{{end}}"#,
            "0|nil",
        ),
        (
            r"{{range 2}}{{range 0}}bad{{else}}{{continue}}{{end}}bad{{end}}done",
            "done",
        ),
    ] {
        let format = LineFormat::new(template).unwrap();
        check!(
            format
                .render_bytes_with_variables_and_queries(&BTreeMap::new(), &BTreeMap::new())
                .unwrap()
                == expected.as_bytes(),
            "{template}"
        );
    }
    for template in [
        r#"{{range "abc"}}bad{{else}}bad{{end}}"#,
        r"{{range true}}bad{{end}}",
        r"{{range $i, $v := 3}}bad{{end}}",
        r"{{range $missing = 1}}bad{{end}}",
    ] {
        check!(
            LineFormat::new(template)
                .unwrap()
                .render_bytes_with_variables_and_queries(&BTreeMap::new(), &BTreeMap::new())
                .is_err(),
            "{template}"
        );
    }
}

#[test]
fn template_query_dependencies_survive_parenthesized_result_paths() {
    let format = LineFormat::new_prometheus(r#"{{ (index (query "counter") 0).Value }}|{{ printf "%d" (index (query "counter") 0).Value }}"#).unwrap();
    check!(format.query_calls().into_iter().collect::<Vec<_>>() == vec!["counter"]);
    let queries = BTreeMap::from([(
        "counter".into(),
        TemplateData::QueryResult(
            vec![TemplateData::Sample(std::sync::Arc::new(BTreeMap::from([
                ("Value".into(), TemplateData::Integer64(7)),
            ])))]
            .into(),
        ),
    )]);
    check!(
        format
            .render_bytes_with_variables_and_queries(&BTreeMap::new(), &queries)
            .unwrap()
            == b"7|7"
    );
}

#[test]
fn typed_time_methods_compose_with_chains_named_values_and_byte_results() {
    // Independent Go1.26.5 time.Time/template fixture, not derived from the Rust model.
    for (suffix, expected) in [
        (
            r#"{{$t.Format "2006"}}|{{$t.Unix}}|{{($t).Year}}|{{$t.YearDay}}|{{printf "%T:%v:%s:%d:%#v" $t.Month $t.Month $t.Month $t.Month $t.Month}}"#,
            "2024|1709211845|2024|60|time.Month:February:February:2:2",
        ),
        (
            r#"{{printf "%T:%v:%d" $t.Weekday $t.Weekday $t.Weekday}}"#,
            "time.Weekday:Thursday:4",
        ),
        (
            r#"{{($t.AddDate 1 1 -2).Format "2006-01-02 15:04:05.999999999 MST"}}"#,
            "2025-03-27 13:04:05.123456789 UTC",
        ),
        (
            r"{{$t.Add 1500000000}}",
            "2024-02-29 13:04:06.623456789 +0000 UTC",
        ),
        (
            r#"{{$d := $t.Sub ($t.Add -1500000000)}}{{printf "%T:%v:%.6f" $d $d $d.Seconds}}|{{$d.Abs.Nanoseconds}}"#,
            "time.Duration:1.5s:1.500000|1500000000",
        ),
        (
            r#"{{printf "%T:%x" $t.MarshalBinary $t.MarshalBinary}}"#,
            "[]uint8:010000000edd7277c5075bcd15ffff",
        ),
        (
            r#"{{printf "%T:%s" $t.MarshalText $t.MarshalText}}|{{printf "%s" $t.MarshalJSON}}"#,
            "[]uint8:2024-02-29T13:04:05.123456789Z|\"2024-02-29T13:04:05.123456789Z\"",
        ),
        (
            r#"{{printf "%T:%v" $t.Location $t.Location}}|{{$t.Before ($t.Add 1)}}|{{$t.Equal $t.UTC}}"#,
            "*time.Location:UTC|true|true",
        ),
        (
            r#"{{len ($t.AppendText nil)}}|{{printf "%x" (slice $t.MarshalBinary 0 3)}}|{{printf "%T" (index $t.MarshalBinary 0)}}"#,
            "30|010000|uint8",
        ),
        (
            r#"{{printf "%x" ($t.Format "2006\xff01")}}"#,
            "32303234ff3032",
        ),
    ] {
        let template = format!(
            r#"{{{{$t := toDate "2006-01-02T15:04:05.999999999Z07:00" "2024-02-29T13:04:05.123456789Z"}}}}{suffix}"#
        );
        let output = LineFormat::new(template)
            .unwrap()
            .render_bytes_with_variables_and_queries(&BTreeMap::new(), &BTreeMap::new())
            .unwrap();
        check!(output == expected.as_bytes(), "{suffix}");
    }
    for template in [
        "{{nil}}",
        "{{range nil}}bad{{end}}",
        r#"{{(unixToTime "0000000000").Date}}"#,
        r#"{{(unixToTime "0000000000").Zone}}"#,
        r#"{{$n := 1}}{{(unixToTime "0000000000").Add $n}}"#,
        r#"{{(unixToTime "0000000000").Unix 1}}"#,
        r#"{{(unixToTime "0000000000").MarshalText.Error}}"#,
    ] {
        check!(
            LineFormat::new(template)
                .unwrap()
                .render_bytes_with_variables_and_queries(&BTreeMap::new(), &BTreeMap::new())
                .is_err(),
            "{template}"
        );
    }
}

#[test]
fn go_rune_byte_escapes_are_integer_constants_and_multi_escape_runes_fail() {
    let format =
        LineFormat::new(r#"{{printf "%T:%d:%d:%d" '\xff' '\xff' '\377' '\xe2'}}"#).unwrap();
    check!(format.render("", &BTreeMap::new()) == "int:255:255:226");
    check!(LineFormat::new(r"{{'\xc3\xa9'}}").is_err());
    check!(
        LineFormat::new("{{`a\rb`}}")
            .unwrap()
            .render("", &BTreeMap::new())
            == "ab"
    );
}

#[test]
fn prometheus_template_function_profile_matches_pinned_expander() {
    let sample = |job: &str, value: f64, strvalue: Option<&str>| {
        let mut labels = BTreeMap::from([("job".into(), job.as_bytes().to_vec())]);
        if let Some(value) = strvalue {
            labels.insert("__value__".into(), value.as_bytes().to_vec());
        }
        TemplateData::Sample(std::sync::Arc::new(BTreeMap::from([
            ("Labels".into(), TemplateData::ByteLabels(labels)),
            ("Value".into(), TemplateData::Float(value)),
        ])))
    };
    let vector = TemplateData::QueryResult(
        vec![
            sample("b", 7.0, Some("raw")),
            sample("a", 2.0, None),
            sample("b", 9.0, None),
        ]
        .into(),
    );
    let variables = BTreeMap::from([(
        "externalURL".into(),
        TemplateData::String("https://prom.example/x%20y?arg=a%2Bb".into()),
    )]);
    for (name, expression, expected, rejected) in prometheus_template_functions::CASES {
        let format = LineFormat::new_prometheus(format!("{{{{ {expression} }}}}")).unwrap();
        let query = match *name {
            "first_empty" => TemplateData::QueryResult(Vec::new().into()),
            "query_error" => TemplateData::QueryError("backend unavailable".into()),
            _ => vector.clone(),
        };
        let result =
            format.render_prometheus_bytes(&variables, &[query.clone(), query], 1_709_211_845_123);
        if *rejected {
            check!(result.is_err(), "{name}: {expression}");
        } else {
            check!(
                result.unwrap() == expected.as_bytes(),
                "{name}: {expression}"
            );
        }
    }
    // A first() pointer cloned from a cached result retains identity, while
    // two source executions return separately allocated sample pointers.
    let same =
        LineFormat::new_prometheus(r#"{{$q := query "metric"}}{{eq (first $q) (first $q)}}"#)
            .unwrap();
    check!(
        same.render_prometheus_bytes(&variables, std::slice::from_ref(&vector), 0)
            .unwrap()
            == b"true"
    );
    let distinct =
        LineFormat::new_prometheus(r#"{{eq (first (query "metric")) (first (query "metric"))}}"#)
            .unwrap();
    let other = TemplateData::QueryResult(vec![sample("b", 7.0, Some("raw"))].into());
    check!(
        distinct
            .render_prometheus_bytes(&variables, &[vector, other], 0)
            .unwrap()
            == b"false"
    );
}

#[test]
fn prometheus_query_errors_keep_the_backend_cause_through_consumers() {
    for template in [
        r#"{{ value (first (query "metric")) }}"#,
        r#"{{ range query "metric" }}bad{{ end }}"#,
        r#"{{ and (query "metric") true }}"#,
    ] {
        let format = LineFormat::new_prometheus(template).unwrap();
        let error = format
            .render_prometheus_bytes(
                &BTreeMap::new(),
                &[TemplateData::QueryError("backend unavailable".into())],
                0,
            )
            .unwrap_err();
        check!(
            error.to_string().contains("backend unavailable"),
            "{template}: {error}"
        );
    }
}

#[test]
fn template_nested_pipelines_preserve_quotes_and_reject_unbalanced_groups() {
    let format = LineFormat::new_prometheus(
        r#"{{ printf "%s" (print "a|b" | printf "[%s]") | printf "<%s>" }}"#,
    )
    .unwrap();
    check!(
        format
            .render_prometheus_bytes(&BTreeMap::new(), &[], 0)
            .unwrap()
            == b"<[a|b]>"
    );
    for template in [
        r#"{{ print (print "x" |) }}"#,
        r#"{{ print ("x" }}"#,
        r#"{{ print "x" | }}"#,
        r"{{ print ) }}",
    ] {
        check!(LineFormat::new_prometheus(template).is_err(), "{template}");
    }
}

#[test]
fn json_parser_preserves_physical_key_order_duplicate_keys_and_numeric_text() {
    let query = parse_query(r#"{app="api"} | json"#).unwrap();
    let labels = BTreeMap::from([("app".into(), "api".into())]);
    for (line, expected) in [
        (r#"{"a-b":"first","a_b":"second"}"#, "first"),
        (r#"{"a_b":"second","a-b":"first"}"#, "second"),
        (r#"{"a":{"b":"nested"},"a_b":"flat"}"#, "nested"),
        (r#"{"a_b":"flat","a":{"b":"nested"}}"#, "flat"),
        (r#"{"a_b":"first","a_b":"last"}"#, "first"),
        (r#"{"a_b":"first"} trailing"#, "first"),
        (r#"{"a_b":"one�two\uFFFDthree"}"#, "one two three"),
    ] {
        let evaluation = query
            .evaluate_with_fields(&labels, line, &BTreeMap::new())
            .unwrap();
        check!(
            evaluation.fields.get("a_b").map(String::as_str) == Some(expected),
            "{line}"
        );
    }
    let evaluation = query.evaluate_with_fields(&labels,
        r#"{"exponent":1e3,"decimal":1.00,"large":123456789012345678901234567890,"nothing":null,"array":[1,2],"flag":true}"#,
        &BTreeMap::new()).unwrap();
    check!(
        evaluation.fields
            == BTreeMap::from([
                ("app".into(), "api".into()),
                ("exponent".into(), "1e3".into()),
                ("decimal".into(), "1.00".into()),
                ("large".into(), "123456789012345678901234567890".into()),
                ("flag".into(), "true".into()),
            ])
    );
}

fn app_api_labels() -> BTreeMap<String, String> {
    BTreeMap::from([("app".to_string(), "api".to_string())])
}

/// A log line of the `{app="api"}` stream, and the query to evaluate on it.
struct ApiLine<'a> {
    query: &'a str,
    line: &'a str,
}

impl ApiLine<'_> {
    fn evaluate(&self) -> PipelineEvaluation {
        parse_query(self.query)
            .unwrap()
            .evaluate_with_fields(&app_api_labels(), self.line, &BTreeMap::new())
            .unwrap()
    }
}

fn check_matches_only_rate_30_queries(query: &str) {
    let query = parse_query(query).unwrap();
    let labels = app_api_labels();

    check!(query.matches(
        &labels,
        r#"{"queries":[{"query":"rate","duration":30},{"query":"sum","duration":15}]}"#
    ));
    check!(!query.matches(
        &labels,
        r#"{"queries":[{"query":"rate","duration":20},{"query":"sum","duration":15}]}"#
    ));
}

fn check_matches_only_post_500(query: &str) {
    let query = parse_query(query).unwrap();
    let labels = app_api_labels();

    check!(query.matches(&labels, "POST /api/prom/query_range (500) 1.5s"));
    check!(!query.matches(&labels, "GET /api/prom/query_range (500) 1.5s"));
    check!(!query.matches(&labels, "POST /api/prom/query_range (200) 1.5s"));
    check!(!query.matches(&labels, "not a matching line"));
}

fn check_sanitized_field_names(evaluation: &PipelineEvaluation) {
    check!(evaluation.fields.get("trace_id") == Some(&"abc".to_string()));
    check!(evaluation.fields.get("span:id") == Some(&"def".to_string()));
    check!(evaluation.fields.get("already_ok") == Some(&"ghi".to_string()));
    check!(evaluation.fields.get("_9lives") == Some(&"cat".to_string()));
}
