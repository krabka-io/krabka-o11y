use crate::tests::prelude::{
    Arc, ArrayRef, BTreeMap, BTreeSet, DataType, Field, LabelIndex, Labels, LokiStreamEncoding,
    MetricValue, PipelineStage, QueryRow, RecordBatch, Schema, SessionContext, StreamPlan,
    StringArray, TimeRange, append_matching_log_row, check, json, line_filter_sql_predicates,
    loki_streams_response, parse_query,
};
use crate::{LokiStreamEntry, SeriesFingerprint};

// A label index that holds the one series `{app="api"}`, and a plan that
// reads that series with a query.
struct ApiSeriesPlan {
    label_index: LabelIndex,
    api: SeriesFingerprint,
    plan: StreamPlan,
}

impl ApiSeriesPlan {
    fn new(query: &str) -> Self {
        let mut labels = Labels::default();
        labels.insert("app".to_string(), "api".to_string());
        Self::with_labels(query, labels)
    }

    fn with_labels(query: &str, labels: Labels) -> Self {
        let mut label_index = LabelIndex::default();
        let api = label_index.insert_series("tenant", labels);
        let plan = StreamPlan {
            tenant: "tenant".to_string(),
            time_range: TimeRange::new(0, 100).expect("a valid range"),
            query: parse_query(query).expect("the query parses"),
            fingerprints: [api].into_iter().collect(),
            blocks: Vec::new(),
        };
        Self {
            label_index,
            api,
            plan,
        }
    }
}

#[allow(clippy::needless_pass_by_value)]
fn assert_stream_response_encodings(
    streams: BTreeMap<Labels, Vec<LokiStreamEntry>>,
    folded: crate::Value,
    categorized: crate::Value,
) {
    check!(loki_streams_response(streams.clone(), LokiStreamEncoding::Folded) == folded);
    check!(loki_streams_response(streams, LokiStreamEncoding::CategorizeLabels) == categorized);
}

/// `label_format` is the one stage whose write cannot be recognised from the
/// value it left behind: it can name a series label, a piece of structured
/// metadata, or a label that did not exist, and it can write any of them the
/// string that was already there. Loki categorises all four the same way --
/// the destination is parsed, and the name leaves whichever bucket held it.
///
/// `env` here is rewritten to the value the series already carried, which is
/// the case a value-based reading gets wrong, and `shard` is a metadata name
/// the stage takes over.
#[test]
pub(crate) fn a_label_format_stage_parses_a_label_whatever_it_overwrote() {
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    labels.insert("env".to_string(), "prod".to_string());
    let ApiSeriesPlan {
        label_index,
        api,
        plan,
    } = ApiSeriesPlan::with_labels(
        "{app=\"api\"} | label_format env=\"prod\", shard=\"z\", region=\"eu\"",
        labels,
    );

    let mut structured_metadata = Labels::default();
    structured_metadata.insert("shard".to_string(), "a".to_string());
    let mut streams = BTreeMap::new();
    append_matching_log_row(
        &mut streams,
        &plan,
        &label_index,
        QueryRow {
            fingerprint: api,
            timestamp_ns: 10,
            line: "api ok",
            structured_metadata: &structured_metadata,
        },
        &[],
    )
    .expect("the plan names the series");

    assert_stream_response_encodings(
        streams,
        json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {
                            "app": "api",
                            "env": "prod",
                            "region": "eu",
                            "shard": "z"
                        },
                        "values": [["10", "api ok"]]
                    }
                ]
            }
        }),
        json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "api"},
                        "values": [
                            [
                                "10",
                                "api ok",
                                {
                                    "parsed": {
                                        "env": "prod",
                                        "region": "eu",
                                        "shard": "z"
                                    }
                                }
                            ]
                        ]
                    }
                ]
            }
        }),
    );
}

/// A label a parser stage produced is a category of its own upstream. The
/// default encoding folds it into the stream's labels alongside the structured
/// metadata, so the two are indistinguishable there; under `categorize-labels`
/// it lands in `parsed` while the pushed metadata stays in
/// `structuredMetadata`, and the stream keeps only the series' own labels.
#[test]
pub(crate) fn a_parser_stage_fills_the_parsed_bucket_and_not_the_metadata_one() {
    let ApiSeriesPlan {
        label_index,
        api,
        plan,
    } = ApiSeriesPlan::new("{app=\"api\"} | json");

    let mut structured_metadata = Labels::default();
    structured_metadata.insert("trace_id".to_string(), "abc".to_string());
    let mut streams = BTreeMap::new();
    append_matching_log_row(
        &mut streams,
        &plan,
        &label_index,
        QueryRow {
            fingerprint: api,
            timestamp_ns: 10,
            line: r#"{"status":"500"}"#,
            structured_metadata: &structured_metadata,
        },
        &[],
    )
    .expect("the plan names the series");

    assert_stream_response_encodings(
        streams,
        json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {
                            "app": "api",
                            "status": "500",
                            "trace_id": "abc"
                        },
                        "values": [["10", r#"{"status":"500"}"#]]
                    }
                ]
            }
        }),
        json!({
            "status": "success",
            "data": {
                "resultType": "streams",
                "result": [
                    {
                        "stream": {"app": "api"},
                        "values": [
                            [
                                "10",
                                r#"{"status":"500"}"#,
                                {
                                    "structuredMetadata": {
                                        "trace_id": "abc"
                                    },
                                    "parsed": {"status": "500"}
                                }
                            ]
                        ]
                    }
                ]
            }
        }),
    );
}

/// `page_groups` pages the rules response by group, resuming AFTER the
/// token the client sent rather than at it -- resuming at it would return
/// the same group forever. The token it hands back names the LAST group in
/// the page, which is what makes the next request resume correctly.
///
/// The two are checked against each other by walking a five-group list to
/// exhaustion in pages of two: an off-by-one in either the resume or the
/// token shows up as a repeated or skipped group rather than as a wrong
/// count.
#[test]
pub(crate) fn paging_rule_groups_resumes_after_the_token_it_handed_back() {
    let groups = || {
        ["a", "b", "c", "d", "e"]
            .iter()
            .map(|name| crate::tests::prelude::PrometheusRuleGroupResponse {
                token: (*name).to_string(),
                value: serde_json::json!({"name": name}),
            })
            .collect::<Vec<_>>()
    };
    let page = |limit: Option<usize>, token: Option<&str>| {
        crate::tests::prelude::PrometheusRulesFilters {
            group_limit: limit,
            group_next_token: token.map(str::to_string),
            ..crate::tests::prelude::PrometheusRulesFilters::default()
        }
        .page_groups(groups())
    };
    let names = |page: &crate::tests::prelude::PrometheusRulesPage| {
        page.groups
            .iter()
            .map(|group| group["name"].as_str().expect("a name").to_string())
            .collect::<Vec<_>>()
    };

    // No limit returns everything, with nothing to resume from.
    let all = page(None, None).expect("no limit is valid");
    check!(names(&all) == vec!["a", "b", "c", "d", "e"]);
    check!(all.next_token.is_none());

    // Walk the list in pages of two. The token names the last group
    // returned, and the next page starts after it.
    let first = page(Some(2), None).expect("a first page");
    check!(names(&first) == vec!["a", "b"]);
    check!(
        first.next_token.as_deref() == Some("b"),
        "the LAST group of the page"
    );

    let second = page(Some(2), Some("b")).expect("a second page");
    check!(
        names(&second) == vec!["c", "d"],
        "resumes after b, not at it"
    );
    check!(second.next_token.as_deref() == Some("d"));

    // The final page is short and offers no token, because nothing follows.
    let third = page(Some(2), Some("d")).expect("a third page");
    check!(names(&third) == vec!["e"]);
    check!(third.next_token.is_none());

    // A page that exactly exhausts the list offers no token either: the
    // boundary is `>` and not `>=`, or a client would ask for an empty page.
    let exact = page(Some(5), None).expect("an exact page");
    check!(names(&exact) == vec!["a", "b", "c", "d", "e"]);
    check!(exact.next_token.is_none(), "nothing follows an exact fit");

    // A zero limit returns nothing and offers no token to resume from,
    // rather than a token that would never advance.
    let none = page(Some(0), None).expect("a zero limit is valid");
    check!(names(&none).is_empty());
    check!(none.next_token.is_none());

    // Resuming from the last group leaves an empty page.
    let past = page(Some(2), Some("e")).expect("resuming from the end");
    check!(names(&past).is_empty());

    // A token naming no group is a client error, not an empty page: it
    // usually means the group was deleted between requests.
    check!(page(Some(2), Some("nonsense")).is_err());
}

/// `append_matching_log_row` decides whether one row belongs in the
/// response. Its first guard is three conditions or-ed as REJECTIONS --
/// too early, too late, or not a series the plan wants -- so each rejects
/// on its own against a row the other two accept.
///
/// Both range bounds are inclusive, pinned by rows sitting exactly on
/// each. A row whose series the label index cannot name is an ERROR rather
/// than a skip: the plan asked for that series, so being unable to label
/// it means the index disagrees with the plan.
#[test]
pub(crate) fn a_log_row_is_appended_only_when_the_plan_asked_for_it() {
    use krabka_logql::StreamPlan;

    let mut label_index = LabelIndex::default();
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    let known = label_index.insert_series("tenant", labels);
    let mut other = Labels::default();
    other.insert("app".to_string(), "web".to_string());
    let unwanted = label_index.insert_series("tenant", other);

    let plan = StreamPlan {
        tenant: "tenant".to_string(),
        time_range: krabka_blockstore::TimeRange::new(10, 90).expect("a valid range"),
        query: krabka_logql::parse_query("{app=\"api\"}").expect("the query parses"),
        fingerprints: [known].into_iter().collect(),
        blocks: Vec::new(),
    };
    let metadata = Labels::default();
    let appended = |fingerprint, timestamp_ns| {
        let mut streams = BTreeMap::new();
        let result = crate::tests::prelude::append_matching_log_row(
            &mut streams,
            &plan,
            &label_index,
            crate::tests::prelude::QueryRow {
                fingerprint,
                timestamp_ns,
                line: "line",
                structured_metadata: &metadata,
            },
            &[],
        );
        result.map(|()| streams.values().map(Vec::len).sum::<usize>())
    };

    // Inside the range, and a series the plan wants.
    check!(appended(known, 50).ok() == Some(1));
    // Exactly on each bound: both inclusive.
    check!(
        appended(known, 10).ok() == Some(1),
        "the start bound is inclusive"
    );
    check!(appended(known, 90).ok() == Some(1), "and so is the end");
    // One step outside each.
    check!(appended(known, 9).ok() == Some(0), "before the range");
    check!(appended(known, 91).ok() == Some(0), "after it");
    // A series the plan did not ask for, inside the range.
    check!(
        appended(unwanted, 50).ok() == Some(0),
        "not a wanted series"
    );

    // A fingerprint the label index cannot name is an error, not a skip --
    // but only once the row has passed the range and series filters, so a
    // nameless series the plan never wanted is still simply skipped.
    let nameless = 999_999_u64;
    check!(
        appended(nameless, 50).ok() == Some(0),
        "not wanted, so not named"
    );
    let mut wants_nameless = plan.clone();
    wants_nameless.fingerprints.insert(nameless);
    let mut streams = BTreeMap::new();
    check!(matches!(
        crate::tests::prelude::append_matching_log_row(
            &mut streams,
            &wants_nameless,
            &label_index,
            crate::tests::prelude::QueryRow {
                fingerprint: nameless,
                timestamp_ns: 50,
                line: "line",
                structured_metadata: &metadata,
            },
            &[],
        ),
        Err(crate::tests::prelude::QueryError::MissingSeriesLabels { .. })
    ));
}

struct ComparisonCase {
    ordering: std::cmp::Ordering,
    sample: MetricValue,
    scalar: MetricValue,
    scalar_side: crate::tests::prelude::ScalarSide,
}

/// `metric_scalar_comparison_matches` compares a sample against a scalar,
/// given the side the scalar was written on. That side only
/// matters for the four ordered operators -- `1 > x` and `x > 1` disagree
/// where `1 == x` and `x == 1` do not -- so every operator is checked at
/// all three orderings AND on both sides.
///
/// The two regex operators are always false here: a regex against a number
/// is not a comparison `LogQL` can evaluate, and answering either way would
/// silently filter samples on a predicate nobody wrote.
#[test]
pub(crate) fn a_scalar_comparison_answers_every_operator_from_both_sides() {
    use std::cmp::Ordering;

    use krabka_logql::ComparisonOp;

    use crate::tests::prelude::{ScalarOperands, ScalarSide};

    let one = MetricValue::new(1, 1);
    let two = MetricValue::new(2, 1);
    let matches = |sample, op, scalar, scalar_side| {
        crate::tests::prelude::metric_scalar_comparison_matches(
            ScalarOperands {
                sample,
                scalar,
                scalar_side,
            },
            op,
        )
    };

    // The ordering of left against right, the sample, scalar, and scalar side.
    let cases = [
        ComparisonCase {
            ordering: Ordering::Less,
            sample: one,
            scalar: two,
            scalar_side: ScalarSide::Right,
        },
        ComparisonCase {
            ordering: Ordering::Greater,
            sample: one,
            scalar: two,
            scalar_side: ScalarSide::Left,
        },
        ComparisonCase {
            ordering: Ordering::Greater,
            sample: two,
            scalar: one,
            scalar_side: ScalarSide::Right,
        },
        ComparisonCase {
            ordering: Ordering::Less,
            sample: two,
            scalar: one,
            scalar_side: ScalarSide::Left,
        },
        ComparisonCase {
            ordering: Ordering::Equal,
            sample: one,
            scalar: one,
            scalar_side: ScalarSide::Right,
        },
        ComparisonCase {
            ordering: Ordering::Equal,
            sample: one,
            scalar: one,
            scalar_side: ScalarSide::Left,
        },
    ];
    for case in cases {
        let want = |op| match op {
            ComparisonOp::Equal => case.ordering == Ordering::Equal,
            ComparisonOp::NotEqual => case.ordering != Ordering::Equal,
            ComparisonOp::Greater => case.ordering == Ordering::Greater,
            ComparisonOp::GreaterEqual => case.ordering != Ordering::Less,
            ComparisonOp::Less => case.ordering == Ordering::Less,
            ComparisonOp::LessEqual => case.ordering != Ordering::Greater,
            ComparisonOp::RegexEqual | ComparisonOp::RegexNotEqual => false,
        };
        for op in [
            ComparisonOp::Equal,
            ComparisonOp::NotEqual,
            ComparisonOp::Greater,
            ComparisonOp::GreaterEqual,
            ComparisonOp::Less,
            ComparisonOp::LessEqual,
            ComparisonOp::RegexEqual,
            ComparisonOp::RegexNotEqual,
        ] {
            check!(
                matches(case.sample, op, case.scalar, case.scalar_side) == want(op),
                "{op:?} at {:?} with the scalar on the {:?}",
                case.ordering,
                case.scalar_side
            );
        }
    }

    // Spelled out for the case the table exists to protect: the scalar's
    // side changes the answer for an ordered operator and not for equality.
    check!(
        matches(one, ComparisonOp::Less, two, ScalarSide::Right),
        "x < 1 where x is smaller"
    );
    check!(
        !matches(one, ComparisonOp::Less, two, ScalarSide::Left),
        "but 1 < x is not"
    );
    check!(matches(one, ComparisonOp::Equal, one, ScalarSide::Right));
    check!(
        matches(one, ComparisonOp::Equal, one, ScalarSide::Left),
        "equality is side-blind"
    );
}

struct ResponseEntry<'a> {
    timestamp: i64,
    line: &'a str,
}

/// `count_loki_stream_result_hot_tail_lines` reports how many lines of a
/// response came from the hot tail rather than from blocks. It is a
/// MULTISET match-off: each hot record can account for one response line
/// and no more, so two identical records admit two identical lines and a
/// third line is attributed to the blocks.
///
/// That counting-down is the whole design, and one record with one line
/// cannot show it -- a plain membership test would agree.
///
/// The key uses the query's output labels. Plain stored records contribute
/// only their existing labels; the later parser/format pipeline changes both
/// labels and line before matching them against the independent response.
#[test]
pub(crate) fn hot_tail_lines_are_matched_off_one_record_at_a_time() {
    use krabka_logql::StreamPlan;

    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    let plan = StreamPlan {
        tenant: "tenant".to_string(),
        time_range: krabka_blockstore::TimeRange::new(0, 100).expect("a valid range"),
        query: krabka_logql::parse_query("{app=\"api\"}").expect("the query parses"),
        fingerprints: BTreeSet::new(),
        blocks: Vec::new(),
    };
    let record = |tenant: &str, timestamp_ns, line: &str| crate::tests::prelude::WalLogRecord {
        tenant: tenant.to_string(),
        labels: labels.clone(),
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
        position: None,
    };
    let response = |entries: &[ResponseEntry<'_>]| {
        serde_json::json!({
            "data": {"result": [{
                "stream": {"app": "api"},
                "values": entries
                    .iter()
                    .map(|entry| serde_json::json!([entry.timestamp.to_string(), entry.line]))
                    .collect::<Vec<_>>(),
            }]}
        })
    };
    let response_entry = |timestamp, line| ResponseEntry { timestamp, line };
    let open = crate::tests::prelude::CompactionFrontier::new(0);
    let counted = |value: &serde_json::Value, hot: &[crate::tests::prelude::WalLogRecord]| {
        crate::tests::prelude::count_loki_stream_result_hot_tail_lines(value, &plan, hot, &open)
    };

    // One record, one matching line.
    check!(
        counted(
            &response(&[response_entry(10, "a")]),
            &[record("tenant", 10, "a")]
        ) == 1
    );

    // Querying a stored row with no discovery metadata cannot synthesize it.
    let mut invented_level = response(&[response_entry(10, "a")]);
    invented_level["data"]["result"][0]["stream"]["detected_level"] = "unknown".into();
    check!(counted(&invented_level, &[record("tenant", 10, "a")]) == 0);

    // A line the hot tail does not hold came from the blocks.
    check!(
        counted(
            &response(&[response_entry(10, "b")]),
            &[record("tenant", 10, "a")]
        ) == 0
    );
    check!(
        counted(
            &response(&[response_entry(20, "a")]),
            &[record("tenant", 10, "a")]
        ) == 0
    );

    // The multiset: two identical records admit two identical lines, and
    // a third is attributed to the blocks rather than counted again.
    let twice = [record("tenant", 10, "a"), record("tenant", 10, "a")];
    check!(counted(&response(&[response_entry(10, "a")]), &twice) == 1);
    check!(
        counted(
            &response(&[response_entry(10, "a"), response_entry(10, "a")]),
            &twice
        ) == 2
    );
    check!(
        counted(
            &response(&[
                response_entry(10, "a"),
                response_entry(10, "a"),
                response_entry(10, "a")
            ]),
            &twice
        ) == 2,
        "two records cannot account for three lines"
    );

    // A shared label set does not make the lines interchangeable. This also
    // exercises repeated timestamps and rejected source labels in the cache.
    let mut rejected = record("tenant", 10, "a");
    rejected
        .labels
        .insert("app".to_string(), "other".to_string());
    check!(
        counted(
            &response(&[
                response_entry(10, "a"),
                response_entry(10, "b"),
                response_entry(20, "c")
            ]),
            &[
                rejected.clone(),
                record("tenant", 10, "a"),
                record("tenant", 10, "b"),
                record("tenant", 20, "c"),
                rejected,
            ],
        ) == 3
    );
    for timestamp in ["010", "+10", "invalid"] {
        let mut malformed = response(&[response_entry(10, "a")]);
        malformed["data"]["result"][0]["values"][0][0] = timestamp.into();
        check!(counted(&malformed, &[record("tenant", 10, "a")]) == 0);
    }

    // Per-record metadata still changes the output stream, even when two
    // records have identical source labels, timestamps and text.
    let mut with_metadata = record("tenant", 10, "a");
    with_metadata
        .structured_metadata
        .insert("request".to_string(), "first".to_string());
    let mut other_metadata = with_metadata.clone();
    other_metadata
        .structured_metadata
        .insert("request".to_string(), "second".to_string());
    let mut metadata_response = response(&[response_entry(10, "a"), response_entry(10, "a")]);
    metadata_response["data"]["result"][0]["stream"]["request"] = "first".into();
    check!(counted(&metadata_response, &[with_metadata, other_metadata]) == 1);

    // Parser, label-format and line-format stages are evaluated for each
    // record. The golden response uses the rendered line and output labels.
    let pipeline_plan = StreamPlan {
        query: krabka_logql::parse_query(
            r#"{app="api"} | json | label_format app="{{.message}}" | line_format "{{.message}}""#,
        )
        .expect("the pipeline parses"),
        ..plan.clone()
    };
    let pipeline_response = serde_json::json!({"data": {"result": [{
        "stream": {"app": "hello", "message": "hello"},
        "values": [["10", "hello"], ["10", "hello"]],
    }]}});
    check!(
        crate::tests::prelude::count_loki_stream_result_hot_tail_lines(
            &pipeline_response,
            &pipeline_plan,
            &[
                record("tenant", 10, r#"{"message":"hello"}"#),
                record("tenant", 10, r#"{"message":"other"}"#),
            ],
            &open,
        ) == 1
    );

    // Hot records are filtered before the match-off: another tenant, a
    // timestamp outside the plan's range, and one already compacted.
    check!(
        counted(
            &response(&[response_entry(10, "a")]),
            &[record("other", 10, "a")]
        ) == 0
    );
    // Both ends of the range, since they are separate clauses. Past the
    // end is straightforward. BEFORE the start needs its own plan: with
    // this plan starting at zero, anything earlier is also behind the
    // compaction frontier and would be filtered by that instead, leaving
    // the start-of-range clause untested.
    check!(
        counted(
            &response(&[response_entry(200, "a")]),
            &[record("tenant", 200, "a")]
        ) == 0
    );
    let later_plan = StreamPlan {
        time_range: krabka_blockstore::TimeRange::new(50, 100).expect("a valid range"),
        ..plan.clone()
    };
    check!(
        crate::tests::prelude::count_loki_stream_result_hot_tail_lines(
            &response(&[response_entry(10, "a")]),
            &later_plan,
            &[record("tenant", 10, "a")],
            &open,
        ) == 0,
        "before the range start, but after the frontier"
    );
    let compacted = crate::tests::prelude::CompactionFrontier::new(50);
    check!(
        crate::tests::prelude::count_loki_stream_result_hot_tail_lines(
            &response(&[response_entry(10, "a")]),
            &plan,
            &[record("tenant", 10, "a")],
            &compacted,
        ) == 0,
        "a compacted record is the blocks' line, not the hot tail's"
    );

    // Nothing on either side.
    check!(counted(&response(&[]), &[record("tenant", 10, "a")]) == 0);
    check!(counted(&response(&[response_entry(10, "a")]), &[]) == 0);
}

struct StructuredMetadataCase {
    timestamp_ns: i64,
    trace_id: &'static str,
}

/// Structured metadata reaches the pipeline as fields, which is how a filter
/// such as `| trace_id="abc"` matches on it, and Loki's default encoding leaves
/// it there: the metadata is folded into the stream's label map, so two rows
/// that differ only by `trace_id` come back as two streams of one entry each,
/// and every entry stays two elements long.
///
/// `categorize-labels` is the encoding that pulls the metadata back out. The
/// two streams become one again -- the stream Grafana's log browser shows --
/// and each entry names what it carried. These stored rows contain only the
/// explicit trace metadata; querying them does not discover additional fields.
#[test]
pub(crate) fn structured_metadata_splits_a_stream_unless_the_request_categorizes_labels() {
    let ApiSeriesPlan {
        label_index,
        api,
        plan,
    } = ApiSeriesPlan::new("{app=\"api\"}");

    let mut streams = BTreeMap::new();
    for case in [
        StructuredMetadataCase {
            timestamp_ns: 10,
            trace_id: "abc",
        },
        StructuredMetadataCase {
            timestamp_ns: 20,
            trace_id: "def",
        },
    ] {
        let mut structured_metadata = Labels::default();
        structured_metadata.insert("trace_id".to_string(), case.trace_id.to_string());
        append_matching_log_row(
            &mut streams,
            &plan,
            &label_index,
            QueryRow {
                fingerprint: api,
                timestamp_ns: case.timestamp_ns,
                line: "api error",
                structured_metadata: &structured_metadata,
            },
            &[],
        )
        .expect("the plan names the series");
    }

    check!(
        loki_streams_response(streams.clone(), LokiStreamEncoding::Folded)
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {
                                "app": "api",
                                "trace_id": "abc"
                            },
                            "values": [["10", "api error"]]
                        },
                        {
                            "stream": {
                                "app": "api",
                                "trace_id": "def"
                            },
                            "values": [["20", "api error"]]
                        }
                    ]
                }
            })
    );

    check!(
        loki_streams_response(streams, LokiStreamEncoding::CategorizeLabels)
            == json!({
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": [
                        {
                            "stream": {"app": "api"},
                            "values": [
                                [
                                    "10",
                                    "api error",
                                    {
                                        "structuredMetadata": {
                                            "trace_id": "abc"
                                        }
                                    }
                                ],
                                [
                                    "20",
                                    "api error",
                                    {
                                        "structuredMetadata": {
                                            "trace_id": "def"
                                        }
                                    }
                                ]
                            ]
                        }
                    ]
                }
            })
    );
}

mod a_pushed_line_filter_predicate_selects_the_same_lines_as_the_rust_filter {
use super::*;

/// The corpus is chosen for the characters that have to survive two escaping
/// layers, or that betray one that is missing: quotes, backslashes, `LIKE`
/// wildcards, regex metacharacters, an embedded newline, and multi-byte text.
fn corpus() -> Vec<&'static str> {
    vec![
        "level=error msg=boom",
        "level=warn msg=boom",
        "100% cpu",
        "a_b_c",
        "path C:\\Users\\logs",
        "he said 'hi'",
        "err0r",
        "error",
        "panic:\ngoroutine 1",
        "поток error",
        "a.b",
        "axb",
        "start middle end",
        "start end",
        "endstart",
        "status 503",
        "",
    ]
}

async fn lines_selected_by_predicates(predicates: &[String]) -> Vec<String> {
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("line", DataType::Utf8, false)])),
        vec![Arc::new(StringArray::from(corpus())) as ArrayRef],
    )
    .expect("corpus batch");
    let ctx = SessionContext::new();
    ctx.register_batch("logs", batch).expect("register corpus");

    let sql = format!("select line from logs where {}", predicates.join(" and "));
    let batches = ctx
        .sql(&sql)
        .await
        .unwrap_or_else(|error| panic!("{sql} plans: {error}"))
        .collect()
        .await
        .unwrap_or_else(|error| panic!("{sql} runs: {error}"));

    batches
        .iter()
        .flat_map(|batch| {
            let lines = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("line column is Utf8");
            (0..batch.num_rows())
                .map(|row| lines.value(row).to_string())
                .collect::<Vec<_>>()
        })
        .collect()
}

fn lines_selected_by_pipeline(pipeline: &[PipelineStage]) -> Vec<String> {
    corpus()
        .into_iter()
        .filter(|line| pipeline.iter().all(|stage| stage.matches(line)))
        .map(str::to_string)
        .collect()
}

/// Each case is a query and the number of its line filters that reach SQL.
///
/// A case that pushes every one of its filters must select exactly the lines
/// the Rust pipeline selects. A case that pushes fewer may only select more:
/// the scan prunes, and the row-by-row pass decides.
#[tokio::test]
pub(crate) async fn a_pushed_line_filter_predicate_selects_the_same_lines_as_the_rust_filter() {
    struct PredicatePushCase {
        query: &'static str,
        expected_predicates: usize,
    }
    let cases = [
        PredicatePushCase {
            query: r#"{app="a"} |= "error""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} != "error""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |= "100%""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |= "a_b""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |= "C:\\Users""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |= "'hi'""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |~ "err(0|o)r""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} !~ "err(0|o)r""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |~ "^level=error""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |~ "a.b""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |~ "a\\.b""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |~ "(?i)ERROR""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |~ "5\\d\\d""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |~ "100%""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |~ "'hi'""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |> "start<_>end""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} !> "start<_>end""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |> "100%<_>cpu""#,
            expected_predicates: 1,
        },
        PredicatePushCase {
            query: r#"{app="a"} |= "level=" |~ "err(0|o)r" |> "level=<_>r""#,
            expected_predicates: 3,
        },
        // A regex whose literal text can hold a backslash is not pushed:
        // DataFusion would rewrite it into a LIKE that reads the backslash as
        // an escape and drops the lines it should keep.
        PredicatePushCase {
            query: r#"{app="a"} |~ "C:\\\\Users""#,
            expected_predicates: 0,
        },
        PredicatePushCase {
            query: r#"{app="a"} !~ "C:\\\\Users""#,
            expected_predicates: 0,
        },
        PredicatePushCase {
            query: r#"{app="a"} |~ "\\x41""#,
            expected_predicates: 0,
        },
        // An `ip(...)` filter and a `<_>`-only pattern have no SQL form.
        PredicatePushCase {
            query: r#"{app="a"} |= ip("127.0.0.1")"#,
            expected_predicates: 0,
        },
        PredicatePushCase {
            query: r#"{app="a"} |> "<_>""#,
            expected_predicates: 0,
        },
    ];

    for case in cases {
        let parsed = parse_query(case.query).expect("query parses");
        let predicates = line_filter_sql_predicates(&parsed.pipeline);
        check!(
            predicates.len() == case.expected_predicates,
            "{}",
            case.query
        );

        let rust = lines_selected_by_pipeline(&parsed.pipeline);
        if predicates.is_empty() {
            continue;
        }
        let pushed = lines_selected_by_predicates(&predicates).await;
        if predicates.len() == parsed.pipeline.len() {
            check!(pushed == rust, "{}", case.query);
        } else {
            check!(
                rust.iter().all(|line| pushed.contains(line)),
                "{}",
                case.query
            );
        }
    }
}
}
