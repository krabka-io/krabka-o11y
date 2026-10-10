//! `TraceQL` planner entry points.

mod selector;

use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use datafusion::{catalog::MemTable, logical_expr::LogicalPlan, prelude::SessionContext};
use krabka_units::ByteSize;

use crate::{
    ast::{
        Aggregate, ComparisonOp, Field, FieldExpr, Intrinsic, Pipeline, Query, Scope, SpansetExpr,
        StructuralOp,
    },
    error::{Result, TraceqlError},
    ids::UnixNano,
    span_columns::{COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_SPAN_ID, COL_TRACE_ID},
    store::{MatchCmp, MatchScope, MatchValue, ScanOptions, SpanMatcher, SpanStore},
};

#[cfg(test)]
mod tests {
    use arrow::{array::Array, record_batch::RecordBatch};
    use assert2::{assert, check};
    use datafusion::arrow::array::AsArray;
    use krabka_units::nanos;

    use super::*;
    use crate::{
        InMemorySpanStore,
        ast::Value,
        parser::parse,
        result::{AttrValue, EventRef},
        span_columns::{COL_NAME, InputSpan},
    };

    /// A span of trace `[trace; 16]` that starts `id` nanoseconds after the
    /// epoch. The default is root span 1 of trace `[1; 16]`, with no
    /// attributes.
    struct TraceSpan<'a> {
        id: u8,
        parent: Option<u8>,
        trace: u8,
        name: &'a str,
        duration_nanos: i64,
        attrs: Vec<(&'a str, AttrValue)>,
    }

    impl Default for TraceSpan<'_> {
        fn default() -> Self {
            Self {
                id: 1,
                parent: None,
                trace: 1,
                name: "span",
                duration_nanos: 0,
                attrs: Vec::new(),
            }
        }
    }

    impl TraceSpan<'_> {
        fn build(self) -> InputSpan {
            InputSpan {
                trace_id: [self.trace; 16],
                start_unix_nano: i64::from(self.id),
                ..crate::testkit::span::span(
                    0,
                    self.id,
                    self.parent,
                    self.name,
                    self.duration_nanos,
                    self.attrs,
                )
            }
        }
    }

    fn span(id: u8, name: &str, duration_nanos: i64, attrs: Vec<(&str, AttrValue)>) -> InputSpan {
        TraceSpan {
            id,
            name,
            duration_nanos,
            attrs,
            ..TraceSpan::default()
        }
        .build()
    }

    const CACHE_SPAN_NAMES: [&str; 3] = ["cache-a", "cache-b", "cache-c"];

    /// A root span carrying one `svc` attribute. The default is span 1 of
    /// trace `[1; 16]` in service `api`.
    struct SvcSpan {
        id: u8,
        trace: u8,
        name: &'static str,
        duration_nanos: i64,
        svc: &'static str,
    }

    impl Default for SvcSpan {
        fn default() -> Self {
            Self {
                id: 1,
                trace: 1,
                name: "api-a",
                duration_nanos: 0,
                svc: "api",
            }
        }
    }

    impl SvcSpan {
        fn build(self) -> InputSpan {
            TraceSpan {
                id: self.id,
                trace: self.trace,
                name: self.name,
                duration_nanos: self.duration_nanos,
                attrs: vec![("svc", AttrValue::Str(self.svc.into()))],
                ..TraceSpan::default()
            }
            .build()
        }
    }

    fn store_with_traces(traces: Vec<Vec<InputSpan>>) -> InMemorySpanStore {
        let mut store = InMemorySpanStore::new();
        for spans in traces {
            store.push_trace("t", "svc", "root", spans);
        }
        store
    }

    // Two `api` spans and one `db` span.
    fn api_db_spans() -> Vec<InputSpan> {
        vec![
            SvcSpan {
                id: 1,
                name: "api-a",
                duration_nanos: 20,
                svc: "api",
                ..SvcSpan::default()
            }
            .build(),
            SvcSpan {
                id: 2,
                name: "api-b",
                duration_nanos: 40,
                svc: "api",
                ..SvcSpan::default()
            }
            .build(),
            SvcSpan {
                id: 3,
                name: "db-a",
                duration_nanos: 200,
                svc: "db",
                ..SvcSpan::default()
            }
            .build(),
        ]
    }

    // `api_db_spans` in one trace.
    fn api_db_store() -> InMemorySpanStore {
        store_with_traces(vec![api_db_spans()])
    }

    // `api_db_spans` plus three 10ns `cache` spans in the same trace.
    fn api_db_cache_store() -> InMemorySpanStore {
        let mut spans = api_db_spans();
        spans.extend((4..).zip(CACHE_SPAN_NAMES).map(|(id, name)| {
            SvcSpan {
                id,
                name,
                duration_nanos: 10,
                svc: "cache",
                ..SvcSpan::default()
            }
            .build()
        }));
        store_with_traces(vec![spans])
    }

    // A fast `api` trace (20ns, 40ns) and a slow one (200ns, 400ns).
    fn fast_slow_store() -> InMemorySpanStore {
        store_with_traces(vec![
            vec![
                SvcSpan {
                    id: 1,
                    trace: 1,
                    name: "fast-a",
                    duration_nanos: 20,
                    ..SvcSpan::default()
                }
                .build(),
                SvcSpan {
                    id: 2,
                    trace: 1,
                    name: "fast-b",
                    duration_nanos: 40,
                    ..SvcSpan::default()
                }
                .build(),
            ],
            vec![
                SvcSpan {
                    id: 3,
                    trace: 2,
                    name: "slow-a",
                    duration_nanos: 200,
                    ..SvcSpan::default()
                }
                .build(),
                SvcSpan {
                    id: 4,
                    trace: 2,
                    name: "slow-b",
                    duration_nanos: 400,
                    ..SvcSpan::default()
                }
                .build(),
            ],
        ])
    }

    // Two attribute-less spans in one trace and one in another.
    fn ungrouped_two_trace_store() -> InMemorySpanStore {
        store_with_traces(vec![
            vec![span(1, "api-a", 20, vec![]), span(2, "api-b", 40, vec![])],
            vec![
                TraceSpan {
                    id: 3,
                    trace: 2,
                    name: "db-a",
                    duration_nanos: 200,
                    ..TraceSpan::default()
                }
                .build(),
            ],
        ])
    }

    async fn execute(planned: PlannedSpanset) -> Result<Vec<RecordBatch>> {
        Ok(planned
            .ctx
            .execute_logical_plan(planned.plan)
            .await?
            .collect()
            .await?)
    }

    async fn planned(query: &str, store: &InMemorySpanStore) -> Result<Vec<RecordBatch>> {
        let q = parse(query)?;
        execute(
            plan_query(
                store,
                &PlannerContext {
                    tenant: "t".into(),
                    start_ns: UnixNano(0),
                    end_ns: UnixNano(10_000),
                    scan_options: ScanOptions::default(),
                },
                &q,
            )
            .await?,
        )
        .await
    }

    fn first_name(batches: &[RecordBatch]) -> String {
        batches[0]
            .column_by_name(COL_NAME)
            .unwrap()
            .as_string::<i32>()
            .value(0)
            .to_string()
    }

    fn names(batches: &[RecordBatch]) -> Vec<String> {
        let mut out = Vec::new();
        for batch in batches {
            let arr = batch.column_by_name(COL_NAME).unwrap().as_string::<i32>();
            for i in 0..arr.len() {
                out.push(arr.value(i).to_string());
            }
        }
        out.sort_unstable();
        out
    }

    fn span_ids(batches: &[RecordBatch]) -> Vec<[u8; 8]> {
        let mut out = Vec::new();
        for batch in batches {
            let arr = batch
                .column_by_name(crate::COL_SPAN_ID)
                .unwrap()
                .as_fixed_size_binary();
            for i in 0..arr.len() {
                out.push(arr.value(i).try_into().unwrap());
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    fn test_field(scope: Scope, key: &str) -> Field {
        Field {
            scope,
            key: key.into(),
        }
    }

    #[tokio::test]
    async fn field_comparisons_execute_typed_operands_through_structural_and_metric_plans() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(
                    1,
                    "equal",
                    10,
                    vec![("lhs", AttrValue::Int(3)), ("rhs", AttrValue::Int(3))],
                ),
                span(
                    2,
                    "different",
                    20,
                    vec![("lhs", AttrValue::Int(4)), ("rhs", AttrValue::Int(3))],
                ),
                span(3, "missing", 30, vec![("lhs", AttrValue::Int(3))]),
                span(
                    4,
                    "wrong-type",
                    40,
                    vec![
                        ("text", AttrValue::Str("3".into())),
                        ("rhs", AttrValue::Int(3)),
                    ],
                ),
            ],
        );
        for (query, expected) in [
            ("{ .lhs = .rhs }", vec!["equal"]),
            ("{ .lhs > .rhs }", vec!["different"]),
            ("{ .lhs != .rhs }", vec!["different"]),
            ("{ .text = .rhs }", vec![]),
            ("{ name = \"equal\" } && { .lhs = .rhs }", vec!["equal"]),
            (
                "{ .lhs = .rhs || name = \"wrong-type\" }",
                vec!["equal", "wrong-type"],
            ),
        ] {
            assert!(names(&planned(query, &store).await.unwrap()) == expected);
        }
        let engine = crate::TraceqlEngine::new(Arc::new(store), crate::EngineOpts::default());
        let metrics = engine
            .query_range("t", "{ .lhs = .rhs } | count_over_time()", 0, 100, 100)
            .await
            .unwrap();
        assert!(metrics.series[0].points == vec![(0, 1.0), (100, 0.0)]);
    }

    #[test]
    fn aggregate_pipeline_projects_nested_value_fields() {
        let matchers = pipeline_nested_projection_matchers(&[Pipeline::Aggregate(Aggregate::Avg(
            test_field(Scope::Event, "http.method"),
        ))]);
        assert!(
            matchers
                == vec![SpanMatcher {
                    scope: MatchScope::Event,
                    key: "http.method".into(),
                    op: MatchCmp::Neq,
                    value: MatchValue::Nil,
                    negated: false,
                }]
        );

        assert!(
            pipeline_nested_projection_matchers(&[Pipeline::Aggregate(Aggregate::Count)])
                .is_empty()
        );
    }

    #[test]
    fn collect_nested_selectors_keeps_only_unique_nested_selectors() {
        let nested = FieldExpr::Comparison {
            lhs: test_field(Scope::Event, "http.method"),
            op: ComparisonOp::Eq,
            rhs: Value::Str("GET".into()),
        };
        let non_nested = FieldExpr::Comparison {
            lhs: test_field(Scope::Span, "http.method"),
            op: ComparisonOp::Eq,
            rhs: Value::Str("GET".into()),
        };
        let root = SpansetExpr::And(
            Box::new(SpansetExpr::Selector(Box::new(nested.clone()))),
            Box::new(SpansetExpr::And(
                Box::new(SpansetExpr::Selector(Box::new(non_nested))),
                Box::new(SpansetExpr::Selector(Box::new(nested.clone()))),
            )),
        );

        let mut selectors = Vec::new();
        collect_nested_selectors(&root, &mut selectors);

        assert!(selectors == vec![nested]);
    }

    #[test]
    fn pipeline_to_sql_empty_pipeline_uses_passthrough_query() {
        assert!(
            pipeline_to_sql("SELECT * FROM spans", &[]).unwrap()
                == "SELECT * FROM (SELECT * FROM spans) AS q"
        );
    }

    #[test]
    fn grouped_no_filter_by_only_accepts_search_preserving_aggregates() {
        let by = vec![test_field(Scope::Span, "svc")];
        let passing = vec![
            Pipeline::Aggregate(Aggregate::Count),
            Pipeline::By(by.clone()),
        ];
        assert!(grouped_no_filter_by(&passing).unwrap() == by.as_slice());

        let non_preserving = vec![
            Pipeline::Aggregate(Aggregate::CountOverTime),
            Pipeline::By(by),
        ];
        check!(grouped_no_filter_by(&non_preserving).is_none());
        check!(is_search_preserving_aggregate(&Aggregate::Count));
        check!(!is_search_preserving_aggregate(&Aggregate::CountOverTime));
    }

    #[test]
    fn aggregate_sql_helpers_cover_scalar_aggregates() {
        let field = test_field(Scope::Intrinsic(Intrinsic::Duration), "duration");
        for (aggregate, prefix) in [
            (Aggregate::Sum(field.clone()), "SUM("),
            (Aggregate::Avg(field.clone()), "AVG("),
            (Aggregate::Min(field.clone()), "MIN("),
            (Aggregate::Max(field), "MAX("),
        ] {
            assert!(aggregate_expr_sql(&aggregate).unwrap().starts_with(prefix));
            assert!(
                aggregate_rank_expr_sql(&aggregate)
                    .unwrap()
                    .starts_with(prefix)
            );
        }
        assert!(aggregate_rank_expr_sql(&Aggregate::CountOverTime).is_err());
    }

    #[tokio::test]
    async fn selector_matches_attribute_value() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(
                    1,
                    "root",
                    50,
                    vec![("http.method", AttrValue::Str("GET".into()))],
                ),
                span(
                    2,
                    "db",
                    50,
                    vec![("http.method", AttrValue::Str("POST".into()))],
                ),
            ],
        );
        let out = planned("{ .http.method = \"GET\" }", &store).await.unwrap();
        assert!(out.iter().map(RecordBatch::num_rows).sum::<usize>() == 1);
        assert!(first_name(&out) == "root");
    }

    #[tokio::test]
    async fn selector_matches_intrinsic_duration() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![span(1, "short", 50, vec![]), span(2, "long", 150, vec![])],
        );
        let out = planned("{ span:duration > 100 }", &store).await.unwrap();
        assert!(out.iter().map(RecordBatch::num_rows).sum::<usize>() == 1);
        assert!(first_name(&out) == "long");
    }

    #[tokio::test]
    async fn grouped_pipeline_filters_by_nested_event_intrinsic() {
        let mut miss_one = span(1, "miss-one", 50, vec![]);
        miss_one.events = vec![EventRef {
            time_since_start: nanos(10),
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut miss_two = span(2, "miss-two", 50, vec![]);
        miss_two.events = vec![EventRef {
            time_since_start: nanos(20),
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut hit = span(3, "hit", 50, vec![]);
        hit.events = vec![EventRef {
            time_since_start: nanos(30),
            name: "cache.hit".into(),
            attributes: Vec::new(),
        }];
        let mut store = InMemorySpanStore::new();
        store.push_trace("t", "svc", "root", vec![miss_one, miss_two, hit]);

        let out = planned(
            "{ event:name != nil } | count() by (event:name) > 1",
            &store,
        )
        .await
        .unwrap();

        assert!(names(&out) == vec!["miss-one".to_string(), "miss-two".to_string()]);
    }

    #[tokio::test]
    async fn grouped_pipeline_by_nested_event_intrinsic_ignores_later_event_decoys() {
        let mut one = span(1, "one", 50, vec![("svc", AttrValue::Str("api".into()))]);
        one.events = vec![
            EventRef {
                time_since_start: nanos(10),
                name: "cache.miss".into(),
                attributes: Vec::new(),
            },
            EventRef {
                time_since_start: nanos(20),
                name: "cache.hit".into(),
                attributes: Vec::new(),
            },
        ];
        let mut two = span(2, "two", 50, vec![("svc", AttrValue::Str("api".into()))]);
        two.events = vec![
            EventRef {
                time_since_start: nanos(30),
                name: "cache.wait".into(),
                attributes: Vec::new(),
            },
            EventRef {
                time_since_start: nanos(40),
                name: "cache.hit".into(),
                attributes: Vec::new(),
            },
        ];
        let mut store = InMemorySpanStore::new();
        store.push_trace("t", "svc", "root", vec![one, two]);

        let out = planned("{ .svc = \"api\" } | count() by (event:name) > 1", &store)
            .await
            .unwrap();

        assert!(names(&out).is_empty());
    }

    #[tokio::test]
    async fn intra_brace_and_matches_one_span() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "a-only", 50, vec![("a", AttrValue::Int(1))]),
                span(2, "b-only", 50, vec![("b", AttrValue::Int(2))]),
                span(
                    3,
                    "both",
                    50,
                    vec![("a", AttrValue::Int(1)), ("b", AttrValue::Int(2))],
                ),
            ],
        );
        let out = planned("{ .a = 1 && .b = 2 }", &store).await.unwrap();
        assert!(out.iter().map(RecordBatch::num_rows).sum::<usize>() == 1);
        assert!(first_name(&out) == "both");
    }

    #[tokio::test]
    async fn regex_is_fully_anchored() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "one", 50, vec![("name", AttrValue::Str("abc".into()))]),
                span(2, "two", 50, vec![("name", AttrValue::Str("xabc".into()))]),
            ],
        );
        let out = planned("{ .name =~ \"ab.*\" }", &store).await.unwrap();
        assert!(out.iter().map(RecordBatch::num_rows).sum::<usize>() == 1);
        assert!(first_name(&out) == "one");
    }

    #[tokio::test]
    async fn inter_brace_and_matches_different_spans_same_trace() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                TraceSpan {
                    id: 1,
                    trace: 1,
                    name: "a-only",
                    duration_nanos: 50,
                    attrs: vec![("a", AttrValue::Int(1))],
                    ..TraceSpan::default()
                }
                .build(),
                TraceSpan {
                    id: 2,
                    trace: 1,
                    name: "b-only",
                    duration_nanos: 50,
                    attrs: vec![("b", AttrValue::Int(2))],
                    ..TraceSpan::default()
                }
                .build(),
            ],
        );
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                TraceSpan {
                    id: 3,
                    trace: 2,
                    name: "other-a",
                    duration_nanos: 50,
                    attrs: vec![("a", AttrValue::Int(1))],
                    ..TraceSpan::default()
                }
                .build(),
            ],
        );

        let out = planned("{ .a = 1 } && { .b = 2 }", &store).await.unwrap();
        assert!(names(&out) == vec!["a-only".to_string(), "b-only".to_string()]);
    }

    fn structural_store() -> InMemorySpanStore {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                TraceSpan {
                    id: 1,
                    trace: 9,
                    name: "root",
                    duration_nanos: 1,
                    attrs: vec![("svc", AttrValue::Str("a".into()))],
                    ..TraceSpan::default()
                }
                .build(),
                TraceSpan {
                    id: 2,
                    parent: Some(1),
                    trace: 9,
                    name: "child-x",
                    duration_nanos: 1,
                    attrs: vec![("svc", AttrValue::Str("b".into()))],
                }
                .build(),
                TraceSpan {
                    id: 4,
                    parent: Some(2),
                    trace: 9,
                    name: "grand-y",
                    duration_nanos: 1,
                    attrs: vec![("svc", AttrValue::Str("c".into()))],
                }
                .build(),
                TraceSpan {
                    id: 3,
                    parent: Some(1),
                    trace: 9,
                    name: "child-z",
                    duration_nanos: 1,
                    attrs: vec![("svc", AttrValue::Str("b".into()))],
                }
                .build(),
            ],
        );
        store.push_trace(
            "t",
            "svc",
            "other-root",
            vec![
                TraceSpan {
                    id: 5,
                    trace: 8,
                    name: "other-root",
                    duration_nanos: 1,
                    attrs: vec![("svc", AttrValue::Str("a".into()))],
                    ..TraceSpan::default()
                }
                .build(),
                TraceSpan {
                    id: 6,
                    parent: Some(5),
                    trace: 8,
                    name: "other-child",
                    duration_nanos: 1,
                    attrs: vec![("svc", AttrValue::Str("d".into()))],
                }
                .build(),
            ],
        );
        store
    }

    #[tokio::test]
    async fn structural_descendant_returns_rhs_descendant_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } >> { .svc = \"c\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[4; 8]]);
    }

    #[tokio::test]
    async fn structural_child_returns_rhs_direct_children() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } > { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn structural_sibling_excludes_self() {
        let store = structural_store();
        let out = planned("{ .svc = \"b\" } ~ { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn negated_sibling_returns_rhs_spans_without_sibling_match() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } !~ { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn structural_ancestor_returns_rhs_ancestor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } << { .svc = \"a\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[1; 8]]);
    }

    #[tokio::test]
    async fn structural_parent_returns_direct_parent_only() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } < { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8]]);
    }

    #[tokio::test]
    async fn structural_join_is_trace_isolated() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } >> { .svc = \"d\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[6; 8]]);
    }

    #[tokio::test]
    async fn negated_ancestor_returns_rhs_spans_without_anchor_match() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } !<< { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[3; 8]]);
    }

    #[tokio::test]
    async fn negated_descendant_returns_rhs_spans_without_descendant_match() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } !>> { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn negated_child_returns_rhs_spans_without_direct_child_match() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } !> { .svc = \"c\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[4; 8]]);
    }

    #[tokio::test]
    async fn negated_parent_uses_parent_id_anti_join() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } !< { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[3; 8]]);
    }

    #[tokio::test]
    async fn union_descendant_returns_rhs_and_anchor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"b\" } &>> { .svc = \"c\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [4; 8]]);
    }

    #[tokio::test]
    async fn union_ancestor_returns_rhs_and_anchor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } &<< { .svc = \"a\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[1; 8], [4; 8]]);
    }

    #[tokio::test]
    async fn union_child_returns_rhs_and_anchor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"a\" } &> { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[1; 8], [2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn union_parent_returns_rhs_and_anchor_spans() {
        let store = structural_store();
        let out = planned("{ .svc = \"c\" } &< { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [4; 8]]);
    }

    #[tokio::test]
    async fn union_sibling_deduplicates_spans_matching_both_sides() {
        let store = structural_store();
        let out = planned("{ .svc = \"b\" } &~ { .svc = \"b\" }", &store)
            .await
            .unwrap();
        assert!(span_ids(&out) == vec![[2; 8], [3; 8]]);
    }

    #[tokio::test]
    async fn count_by_filter_keeps_spans_from_passing_groups() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 1, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 1, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 1, vec![("svc", AttrValue::Str("db".into()))]),
            ],
        );
        let out = planned("{ .svc != nil } | count() | by(span.svc) > 1", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn count_filter_accepts_literal_arithmetic_threshold() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 1, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 1, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "api-c", 1, vec![("svc", AttrValue::Str("api".into()))]),
                span(4, "db-a", 1, vec![("svc", AttrValue::Str("db".into()))]),
            ],
        );

        let out = planned("{ .svc != nil } | count() | by(span.svc) > 1 + 1", &store)
            .await
            .unwrap();

        assert!(
            names(&out)
                == vec![
                    "api-a".to_string(),
                    "api-b".to_string(),
                    "api-c".to_string()
                ]
        );
    }

    #[tokio::test]
    async fn count_filter_then_by_preserves_spans_from_passing_traces() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                TraceSpan {
                    id: 1,
                    trace: 1,
                    name: "api-a",
                    duration_nanos: 1,
                    attrs: vec![("svc", AttrValue::Str("api".into()))],
                    ..TraceSpan::default()
                }
                .build(),
                TraceSpan {
                    id: 2,
                    trace: 1,
                    name: "api-b",
                    duration_nanos: 1,
                    attrs: vec![("svc", AttrValue::Str("api".into()))],
                    ..TraceSpan::default()
                }
                .build(),
                TraceSpan {
                    id: 3,
                    trace: 2,
                    name: "db-a",
                    duration_nanos: 1,
                    attrs: vec![("svc", AttrValue::Str("db".into()))],
                    ..TraceSpan::default()
                }
                .build(),
            ],
        );

        let out = planned("{ .svc != nil } | count() > 1 | by(span.svc)", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn preserving_stage_before_count_filter_is_ignored_for_search() {
        let store = api_db_store();

        let out = planned(
            "{ .svc = \"api\" } | select(span:duration, span.svc) | count() > 1",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn avg_filter_keeps_spans_from_passing_traces() {
        let store = fast_slow_store();

        let out = planned("{ .svc = \"api\" } | avg(span:duration) > 100", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["slow-a".to_string(), "slow-b".to_string()]);
    }

    #[tokio::test]
    async fn avg_filter_then_by_preserves_spans_from_passing_traces() {
        let store = fast_slow_store();

        let out = planned(
            "{ .svc = \"api\" } | avg(span:duration) > 100 | by(span.svc)",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == vec!["slow-a".to_string(), "slow-b".to_string()]);
    }

    #[tokio::test]
    async fn avg_without_filter_preserves_matched_spans() {
        let store = api_db_store();

        let out = planned("{ .svc = \"api\" } | avg(span:duration)", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn select_preserves_matched_spans_for_search_projection() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "db-a", 40, vec![("svc", AttrValue::Str("db".into()))]),
            ],
        );

        let out = planned(
            "{ .svc = \"api\" } | select(span:duration, span.svc)",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == vec!["api-a".to_string()]);
    }

    #[tokio::test]
    async fn select_coalesce_preserves_matched_spans_for_search() {
        let store = api_db_store();

        let out = planned(
            "{ .svc = \"api\" } | select(span:duration, span.svc) | coalesce()",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn by_coalesce_preserves_matched_spans_for_search() {
        let store = api_db_store();

        let out = planned("{ .svc != nil } | by(span.svc) | coalesce()", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string(), "db-a".to_string()]);
    }

    #[tokio::test]
    async fn by_without_aggregate_preserves_matched_spans_for_search() {
        let store = api_db_store();

        let out = planned("{ .svc != nil } | by(span.svc)", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string(), "db-a".to_string()]);
    }

    #[tokio::test]
    async fn avg_by_filter_keeps_spans_from_passing_groups() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![
                span(1, "api-a", 20, vec![("svc", AttrValue::Str("api".into()))]),
                span(2, "api-b", 40, vec![("svc", AttrValue::Str("api".into()))]),
                span(3, "db-a", 200, vec![("svc", AttrValue::Str("db".into()))]),
                span(4, "db-b", 400, vec![("svc", AttrValue::Str("db".into()))]),
            ],
        );

        let out = planned(
            "{ .svc != nil } | avg(span:duration) | by(span.svc) > 100",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == vec!["db-a".to_string(), "db-b".to_string()]);
    }

    #[tokio::test]
    async fn avg_by_coalesce_preserves_matched_spans_for_search() {
        let store = api_db_store();

        let out = planned(
            "{ .svc != nil } | avg(span:duration) | by(span.svc) | coalesce()",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string(), "db-a".to_string()]);
    }

    #[tokio::test]
    async fn count_by_topk_and_bottomk_keep_spans_from_ranked_groups() {
        let store = api_db_cache_store();

        let top = planned("{ .svc != nil } | count() | by(span.svc) | topk(1)", &store)
            .await
            .unwrap();
        assert!(names(&top) == CACHE_SPAN_NAMES);

        let bottom = planned(
            "{ .svc != nil } | count() | by(span.svc) | bottomk(1)",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&bottom) == vec!["db-a".to_string()]);
    }

    #[tokio::test]
    async fn count_by_topk_filter_keeps_spans_from_ranked_passing_groups() {
        let store = api_db_cache_store();

        let out = planned(
            "{ .svc != nil } | count() | by(span.svc) | topk(2) > 2",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == CACHE_SPAN_NAMES);
    }

    #[tokio::test]
    async fn count_by_filter_topk_ranks_spans_from_passing_groups() {
        let store = api_db_cache_store();

        let out = planned(
            "{ .svc != nil } | count() by(span.svc) > 1 | topk(1)",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == CACHE_SPAN_NAMES);
    }

    #[tokio::test]
    async fn count_filter_topk_by_ranks_passing_groups() {
        let store = api_db_cache_store();

        let out = planned(
            "{ .svc != nil } | count() > 1 | topk(1) | by(span.svc)",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == CACHE_SPAN_NAMES);
    }

    #[tokio::test]
    async fn count_topk_filter_by_keeps_spans_from_ranked_passing_groups() {
        let store = api_db_cache_store();

        let out = planned(
            "{ .svc != nil } | count() | topk(2) > 2 | by(span.svc)",
            &store,
        )
        .await
        .unwrap();
        assert!(names(&out) == CACHE_SPAN_NAMES);
    }

    #[tokio::test]
    async fn count_topk_by_keeps_spans_from_ranked_groups() {
        let store = api_db_cache_store();

        let top = planned("{ .svc != nil } | count() | topk(1) | by(span.svc)", &store)
            .await
            .unwrap();
        assert!(names(&top) == CACHE_SPAN_NAMES);
    }

    #[tokio::test]
    async fn count_topk_without_by_preserves_all_matched_spans() {
        let store = api_db_store();

        let top = planned("{ .svc != nil } | count() | topk(1)", &store)
            .await
            .unwrap();
        assert!(names(&top) == vec!["api-a".to_string(), "api-b".to_string(), "db-a".to_string()]);

        let bottom = planned("{ .svc != nil } | count() | bottomk(1)", &store)
            .await
            .unwrap();
        assert!(
            names(&bottom) == vec!["api-a".to_string(), "api-b".to_string(), "db-a".to_string()]
        );
    }

    #[tokio::test]
    async fn count_topk_filter_gates_ungrouped_ranked_spans() {
        let store = ungrouped_two_trace_store();

        let out = planned("{ span:name != nil } | count() | topk(1) > 1", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn count_filter_topk_gates_ungrouped_ranked_spans() {
        let store = ungrouped_two_trace_store();

        let out = planned("{ span:name != nil } | count() > 1 | topk(1)", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }
}

mod aggregate_expr_sql;
mod aggregate_filter_sql;
mod aggregate_filter_sql_query;
mod aggregate_filter_sql_query_any;
mod aggregate_projection_field;
mod aggregate_rank_expr_sql;
mod collect_nested_selectors;
mod collect_table;
mod group_join_sql;
mod grouped_aggregate_sql;
mod grouped_no_filter_by;
mod grouped_pipeline_sql;
mod grouped_rank_pipeline_parts;
mod grouped_rank_pipeline_sql;
mod grouped_rank_sql;
mod is_inert_pipeline_stage;
mod is_search_preserving_aggregate;
mod is_search_preserving_pipeline_stage;
pub(crate) mod nested_projection_matcher;
mod pipeline_nested_projection_matchers;
mod pipeline_to_sql;
mod plan_query;
mod plan_spanset_sql;
mod planned_spanset;
mod planner_context;
mod push_nested_projection_matcher;
mod rank_direction;
mod rank_filter;
mod rank_limit;
mod register_batches;
mod register_nested_selector_tables;
mod scan_options_with_pipeline_projections;
mod spanset_to_sql;
mod structural_base_op;
mod structural_is_negated;
mod structural_is_union;
mod structural_predicate_sql;
mod ungrouped_rank_parts;
mod ungrouped_rank_pipeline_parts;
mod ungrouped_rank_pipeline_sql;
mod ungrouped_rank_sql;

use aggregate_expr_sql::aggregate_expr_sql;
use aggregate_filter_sql::aggregate_filter_sql;
use aggregate_filter_sql_query::aggregate_filter_sql_query;
use aggregate_filter_sql_query_any::aggregate_filter_sql_query_any;
use aggregate_projection_field::aggregate_projection_field;
use aggregate_rank_expr_sql::aggregate_rank_expr_sql;
use collect_nested_selectors::collect_nested_selectors;
use collect_table::collect_table;
use group_join_sql::{GroupJoinSql, group_join_sql};
use grouped_aggregate_sql::grouped_aggregate_sql;
use grouped_no_filter_by::grouped_no_filter_by;
use grouped_pipeline_sql::grouped_pipeline_sql;
use grouped_rank_pipeline_parts::grouped_rank_pipeline_parts;
use grouped_rank_pipeline_sql::grouped_rank_pipeline_sql;
use grouped_rank_sql::grouped_rank_sql;
use is_inert_pipeline_stage::is_inert_pipeline_stage;
use is_search_preserving_aggregate::is_search_preserving_aggregate;
use is_search_preserving_pipeline_stage::is_search_preserving_pipeline_stage;
use nested_projection_matcher::nested_projection_matcher;
use pipeline_nested_projection_matchers::pipeline_nested_projection_matchers;
use pipeline_to_sql::pipeline_to_sql;
pub(crate) use plan_query::plan_query;
use plan_spanset_sql::plan_spanset_sql;
pub(crate) use planned_spanset::PlannedSpanset;
pub(crate) use planner_context::PlannerContext;
use push_nested_projection_matcher::push_nested_projection_matcher;
use rank_direction::RankDirection;
use rank_filter::RankFilter;
use rank_limit::{RankLimit, rank_limit};
use register_batches::register_batches;
use register_nested_selector_tables::register_nested_selector_tables;
use scan_options_with_pipeline_projections::scan_options_with_pipeline_projections;
use spanset_to_sql::spanset_to_sql;
use structural_base_op::structural_base_op;
use structural_is_negated::structural_is_negated;
use structural_is_union::structural_is_union;
use structural_predicate_sql::structural_predicate_sql;
use ungrouped_rank_parts::UngroupedRankParts;
use ungrouped_rank_pipeline_parts::ungrouped_rank_pipeline_parts;
use ungrouped_rank_pipeline_sql::ungrouped_rank_pipeline_sql;
use ungrouped_rank_sql::ungrouped_rank_sql;

mod register_field_comparison_columns;
use register_field_comparison_columns::{
    collect_field_selectors, register_field_comparison_columns,
};

mod apply_expression_pipeline;

mod expression_name;

mod sample_metric_scan;
