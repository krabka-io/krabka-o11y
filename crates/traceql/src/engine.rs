//! Public `TraceQL` engine.

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

use arrow::{
    array::{
        Array, BooleanArray, DictionaryArray, Float64Array, Int64Array, LargeStringArray,
        ListArray, StringArray, StringViewArray,
    },
    datatypes::{DataType, Int32Type},
    record_batch::RecordBatch,
};
use datafusion::arrow::array::AsArray;
use krabka_units::{ByteSize, Time, convert::TimeExt as _, millis};

use crate::{
    ast::{
        Aggregate, ArithmeticOp, ComparisonOp, Field, FieldExpr, Intrinsic, Pipeline, Query,
        QueryHints, ScalarExpr, Scope, SpansetExpr, Value,
    },
    error::{Result, TraceqlError},
    ids::{DurationNanos, UnixNano},
    parser::parse,
    planner::{
        PlannerContext, RankDirection, RankLimit,
        nested_projection_matcher::nested_projection_matcher, plan_query, rank_stage_limit,
    },
    result::{
        AttrValue, Attribute, MetricLabel, MetricPoint, ScopedTag, SearchResponse, SpanRef,
        SpanSet, TagScope, TraceMetricExemplar, TraceMetricLabelType, TraceMetricSeries,
        TraceMetricsResponse, TraceResult, TraceSpans, TypedValue,
    },
    span_columns::{
        ATTR_PREFIX, COL_CHILD_COUNT, COL_DURATION, COL_EVENT_NAME, COL_EVENT_TIME_SINCE_START,
        COL_INSTRUMENTATION_NAME, COL_INSTRUMENTATION_VERSION, COL_KIND, COL_LINK_SPAN_ID,
        COL_LINK_TRACE_ID, COL_NAME, COL_NS_LEFT, COL_NS_RIGHT, COL_PARENT_ID, COL_PARENT_SPAN_ID,
        COL_ROOT_SERVICE_NAME, COL_ROOT_SPAN_NAME, COL_SPAN_ID, COL_START, COL_STATUS_CODE,
        COL_STATUS_MESSAGE, COL_TRACE_DURATION, COL_TRACE_ID, COL_TRACE_START, EVENT_ATTR_PREFIX,
        INSTRUMENTATION_ATTR_PREFIX, LINK_ATTR_PREFIX,
    },
    store::{ScanOptions, SpanMatcher, SpanStore},
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    /// `usize_from_integer_f64` refuses anything that is not a non-negative
    /// whole number. Joined with `&&` instead of `||`, a value has to be both
    /// kinds of wrong at once to be refused -- so a negative or a fractional
    /// limit sails through and is parsed as a count. Infinities and NaNs are
    /// refused by the fractional clause, whose `fract()` is NaN for both.
    #[test]
    fn a_limit_must_be_a_non_negative_whole_number() {
        use super::usize_from_integer_f64;

        check!(usize_from_integer_f64(0.0).unwrap() == 0);
        check!(usize_from_integer_f64(42.0).unwrap() == 42);

        // The first two are wrong in exactly one way, so each isolates one
        // clause; the non-finite pair rides on the fractional clause.
        //
        // The guard's *message* is what has to be asserted, not merely that an
        // error came back: `-1.0` and `1.5` both fail the `parse::<usize>()`
        // below the guard as well, so `is_err()` holds whether or not the guard
        // fired. Only the diagnostic separates a rejected limit from a value
        // that happened not to parse.
        let refused = |value: f64| match usize_from_integer_f64(value) {
            Err(TraceqlError::Exec(message)) => {
                message.contains("expected non-negative integer float")
            }
            _ => false,
        };
        check!(refused(-1.0), "negative");
        check!(refused(1.5), "fractional");
        check!(refused(f64::INFINITY), "infinite");
        check!(refused(f64::NAN), "not a number");
    }

    use arrow::{
        array::{
            ArrayRef, FixedSizeBinaryBuilder, Int32Array, Int64Array, StringArray,
            StringDictionaryBuilder,
        },
        datatypes::{Field as ArrowField, Int32Type, Schema},
    };
    use assert2::{assert, check};
    use datafusion::{catalog::MemTable, prelude::SessionContext};
    use krabka_units::{convert::ByteSizeExt as _, millis, nanos, secs};

    use super::*;
    use crate::{
        in_memory::InMemorySpanStore,
        result::{AttrValue, EventRef, LinkRef, TypedValue},
        span_columns::InputSpan,
        store::{MatchScope, ScanResult, TagCatalog},
        testkit::{RootSpanRow, root_span_columns},
    };

    struct MetricExpectedCase {
        metric: &'static str,
        expected: f64,
    }

    struct QuerySeriesCase {
        query: &'static str,
        wanted: Vec<TraceMetricSeries>,
    }

    struct MetricTypedExpectedCase {
        metric: &'static str,
        expected_kind: TraceMetricLabelType,
        expected_value: f64,
    }

    struct CompareTotalCase {
        meta: &'static str,
        attr: &'static str,
        value: &'static str,
        want: i64,
    }

    struct PathFrequencyCase {
        path: &'static str,
        frequency: u8,
    }

    struct AttributeValueCase {
        attr: &'static str,
        value: &'static str,
    }

    struct EmptyTypedValues {
        int_values: arrow::array::ListArray,
        double_values: arrow::array::ListArray,
        bool_values: arrow::array::ListArray,
    }

    struct AttrComparisonCase {
        attr: AttrValue,
        rhs: Value,
    }

    struct IntrinsicComparisonCase {
        intrinsic: Intrinsic,
        op: ComparisonOp,
        rhs: Value,
    }

    struct IntegerComparisonCase {
        lhs: i64,
        op: ComparisonOp,
        rhs: i64,
        expected: bool,
    }

    struct FloatComparisonCase {
        lhs: f64,
        op: ComparisonOp,
        rhs: f64,
        expected: bool,
    }

    struct KindNameCase {
        name: &'static str,
        code: i32,
    }

    struct StatusEnumCase {
        code: i32,
        value: Value,
    }

    struct RegionCountCase {
        meta: &'static str,
        value: &'static str,
        want: i64,
    }

    fn sp(tid: u8, id: u8, parent: Option<u8>, svc: &str) -> InputSpan {
        sp_at(tid, id, parent, svc, 1000 + i64::from(id))
    }

    fn sp_at(tid: u8, id: u8, parent: Option<u8>, svc: &str, start_unix_nano: i64) -> InputSpan {
        InputSpan {
            trace_id: [tid; 16],
            span_id: [id; 8],
            parent_span_id: parent.map(|p| [p; 8]),
            name: format!("op-{id}"),
            kind: 0,
            start_unix_nano,
            duration: nanos(200),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: "tracer".into(),
            instrumentation_version: String::new(),
            attrs: vec![Attribute::new("svc", AttrValue::Str(svc.into()))],
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    // An engine over tenant `t` holding one trace (service `a`, root `root`).
    fn one_trace_engine(spans: Vec<InputSpan>) -> TraceqlEngine<InMemorySpanStore> {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", spans);
        TraceqlEngine::new(Arc::new(s), EngineOpts::default())
    }

    struct TraceCosts {
        trace: u8,
        costs: Vec<i64>,
    }

    struct EnumSpanCase {
        id: u8,
        kind: i32,
        status: i32,
    }

    struct EnumGroupCase {
        field: &'static str,
        text: &'static str,
        expected_ids: Vec<[u8; 8]>,
    }

    struct MetricLabelCase {
        value: Option<&'static str>,
        kind: Option<TraceMetricLabelType>,
        count: f64,
    }

    struct AggregateCase {
        aggregate: &'static str,
        expected: f64,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct ExpectedTraceSpans {
        trace_id: [u8; 16],
        span_ids: Vec<[u8; 8]>,
    }

    struct QueryResultCase {
        query: &'static str,
        expected: Vec<ExpectedTraceSpans>,
    }

    struct MetricFieldColumnCase {
        scope: Scope,
        key: &'static str,
        expected_column: String,
    }

    struct IntegerFloatConversionCase {
        value: f64,
        expected: Option<usize>,
    }

    struct MetricFilterCase {
        value: f64,
        operator: ComparisonOp,
        expected: bool,
    }

    struct BlockAttrValueCase {
        index: usize,
        expected: Vec<AttrValue>,
    }

    struct MetricPipelineCase {
        name: &'static str,
        stages: Vec<Pipeline>,
    }

    struct QuerySpanCase {
        query: &'static str,
        expected_ids: Vec<[u8; 8]>,
    }

    struct ArithmeticSpanCase {
        a: Option<AttrValue>,
        b: AttrValue,
        total: i64,
    }

    struct PredicateCase {
        predicate: &'static str,
        expected_span_ids: Vec<[u8; 8]>,
    }

    struct QueryMetricsCase {
        query: &'static str,
        expected: Vec<TraceMetricSeries>,
    }

    struct InitializedMetricCase {
        tenant: &'static str,
        query: &'static str,
        initialized: bool,
    }

    #[tokio::test]
    async fn expression_aggregates_group_per_trace_keep_nil_and_execute_coalesce_in_order() {
        let mut store = InMemorySpanStore::new();
        for case in [
            TraceCosts {
                trace: 1,
                costs: vec![1, 1, 4],
            },
            TraceCosts {
                trace: 2,
                costs: vec![1],
            },
        ] {
            let spans = case
                .costs
                .into_iter()
                .enumerate()
                .map(|indexed_cost| {
                    let index = indexed_cost.0;
                    let cost = indexed_cost.1;
                    let mut span = sp_at(
                        case.trace,
                        u8::try_from(index + 1).unwrap(),
                        None,
                        "groups",
                        1,
                    );
                    span.attrs
                        .push(Attribute::new("cost", AttrValue::Int(cost)));
                    if cost == 1 {
                        span.attrs
                            .push(Attribute::new("lane", AttrValue::Str("a".into())));
                    }
                    span
                })
                .collect();
            store.push_trace("t", "groups", "root", spans);
        }
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        let grouped = engine
            .search_with_spss("t", "{} | by(span.lane)", 0, 1000, 100, 100)
            .await
            .unwrap();
        assert!(grouped.traces.len() == 2);
        assert!(grouped.traces[0].span_sets.len() == 2);
        assert!(grouped.traces[1].span_sets.len() == 1);
        let mut group_sizes = grouped.traces[0]
            .span_sets
            .iter()
            .map(|set| set.matched)
            .collect::<Vec<_>>();
        group_sizes.sort_unstable();
        assert!(group_sizes == vec![1, 2]);
        let mut grouped_attributes = grouped.traces[0]
            .span_sets
            .iter()
            .map(|set| set.attributes.clone())
            .collect::<Vec<_>>();
        grouped_attributes.sort_by_key(|attrs| format!("{attrs:?}"));
        assert!(
            grouped_attributes
                == vec![
                    vec![Attribute::new("by(span.lane)", AttrValue::Str("a".into()))],
                    vec![Attribute::new(
                        "by(span.lane)",
                        AttrValue::Str("nil".into())
                    )],
                ]
        );
        let boolean = engine
            .search_with_spss(
                "t",
                "{} | by(span.cost > 1) | count() > 1",
                0,
                1000,
                100,
                100,
            )
            .await
            .unwrap();
        assert!(boolean.traces.len() == 1);
        assert!(boolean.traces[0].span_sets.len() == 1);
        assert!(boolean.traces[0].span_sets[0].matched == 2);
        assert!(
            boolean.traces[0].span_sets[0].attributes
                == vec![
                    Attribute::new("by(span.cost > 1)", AttrValue::Bool(false)),
                    Attribute::new("count()", AttrValue::Int(2)),
                ]
        );

        for case in [
            QueryResultCase {
                query: "{} | sum(span.cost * 2) > 10",
                expected: vec![ExpectedTraceSpans {
                    trace_id: [1; 16],
                    span_ids: vec![[1; 8], [2; 8], [3; 8]],
                }],
            },
            QueryResultCase {
                query: "{} | avg(span.cost + 1) >= 3",
                expected: vec![ExpectedTraceSpans {
                    trace_id: [1; 16],
                    span_ids: vec![[1; 8], [2; 8], [3; 8]],
                }],
            },
            QueryResultCase {
                query: "{} | by(span.cost / 2) | count() > 1",
                expected: vec![ExpectedTraceSpans {
                    trace_id: [1; 16],
                    span_ids: vec![[1; 8], [2; 8]],
                }],
            },
            QueryResultCase {
                query: "{} | by(span.lane) | min(span.cost * 2) > 3",
                expected: vec![ExpectedTraceSpans {
                    trace_id: [1; 16],
                    span_ids: vec![[3; 8]],
                }],
            },
            QueryResultCase {
                query: "{} | by(span.cost / 2) | coalesce() | count() > 2",
                expected: vec![ExpectedTraceSpans {
                    trace_id: [1; 16],
                    span_ids: vec![[1; 8], [2; 8], [3; 8]],
                }],
            },
            QueryResultCase {
                query: "{} | max(span.cost - 1) < 0",
                expected: vec![],
            },
        ] {
            let response = engine
                .search_with_spss("t", case.query, 0, 1000, 100, 100)
                .await
                .unwrap();
            let actual = response
                .traces
                .into_iter()
                .map(|trace| {
                    let mut ids = trace
                        .span_sets
                        .into_iter()
                        .flat_map(|set| set.spans)
                        .map(|span| span.span_id)
                        .collect::<Vec<_>>();
                    ids.sort_unstable();
                    ExpectedTraceSpans {
                        trace_id: trace.trace_id,
                        span_ids: ids,
                    }
                })
                .collect::<Vec<_>>();
            assert!(actual == case.expected, "{}", case.query);
        }
    }

    #[tokio::test]
    async fn enum_group_attributes_compose_with_counts_without_integer_type_conflation() {
        let spans = [
            EnumSpanCase {
                id: 1,
                kind: 2,
                status: 2,
            },
            EnumSpanCase {
                id: 2,
                kind: 3,
                status: 2,
            },
            EnumSpanCase {
                id: 3,
                kind: 2,
                status: 0,
            },
        ]
        .into_iter()
        .map(|case| {
            let mut span = sp_at(1, case.id, None, "enum-groups", 1);
            span.kind = case.kind;
            span.status_code = case.status;
            span
        })
        .collect();
        let mut store = InMemorySpanStore::new();
        store.push_trace("t", "enum-groups", "root", spans);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            EnumGroupCase {
                field: "span:kind",
                text: "server",
                expected_ids: vec![[1; 8], [3; 8]],
            },
            EnumGroupCase {
                field: "span:status",
                text: "error",
                expected_ids: vec![[1; 8], [2; 8]],
            },
        ] {
            let response = engine
                .search_with_spss(
                    "t",
                    &format!("{{}} | by({}) | count() > 1", case.field),
                    0,
                    1000,
                    10,
                    10,
                )
                .await
                .unwrap();
            assert!(response.traces.len() == 1);
            assert!(response.traces[0].span_sets.len() == 1);
            let set = &response.traces[0].span_sets[0];
            let mut ids = set
                .spans
                .iter()
                .map(|span| span.span_id)
                .collect::<Vec<_>>();
            ids.sort_unstable();
            assert!(ids == case.expected_ids);
            assert!(
                set.attributes
                    == vec![
                        Attribute::new(
                            format!("by({})", case.field),
                            AttrValue::Str(case.text.into())
                        ),
                        Attribute::new("count()", AttrValue::Int(2))
                    ]
            );
            assert!(set.attributes[0].value != AttrValue::Int(2));
        }
    }

    #[tokio::test]
    async fn bare_attribute_requires_scalar_boolean_true_without_coercion() {
        let mut spans = Vec::new();
        for indexed_values in [
            vec![AttrValue::Bool(true)],
            vec![AttrValue::Bool(false)],
            vec![AttrValue::Str("true".into())],
            vec![AttrValue::Int(1)],
            vec![AttrValue::Bool(true), AttrValue::Bool(false)],
            Vec::new(),
        ]
        .into_iter()
        .enumerate()
        {
            let index = indexed_values.0;
            let values = indexed_values.1;
            let mut span = sp_at(1, u8::try_from(index + 1).unwrap(), None, "bool", 1);
            span.attrs.extend(
                values
                    .into_iter()
                    .map(|value| Attribute::new("flag", value)),
            );
            spans.push(span);
        }
        let mut store = InMemorySpanStore::new();
        store.push_trace("t", "bool", "root", spans);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            QuerySpanCase {
                query: "{ .flag }",
                expected_ids: vec![[1; 8]],
            },
            QuerySpanCase {
                query: "{ span:id = \"0202020202020202\" && !.flag }",
                expected_ids: vec![[2; 8]],
            },
            QuerySpanCase {
                query: "{ .flag || span:id = \"0606060606060606\" }",
                expected_ids: vec![[1; 8], [5; 8]],
            },
            QuerySpanCase {
                query: "{ .flag && duration > 0ns }",
                expected_ids: vec![[1; 8], [5; 8]],
            },
        ] {
            let response = engine
                .search_with_spss("t", case.query, 0, 1000, 100, 100)
                .await
                .unwrap();
            let mut ids = response
                .traces
                .iter()
                .flat_map(|trace| &trace.span_sets)
                .flat_map(|set| &set.spans)
                .map(|span| span.span_id)
                .collect::<Vec<_>>();
            ids.sort_unstable();
            assert!(ids == case.expected_ids, "{}", case.query);
        }
        assert!(
            engine
                .search("t", "{ !.flag }", 0, 1000, 100)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn dynamic_scopes_preserve_types_intrinsic_units_and_boolean_composition() {
        let mut spans = Vec::new();
        for id in 1..=3 {
            let mut span = sp_at(1, id, (id > 1).then_some(1), "typed", i64::from(id));
            span.attrs.extend([
                Attribute::new("limit", AttrValue::Int(3)),
                Attribute::new("__instrumentation.cost", AttrValue::Int(i64::from(id))),
                Attribute::new("__instrumentation.name", AttrValue::Str("tracer".into())),
            ]);
            for value in match id {
                1 => [2, 4],
                2 => [1, 2],
                _ => [6, 8],
            } {
                span.attrs
                    .push(Attribute::new("numbers", AttrValue::Int(value)));
            }
            span.events = vec![EventRef {
                name: format!("op-{id}"),
                time_since_start: nanos(50),
                attributes: if id == 3 {
                    Vec::new()
                } else {
                    vec![
                        Attribute::new(
                            "cost",
                            if id == 1 {
                                AttrValue::Int(2)
                            } else {
                                AttrValue::Str("2".into())
                            },
                        ),
                        Attribute::new("peer", AttrValue::Str(format!("op-{id}"))),
                    ]
                },
            }];
            span.links = vec![LinkRef {
                trace_id: span.trace_id,
                span_id: if id == 3 { [9; 8] } else { span.span_id },
                attributes: vec![Attribute::new("cost", AttrValue::Int(i64::from(id)))],
            }];
            spans.push(span);
        }
        let mut store = InMemorySpanStore::new();
        store.push_trace("t", "typed", "root", spans);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            QuerySpanCase {
                query: "event.cost + 1 = span.limit",
                expected_ids: vec![[1; 8]],
            },
            QuerySpanCase {
                query: "event.peer = name",
                expected_ids: vec![[1; 8], [2; 8]],
            },
            QuerySpanCase {
                query: "instrumentation.cost + link.cost = span.limit + 1",
                expected_ids: vec![[2; 8]],
            },
            QuerySpanCase {
                query: "instrumentation.name = instrumentation:name",
                expected_ids: vec![[1; 8], [2; 8], [3; 8]],
            },
            QuerySpanCase {
                query: "event:timeSinceStart * 2 < duration && event:name = name",
                expected_ids: vec![[1; 8], [2; 8], [3; 8]],
            },
            QuerySpanCase {
                query: "span:id = link:spanID && trace:id = link:traceID",
                expected_ids: vec![[1; 8], [2; 8]],
            },
            QuerySpanCase {
                query: "trace:duration >= duration && span:childCount + 1 > 2",
                expected_ids: vec![[1; 8]],
            },
            QuerySpanCase {
                query: "event.peer = name || span:childCount > 1",
                expected_ids: vec![[1; 8], [2; 8]],
            },
            QuerySpanCase {
                query: "event.cost = span.limit",
                expected_ids: vec![],
            },
            QuerySpanCase {
                query: "event.cost = nil && link.cost = span.limit",
                expected_ids: vec![[3; 8]],
            },
            QuerySpanCase {
                query: "span.numbers > span.limit",
                expected_ids: vec![[1; 8], [3; 8]],
            },
            QuerySpanCase {
                query: "span.limit < span.numbers && name != \"op-1\"",
                expected_ids: vec![[3; 8]],
            },
        ] {
            let query = format!("{{ {} }}", case.query);
            let response = engine
                .search_with_spss("t", &query, 0, 1000, 100, 100)
                .await
                .unwrap();
            assert!(
                sorted_matched_span_ids(&response) == case.expected_ids,
                "{query}"
            );
        }
        for predicate in ["span.numbers + 1 > 0", "span.numbers = span.numbers"] {
            assert!(
                engine
                    .search("t", &format!("{{ {predicate} }}"), 0, 1000, 100)
                    .await
                    .is_err()
            );
        }
    }

    #[tokio::test]
    async fn array_shapes_are_lossless_but_query_values_follow_pinned_collector_cardinality() {
        let mut store = InMemorySpanStore::new();
        let spans = [
            Some(AttrValue::Int(1)),
            Some(AttrValue::Array(vec![AttrValue::Int(1)])),
            Some(AttrValue::Array(vec![])),
            Some(AttrValue::Array(vec![AttrValue::Int(1), AttrValue::Int(2)])),
            None,
            Some(AttrValue::Unsupported(
                r#"{"arrayValue":{"values":[{"intValue":"1"},{"stringValue":"x"}]}}"#.into(),
            )),
        ]
        .into_iter()
        .enumerate()
        .map(|indexed_value| {
            let index = indexed_value.0;
            let value = indexed_value.1;
            let mut span = sp_at(1, u8::try_from(index + 1).unwrap(), None, "arrays", 1);
            if let Some(value) = value {
                span.attrs.push(Attribute::new("v", value));
            }
            span
        })
        .collect();
        store.push_trace("t", "arrays", "root", spans);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            QuerySpanCase {
                query: "{ span.v > 0 }",
                expected_ids: vec![[1; 8], [2; 8], [4; 8]],
            },
            QuerySpanCase {
                query: "{ span.v = nil }",
                expected_ids: vec![[3; 8], [5; 8], [6; 8]],
            },
        ] {
            let response = engine
                .search_with_spss("t", case.query, 0, 1000, 10, 10)
                .await
                .unwrap();
            let mut ids = response
                .traces
                .into_iter()
                .flat_map(|trace| trace.span_sets)
                .flat_map(|set| set.spans)
                .map(|span| span.span_id)
                .collect::<Vec<_>>();
            ids.sort_unstable();
            assert!(ids == case.expected_ids, "{}", case.query);
        }
        let grouped = engine
            .query_range("t", "{} | count_over_time() by(span.v)", 0, 1000, 1000)
            .await
            .unwrap();
        assert!(grouped.series.len() == 3);
        let scalar = grouped
            .series
            .iter()
            .find(|series| series.label_types.get("span.v") == Some(&TraceMetricLabelType::Int))
            .unwrap();
        assert!(scalar.labels == vec![MetricLabel::new("span.v", "1")]);
        assert!(scalar.points[0].value.to_bits() == 2.0_f64.to_bits());
        let nil = grouped
            .series
            .iter()
            .find(|series| series.labels == vec![MetricLabel::new("span.v", "nil")])
            .unwrap();
        assert!(nil.points[0].value.to_bits() == 3.0_f64.to_bits());
        let array = grouped
            .series
            .iter()
            .find(|series| series.label_types.get("span.v") == Some(&TraceMetricLabelType::Array))
            .unwrap();
        assert!(
            serde_json::from_str::<serde_json::Value>(&array.labels[0].value).unwrap()
                == serde_json::json!({"arrayValue":{"values":[{"intValue":"1"},{"intValue":"2"}]}})
        );
        assert!(array.points[0].value.to_bits() == 1.0_f64.to_bits());
        assert!(
            engine
                .search("t", "{ span.v + 1 = 2 }", 0, 1000, 10)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn typed_grouping_keeps_equal_spellings_separate_and_preserves_missing_group() {
        let mut store = InMemorySpanStore::new();
        let spans = [
            Some(AttrValue::Int(1)),
            Some(AttrValue::Int(1)),
            Some(AttrValue::Str("1".into())),
            Some(AttrValue::Float(1.0)),
            Some(AttrValue::Bool(true)),
            None,
        ]
        .into_iter()
        .enumerate()
        .map(|indexed_value| {
            let index = indexed_value.0;
            let value = indexed_value.1;
            let mut span = sp_at(1, u8::try_from(index + 1).unwrap(), None, "typed", 1);
            if let Some(value) = value {
                span.attrs.push(Attribute::new("mixed", value));
            }
            span
        })
        .collect();
        store.push_trace("t", "typed", "root", spans);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        let response = engine
            .query_range("t", "{} | count_over_time() by(span.mixed)", 0, 10, 10)
            .await
            .unwrap();
        let mut expected = Vec::new();
        for case in [
            MetricLabelCase {
                value: Some("1"),
                kind: None,
                count: 1.0,
            },
            MetricLabelCase {
                value: Some("1"),
                kind: Some(TraceMetricLabelType::Int),
                count: 2.0,
            },
            MetricLabelCase {
                value: Some("1"),
                kind: Some(TraceMetricLabelType::Double),
                count: 1.0,
            },
            MetricLabelCase {
                value: Some("nil"),
                kind: None,
                count: 1.0,
            },
            MetricLabelCase {
                value: Some("true"),
                kind: Some(TraceMetricLabelType::Bool),
                count: 1.0,
            },
        ] {
            expected.push(TraceMetricSeries {
                labels: case
                    .value
                    .into_iter()
                    .map(|value| MetricLabel::new("span.mixed", value))
                    .collect(),
                label_types: {
                    let mut label_types = BTreeMap::new();
                    if let Some(kind) = case.kind {
                        label_types.insert("span.mixed".into(), kind);
                    }
                    label_types
                },
                points: vec![MetricPoint::new(0, case.count), MetricPoint::new(10, 0.0)],
                exemplars: Vec::new(),
            });
        }
        assert!(response.series == expected);
    }

    #[tokio::test]
    async fn numeric_metrics_do_not_treat_an_array_first_element_as_a_scalar() {
        let mut store = InMemorySpanStore::new();
        let mut scalar = sp_at(1, 1, None, "arrays", 1);
        scalar
            .attrs
            .push(Attribute::new("values", AttrValue::Int(4)));
        let mut array = sp_at(1, 2, None, "arrays", 1);
        array.attrs.extend([
            Attribute::new("values", AttrValue::Int(100)),
            Attribute::new("values", AttrValue::Int(200)),
        ]);
        store.push_trace("t", "arrays", "root", vec![scalar, array]);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            AggregateCase {
                aggregate: "sum_over_time",
                expected: 4.0,
            },
            AggregateCase {
                aggregate: "avg_over_time",
                expected: 4.0,
            },
            AggregateCase {
                aggregate: "min_over_time",
                expected: 4.0,
            },
            AggregateCase {
                aggregate: "max_over_time",
                expected: 4.0,
            },
        ] {
            let response = engine
                .query_range(
                    "t",
                    &format!("{{}} | {}(span.values)", case.aggregate),
                    0,
                    10,
                    10,
                )
                .await
                .unwrap();
            assert!(response.series.len() == 1);
            assert!(response.series[0].points == vec![MetricPoint::new(0, case.expected)]);
        }
        let histogram = engine
            .query_range("t", "{} | histogram_over_time(span.values)", 0, 10, 10)
            .await
            .unwrap();
        assert!(histogram.series.len() == 1);
        assert!(histogram.series[0].labels == vec![MetricLabel::new("__bucket", "4")]);
        assert!(
            histogram.series[0].points == vec![MetricPoint::new(0, 1.0), MetricPoint::new(10, 0.0)]
        );
    }

    #[tokio::test]
    async fn numeric_metrics_read_original_types_from_heterogeneous_columns() {
        let mut store = InMemorySpanStore::new();
        let spans = [
            AttrValue::Int(2),
            AttrValue::Float(3.5),
            AttrValue::Str("100".into()),
            AttrValue::Bool(true),
        ]
        .into_iter()
        .enumerate()
        .map(|indexed_value| {
            let index = indexed_value.0;
            let value = indexed_value.1;
            let mut span = sp_at(1, u8::try_from(index + 1).unwrap(), None, "typed", 1);
            span.attrs.push(Attribute::new("mixed", value));
            span
        })
        .collect();
        store.push_trace("t", "typed", "root", spans);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        let summed = engine
            .query_range("t", "{} | sum_over_time(span.mixed)", 0, 10, 10)
            .await
            .unwrap();
        assert!(
            summed.series
                == vec![TraceMetricSeries {
                    labels: Vec::new(),
                    label_types: BTreeMap::default(),
                    points: vec![MetricPoint::new(0, 5.5)],
                    exemplars: Vec::new(),
                }]
        );
        let histogram = engine
            .query_range("t", "{} | histogram_over_time(span.mixed)", 0, 10, 10)
            .await
            .unwrap();
        // The pinned Tempo histogram buckets integers; floats, strings and
        // booleans contribute no observations.
        assert!(
            histogram.series
                == vec![TraceMetricSeries {
                    labels: vec![MetricLabel::new("__bucket", "2")],
                    label_types: BTreeMap::default(),
                    points: vec![MetricPoint::new(0, 1.0), MetricPoint::new(10, 0.0)],
                    exemplars: Vec::new(),
                }]
        );
    }

    #[tokio::test]
    async fn typed_comparison_presence_sibling_uses_original_integer_after_float_row() {
        let mut first = sp_at(1, 1, None, "typed", 1);
        first
            .attrs
            .push(Attribute::new("lhs", AttrValue::Float(0.0)));
        let mut second = sp_at(1, 2, None, "typed", 2);
        second.attrs.extend([
            Attribute::new("lhs", AttrValue::Int(2)),
            Attribute::new("rhs", AttrValue::Int(0)),
        ]);
        let mut store = InMemorySpanStore::new();
        store.push_trace("t", "typed", "root", vec![first, second]);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        let response = engine
            .search_with_spss(
                "t",
                "{ (span.lhs != span.rhs) && span.lhs != nil }",
                0,
                10,
                10,
                10,
            )
            .await
            .unwrap();
        assert!(response.traces.len() == 1);
        assert!(
            response.traces[0].span_sets[0]
                .spans
                .iter()
                .map(|span| span.span_id)
                .collect::<Vec<_>>()
                == vec![[2; 8]]
        );
    }

    #[tokio::test]
    async fn field_arithmetic_preserves_types_precedence_units_and_runtime_errors() {
        let ledger = [
            ArithmeticSpanCase {
                a: Some(AttrValue::Int(2)),
                b: AttrValue::Int(3),
                total: 10,
            },
            ArithmeticSpanCase {
                a: Some(AttrValue::Int(4)),
                b: AttrValue::Int(1),
                total: 9,
            },
            ArithmeticSpanCase {
                a: Some(AttrValue::Str("2".into())),
                b: AttrValue::Int(3),
                total: 10,
            },
            ArithmeticSpanCase {
                a: None,
                b: AttrValue::Int(3),
                total: 10,
            },
            ArithmeticSpanCase {
                a: Some(AttrValue::Float(1.5)),
                b: AttrValue::Int(2),
                total: 7,
            },
        ];
        let spans = ledger
            .into_iter()
            .enumerate()
            .map(|entry| {
                let index = entry.0;
                let case = entry.1;
                let mut span = sp_at(1, u8::try_from(index + 1).unwrap(), None, "typed", 1);
                span.attrs.extend([
                    Attribute::new("b", case.b),
                    Attribute::new("total", AttrValue::Int(case.total)),
                ]);
                if let Some(a) = case.a {
                    span.attrs.push(Attribute::new("a", a));
                }
                span
            })
            .collect();
        let mut store = InMemorySpanStore::new();
        store.push_trace("t", "typed", "root", spans);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        for PredicateCase {
            predicate,
            expected_span_ids: expected,
        } in [
            PredicateCase {
                predicate: "(.a + .b) * 2 >= .total && -duration < 0ns",
                expected_span_ids: vec![[1; 8], [2; 8], [5; 8]],
            },
            PredicateCase {
                predicate: ".a / .b > 0",
                expected_span_ids: vec![[2; 8], [5; 8]],
            },
            PredicateCase {
                predicate: "duration / 100ns = 2",
                expected_span_ids: vec![[1; 8], [2; 8], [3; 8], [4; 8], [5; 8]],
            },
            PredicateCase {
                predicate: ".a ^ .b = 9",
                expected_span_ids: vec![[1; 8]],
            },
        ] {
            let query = format!("{{ {predicate} }}");
            let response = engine
                .search_with_spss("t", &query, 0, 100, 100, 100)
                .await
                .unwrap();
            assert!(sorted_matched_span_ids(&response) == expected, "{query}");
            let count = f64::from(u32::try_from(expected.len()).unwrap());
            let response = engine
                .query_range("t", &format!("{query} | count_over_time()"), 0, 100, 100)
                .await
                .unwrap();
            assert!(
                response.series
                    == vec![TraceMetricSeries {
                        labels: Vec::new(),
                        label_types: BTreeMap::default(),
                        points: vec![MetricPoint::new(0, count), MetricPoint::new(100, 0.0)],
                        exemplars: Vec::new()
                    }]
            );
        }
        for query in ["{ .a / (.b - .b) > 0 }", "{ -.missing > 0 }"] {
            assert!(
                engine.search("t", query, 0, 100, 100).await.is_err(),
                "{query}"
            );
            assert!(
                engine
                    .query_range("t", &format!("{query} | count_over_time()"), 0, 100, 100)
                    .await
                    .is_err(),
                "{query}"
            );
        }
    }

    async fn search_all(e: &TraceqlEngine<InMemorySpanStore>, query: &str) -> SearchResponse {
        e.search("t", query, 0, 100_000, 20).await.unwrap()
    }

    async fn query_one_bucket(
        e: &TraceqlEngine<InMemorySpanStore>,
        query: &str,
    ) -> TraceMetricsResponse {
        e.query_range("t", query, 0, 60_000, 60_000).await.unwrap()
    }

    fn check_single_matched_span(r: &SearchResponse, span_id: u8) {
        check!(r.traces.len() == 1);
        check!(r.traces[0].span_sets[0].matched == 1);
        check!(r.traces[0].span_sets[0].spans[0].span_id == [span_id; 8]);
    }

    fn check_single_trace_nine_matched_span(r: &SearchResponse, span_id: u8) {
        check_single_matched_span(r, span_id);
        check!(r.traces[0].trace_id == [9; 16]);
    }

    // Exactly one trace, trace 9, whose span set matched two spans.
    fn check_trace_nine_matched_twice(r: &SearchResponse) {
        check!(r.traces.len() == 1);
        check!(r.traces[0].trace_id == [9; 16]);
        check!(r.traces[0].span_sets[0].matched == 2);
    }

    // A span with one `cache.miss` event and no event attributes.
    fn cache_miss_span() -> InputSpan {
        let mut event_span = sp(9, 1, None, "a");
        event_span.events = vec![EventRef {
            time_since_start: nanos(50),
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        event_span
    }

    // One trace: root span 1 carries a `cache.miss` event, child span 2 none.
    fn cache_miss_engine() -> TraceqlEngine<InMemorySpanStore> {
        let event_span = cache_miss_span();
        let peer = sp(9, 2, Some(1), "b");
        one_trace_engine(vec![event_span, peer])
    }

    fn api_and_db_metric_spans() -> Vec<InputSpan> {
        vec![
            sp_at(1, 1, None, "api", 0),
            sp_at(1, 2, None, "api", 10_000),
            sp_at(1, 3, None, "db", 20_000),
        ]
    }

    // Two `api` spans in the first minute, one `db` span in each minute.
    fn api_db_metric_spans() -> Vec<InputSpan> {
        let mut spans = api_and_db_metric_spans();
        spans.push(sp_at(1, 4, None, "db", 70_000));
        spans
    }

    fn api_db_metric_engine() -> TraceqlEngine<InMemorySpanStore> {
        one_trace_engine(api_db_metric_spans())
    }

    // `count_over_time() | by(event:name)` over one minute, sorted by labels.
    async fn count_by_event_name(s: InMemorySpanStore) -> Vec<TraceMetricSeries> {
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        sorted_series(
            &e,
            "{ event:name != nil } | count_over_time() | by(event:name)",
            TWO_BUCKETS,
        )
        .await
    }

    // The one-minute `by(event:name)` series that counts one span in the first bucket.
    fn event_name_series(event_name: &str) -> TraceMetricSeries {
        TraceMetricSeries {
            label_types: BTreeMap::default(),
            labels: vec![MetricLabel::new("event:name", event_name)],
            points: vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)],
            exemplars: vec![],
        }
    }

    fn metric_series(labels: Vec<MetricLabel>, points: Vec<MetricPoint>) -> TraceMetricSeries {
        TraceMetricSeries {
            label_types: BTreeMap::default(),
            labels,
            points,
            exemplars: Vec::new(),
        }
    }

    // In the first minute: two `api` spans, one `db`, three `worker`.
    fn api_db_worker_engine() -> TraceqlEngine<InMemorySpanStore> {
        let mut spans = api_and_db_metric_spans();
        spans.extend([
            sp_at(1, 4, None, "worker", 30_000),
            sp_at(1, 5, None, "worker", 40_000),
            sp_at(1, 6, None, "worker", 50_000),
        ]);
        one_trace_engine(spans)
    }

    // The series of a metrics query over the first minute, sorted by labels.
    async fn first_minute_series(
        e: &TraceqlEngine<InMemorySpanStore>,
        query: &str,
    ) -> Vec<TraceMetricSeries> {
        let mut series = e
            .query_range("t", query, 0, 59_999, 60_000)
            .await
            .unwrap()
            .series;
        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        series
    }

    /// A metrics query window that starts at zero, in nanoseconds.
    #[derive(Clone, Copy)]
    struct MetricWindow {
        end_ns: i64,
        step_ns: i64,
    }

    /// Two one-minute buckets: `[0, 60s)` and the bucket at `60s`.
    const TWO_BUCKETS: MetricWindow = MetricWindow {
        end_ns: 60_000,
        step_ns: 60_000,
    };

    // The series of a metrics query over `window`, sorted by labels.
    async fn sorted_series<S: SpanStore>(
        e: &TraceqlEngine<S>,
        query: &str,
        window: MetricWindow,
    ) -> Vec<TraceMetricSeries> {
        let mut series = e
            .query_range("t", query, 0, window.end_ns, window.step_ns)
            .await
            .unwrap()
            .series;
        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        series
    }

    // The span ids of every matched span in `response`, sorted.
    fn sorted_matched_span_ids(response: &SearchResponse) -> Vec<[u8; 8]> {
        let mut ids = response
            .traces
            .iter()
            .flat_map(|trace| trace.span_sets.iter())
            .flat_map(|set| set.spans.iter())
            .map(|span| span.span_id)
            .collect::<Vec<_>>();
        ids.sort_unstable();
        ids
    }

    // One trace of `api` spans at 0s and 10s and a `db` span at 20s.
    fn api_api_db_engine() -> TraceqlEngine<InMemorySpanStore> {
        one_trace_engine(vec![
            sp_at(1, 1, None, "api", 0),
            sp_at(1, 2, None, "api", 10_000),
            sp_at(1, 3, None, "db", 20_000),
        ])
    }

    // One trace of an `api` root span at 0s and its `api` child at 10s.
    fn api_parent_child_engine() -> TraceqlEngine<InMemorySpanStore> {
        one_trace_engine(vec![
            sp_at(1, 1, None, "api", 0),
            sp_at(1, 2, Some(1), "api", 10_000),
        ])
    }

    // The top two `api_db_worker_engine` services by span count.
    fn api_and_worker_counts() -> Vec<TraceMetricSeries> {
        vec![
            metric_series(
                vec![MetricLabel::new("span.svc", "api")],
                vec![MetricPoint::new(0, 2.0)],
            ),
            metric_series(
                vec![MetricLabel::new("span.svc", "worker")],
                vec![MetricPoint::new(0, 3.0)],
            ),
        ]
    }

    // The block attribute columns, typed from the key, string, int, double,
    // and bool list arrays in that order.
    fn block_attr_fields(columns: [&dyn Array; 5]) -> Vec<ArrowField> {
        let [keys, str_values, int_values, double_values, bool_values] = columns;
        vec![
            ArrowField::new(BLOCK_ATTR_KEYS, keys.data_type().clone(), true),
            ArrowField::new(BLOCK_ATTR_VALUE, str_values.data_type().clone(), true),
            ArrowField::new(BLOCK_ATTR_VALUE_INT, int_values.data_type().clone(), true),
            ArrowField::new(
                BLOCK_ATTR_VALUE_DOUBLE,
                double_values.data_type().clone(),
                true,
            ),
            ArrowField::new(BLOCK_ATTR_VALUE_BOOL, bool_values.data_type().clone(), true),
        ]
    }

    fn engine() -> TraceqlEngine<InMemorySpanStore> {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![sp(9, 1, None, "a"), sp(9, 2, Some(1), "b")],
        );
        s.push_trace("t", "x", "root", vec![sp(8, 1, None, "x")]);
        TraceqlEngine::new(Arc::new(s), EngineOpts::default())
    }

    struct BatchSpanStore {
        batch: RecordBatch,
    }

    #[async_trait::async_trait]
    impl SpanStore for BatchSpanStore {
        async fn scan(
            &self,
            _tenant: &str,
            _matchers: &[SpanMatcher],
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<ScanResult> {
            let schema = self.batch.schema();
            let ctx = SessionContext::new();
            let inspected = ByteSize::from_bytes(
                u64::try_from(self.batch.get_array_memory_size()).unwrap_or(u64::MAX),
            );
            let table = MemTable::try_new(schema, vec![vec![self.batch.clone()]])?;
            ctx.register_table("spans", Arc::new(table))?;
            Ok(ScanResult {
                ctx,
                span_table: "spans".into(),
                inspected,
            })
        }

        async fn trace_by_id(
            &self,
            _tenant: &str,
            _trace_id: &[u8; 16],
        ) -> Result<Option<TraceSpans>> {
            Ok(None)
        }
    }

    #[async_trait::async_trait]
    impl TagCatalog for BatchSpanStore {
        async fn tag_names(
            &self,
            _tenant: &str,
            _scope: Option<TagScope>,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<ScopedTag>> {
            Ok(Vec::new())
        }

        async fn tag_values(
            &self,
            _tenant: &str,
            _tag: &str,
            _start_ns: i64,
            _end_ns: i64,
        ) -> Result<Vec<TypedValue>> {
            Ok(Vec::new())
        }
    }

    fn dictionary_metric_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            ArrowField::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
            ArrowField::new(COL_PARENT_SPAN_ID, DataType::FixedSizeBinary(8), true),
            ArrowField::new(COL_NS_LEFT, DataType::Int32, false),
            ArrowField::new(COL_NS_RIGHT, DataType::Int32, false),
            ArrowField::new(COL_PARENT_ID, DataType::Int32, false),
            ArrowField::new(COL_CHILD_COUNT, DataType::Int32, false),
            ArrowField::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, true),
            ArrowField::new(COL_ROOT_SPAN_NAME, DataType::Utf8, true),
            ArrowField::new(COL_TRACE_START, DataType::Int64, false),
            ArrowField::new(COL_TRACE_DURATION, DataType::Int64, false),
            ArrowField::new(COL_NAME, DataType::Utf8, true),
            ArrowField::new(COL_KIND, DataType::Int32, false),
            ArrowField::new(COL_START, DataType::Int64, false),
            ArrowField::new(COL_DURATION, DataType::Int64, false),
            ArrowField::new(COL_STATUS_CODE, DataType::Int32, false),
            ArrowField::new(COL_STATUS_MESSAGE, DataType::Utf8, true),
            ArrowField::new(COL_INSTRUMENTATION_NAME, DataType::Utf8, true),
            ArrowField::new(COL_INSTRUMENTATION_VERSION, DataType::Utf8, true),
            ArrowField::new(
                format!("{ATTR_PREFIX}http.method"),
                DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                true,
            ),
        ]));
        let mut methods = StringDictionaryBuilder::<Int32Type>::new();
        methods.append_value("GET");
        methods.append_value("POST");
        let mut columns = root_span_columns(&[
            RootSpanRow {
                trace_id: [1; 16],
                span_id: [1; 8],
                root_service_name: "api",
                root_span_name: "GET /",
                trace_duration_ns: 10,
                name: "GET /",
                kind: 2,
                duration_ns: 10,
                ..RootSpanRow::default()
            },
            RootSpanRow {
                trace_id: [2; 16],
                span_id: [2; 8],
                root_service_name: "api",
                root_span_name: "POST /",
                trace_duration_ns: 20,
                name: "POST /",
                kind: 2,
                start_ns: 10_000,
                duration_ns: 20,
                ..RootSpanRow::default()
            },
        ]);
        columns.push(Arc::new(methods.finish()));

        RecordBatch::try_new(schema, columns).unwrap()
    }

    #[test]
    fn compare_span_identities_reads_every_trace_and_span_id() {
        let identities = compare_span_identities(&[dictionary_metric_batch()]).unwrap();

        assert!(
            identities
                == HashSet::from([
                    SpanIdentity {
                        trace_id: [1; 16],
                        span_id: [1; 8]
                    },
                    SpanIdentity {
                        trace_id: [2; 16],
                        span_id: [2; 8]
                    },
                ])
        );
    }

    #[tokio::test]
    async fn search_selector_returns_matching_trace() {
        let e = engine();
        let r = search_all(&e, "{ .svc = \"b\" }").await;
        check!(r.traces.len() == 1);
        check!(r.traces[0].trace_id == [9; 16]);
        check!(r.traces[0].root_service_name == "a");
        check!(r.traces[0].span_sets[0].matched == 1);
        check!(r.traces[0].span_sets[0].spans[0].span_id == [2; 8]);
    }

    #[tokio::test]
    async fn search_match_all_selectors_return_every_trace() {
        // Grafana's Tempo Explore "Search" tab and TraceQL-metrics default to the
        // empty spanset `{}`; `{ true }` is the equivalent constant-true filter.
        // Both must match every span (not error, not match-none). `{ false }`
        // matches nothing. The fixture holds two traces (3 spans total).
        let e = engine();
        for q in ["{}", "{ true }", "{true}"] {
            let r = e.search("t", q, 0, 100_000, 20).await.unwrap();
            assert!(r.traces.len() == 2, "query {q:?} should match both traces");
        }
        let none = search_all(&e, "{ false }").await;
        assert!(none.traces.is_empty(), "{{ false }} should match no traces");
    }

    #[tokio::test]
    async fn search_reports_inspected_bytes() {
        // The scan's decoded data size is threaded up to `inspected` (non-zero
        // for a non-empty store) for the Tempo search `metrics.inspectedBytes`.
        let e = engine();
        let r = search_all(&e, "{ .svc = \"b\" }").await;
        assert!(r.inspected.bytes_u64() > 0);
    }

    #[tokio::test]
    async fn search_deduplicates_same_span_returned_by_multiple_tiers() {
        let mut s = InMemorySpanStore::new();
        let span = sp(9, 1, None, "a");
        s.push_trace("t", "a", "root", vec![span.clone(), span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = search_all(&e, "{ .svc = \"a\" }").await;

        check!(r.traces.len() == 1);
        check!(r.traces[0].span_sets[0].matched == 1);
        check!(r.traces[0].span_sets[0].spans.len() == 1);
        check!(r.traces[0].span_sets[0].spans[0].span_id == [1; 8]);
    }

    #[tokio::test]
    async fn search_most_recent_hint_returns_newest_traces_first() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "old", "root", vec![sp_at(1, 1, None, "match", 1_000)]);
        s.push_trace("t", "new", "root", vec![sp_at(2, 1, None, "match", 10_000)]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = e
            .search(
                "t",
                "{ .svc = \"match\" } with (most_recent=true)",
                0,
                100_000,
                1,
            )
            .await
            .unwrap();

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].trace_id == [2; 16]);
    }

    #[tokio::test]
    async fn search_pipeline_with_preserves_matched_spans() {
        let e = engine();
        let r = search_all(
            &e,
            "{ .svc = \"b\" } | with(is_error = span:status = error)",
        )
        .await;

        check_single_trace_nine_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn search_inter_brace_and_matches_different_spans() {
        let e = engine();
        let r = search_all(&e, "{ .svc = \"a\" } && { .svc = \"b\" }").await;
        check_trace_nine_matched_twice(&r);
    }

    #[tokio::test]
    async fn search_inter_brace_and_keeps_nested_selector_predicate() {
        let event_span = cache_miss_span();
        let peer = sp(9, 2, Some(1), "b");
        let unrelated = sp(8, 1, None, "b");
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![event_span, peer]);
        s.push_trace("t", "b", "root", vec![unrelated]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = search_all(&e, "{ event:name = \"cache.miss\" } && { .svc = \"b\" }").await;

        check_trace_nine_matched_twice(&r);
        let spans = &r.traces[0].span_sets[0].spans;
        check!(spans.iter().any(|span| span.span_id == [1; 8]));
        check!(spans.iter().any(|span| span.span_id == [2; 8]));
    }

    #[tokio::test]
    async fn search_descendant_structural_returns_right_hand_spans() {
        let e = engine();
        let r = search_all(&e, "{ .svc = \"a\" } >> { .svc = \"b\" }").await;
        check_single_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn search_selector_matches_child_count_intrinsic() {
        let e = engine();
        let r = search_all(&e, "{ span:childCount = 1 }").await;
        check_single_trace_nine_matched_span(&r, 1);
    }

    #[tokio::test]
    async fn search_scopeless_nested_set_parent_matches_scoped_and_finds_roots() {
        // Grafana's Traces Drilldown selects root spans with the scopeless
        // primary signal `nestedSetParent < 0`. This must (a) parse as the
        // intrinsic rather than `attr.nestedSetParent`, and (b) actually match
        // roots, whose sentinel is -1. It is equivalent to the scoped form and
        // returns at least one trace (every trace has a root).
        let e = engine();
        let scopeless = search_all(&e, "{ nestedSetParent < 0 }").await;
        let scoped = search_all(&e, "{ span:nestedSetParent < 0 }").await;
        let mut scopeless_ids: Vec<_> = scopeless.traces.iter().map(|t| t.trace_id).collect();
        let mut scoped_ids: Vec<_> = scoped.traces.iter().map(|t| t.trace_id).collect();
        scopeless_ids.sort_unstable();
        scoped_ids.sort_unstable();
        assert!(
            !scopeless_ids.is_empty(),
            "roots (nestedSetParent < 0) must exist"
        );
        assert!(scopeless_ids == scoped_ids);
    }

    #[tokio::test]
    async fn search_selector_matches_instrumentation_name_intrinsic() {
        let e = engine();
        let r = search_all(&e, "{ instrumentation:name = \"tracer\" }").await;
        assert!(r.traces.len() == 2);
        let first = r
            .traces
            .iter()
            .find(|trace| trace.trace_id == [9; 16])
            .unwrap();
        let second = r
            .traces
            .iter()
            .find(|trace| trace.trace_id == [8; 16])
            .unwrap();
        assert!(first.span_sets[0].matched == 2);
        assert!(second.span_sets[0].matched == 1);
    }

    #[tokio::test]
    async fn instrumentation_attributes_filter_and_group_metrics() {
        let mut span = sp_at(1, 1, None, "api", 0);
        span.attrs.push(Attribute::new(
            format!("{INSTRUMENTATION_ATTR_PREFIX}language"),
            AttrValue::Str("rust".into()),
        ));
        let engine = one_trace_engine(vec![span]);

        let search = engine
            .search(
                "t",
                "{ instrumentation.language = \"rust\" }",
                0,
                60_000,
                20,
            )
            .await
            .unwrap();
        assert!(search.traces.len() == 1);
        let metrics = query_one_bucket(
            &engine,
            "{} | count_over_time() | by(instrumentation.language)",
        )
        .await;
        assert!(
            metrics.series[0].labels == vec![MetricLabel::new("instrumentation.language", "rust")]
        );
    }

    #[tokio::test]
    async fn search_selector_matches_resource_service_name() {
        let e = engine();
        let r = search_all(&e, "{ resource.service.name = \"a\" }").await;

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].root_service_name == "a");
    }

    #[tokio::test]
    async fn search_selector_matches_trace_id_hex_string() {
        let e = engine();
        let r = search_all(&e, "{ trace:id = \"09090909090909090909090909090909\" }").await;

        check_trace_nine_matched_twice(&r);
    }

    #[tokio::test]
    async fn search_selector_matches_span_id_hex_string() {
        let e = engine();
        let r = search_all(&e, "{ span:id = \"0202020202020202\" }").await;

        check_single_trace_nine_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn search_selector_matches_parent_id_hex_string() {
        let e = engine();
        let r = search_all(&e, "{ span:parentID = \"0101010101010101\" }").await;

        check_single_trace_nine_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn search_selector_matches_event_intrinsic() {
        let mut span = sp(9, 1, None, "a");
        span.events = vec![EventRef {
            time_since_start: nanos(50),
            name: "cache.miss".into(),
            attributes: vec![Attribute::new("cache.key", AttrValue::Str("users".into()))],
        }];
        let e = one_trace_engine(vec![span, sp(8, 1, None, "x")]);

        let r = search_all(&e, "{ event:name = \"cache.miss\" }").await;

        check_single_trace_nine_matched_span(&r, 1);
    }

    #[tokio::test]
    async fn search_selector_matches_event_intrinsic_presence() {
        let e = cache_miss_engine();

        let r = search_all(&e, "{ event:name != nil }").await;

        check_single_matched_span(&r, 1);
    }

    #[tokio::test]
    async fn search_selector_not_event_intrinsic_excludes_matching_spans() {
        let e = cache_miss_engine();

        let r = search_all(&e, "{ !event:name = \"cache.miss\" }").await;

        check_single_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn search_selector_grouped_not_event_intrinsic() {
        let e = cache_miss_engine();

        let r = search_all(&e, "{ !(event:name = \"cache.miss\") }").await;

        check_single_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn search_selector_not_nested_or_excludes_each_branch() {
        let mut miss_span = sp(9, 1, None, "a");
        miss_span.events = vec![EventRef {
            time_since_start: nanos(50),
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut hit_span = sp(9, 2, Some(1), "b");
        hit_span.events = vec![EventRef {
            time_since_start: nanos(60),
            name: "cache.hit".into(),
            attributes: Vec::new(),
        }];
        let peer = sp(9, 3, Some(1), "c");
        let e = one_trace_engine(vec![miss_span, hit_span, peer]);

        let r = search_all(
            &e,
            "{ !(event:name = \"cache.miss\" || event:name = \"cache.hit\") }",
        )
        .await;

        check_single_matched_span(&r, 3);
    }

    #[tokio::test]
    async fn search_selector_not_nested_and_uses_disjuncts() {
        let mut miss_users = sp(9, 1, None, "a");
        miss_users.events = vec![EventRef {
            time_since_start: nanos(50),
            name: "cache.miss".into(),
            attributes: vec![Attribute::new("cache.key", AttrValue::Str("users".into()))],
        }];
        let mut miss_orders = sp(9, 2, Some(1), "b");
        miss_orders.events = vec![EventRef {
            time_since_start: nanos(60),
            name: "cache.miss".into(),
            attributes: vec![Attribute::new("cache.key", AttrValue::Str("orders".into()))],
        }];
        let mut hit_users = sp(9, 3, Some(1), "c");
        hit_users.events = vec![EventRef {
            time_since_start: nanos(70),
            name: "cache.hit".into(),
            attributes: vec![Attribute::new("cache.key", AttrValue::Str("users".into()))],
        }];
        let e = one_trace_engine(vec![miss_users, miss_orders, hit_users]);

        let r = search_all(
            &e,
            "{ !(event:name = \"cache.miss\" && event.cache.key = \"users\") }",
        )
        .await;

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 2);
        let spans = &r.traces[0].span_sets[0].spans;
        check!(!spans.iter().any(|span| span.span_id == [1; 8]));
        check!(spans.iter().any(|span| span.span_id == [2; 8]));
        check!(spans.iter().any(|span| span.span_id == [3; 8]));
    }

    #[tokio::test]
    async fn search_selector_requires_event_matchers_on_same_event() {
        let mut split_events = sp(9, 1, None, "a");
        split_events.events = vec![
            EventRef {
                time_since_start: nanos(50),
                name: "cache.miss".into(),
                attributes: vec![Attribute::new("cache.key", AttrValue::Str("orders".into()))],
            },
            EventRef {
                time_since_start: nanos(60),
                name: "cache.hit".into(),
                attributes: vec![Attribute::new("cache.key", AttrValue::Str("users".into()))],
            },
        ];
        let mut same_event = sp(9, 2, Some(1), "b");
        same_event.events = vec![EventRef {
            time_since_start: nanos(70),
            name: "cache.miss".into(),
            attributes: vec![Attribute::new("cache.key", AttrValue::Str("users".into()))],
        }];
        let e = one_trace_engine(vec![split_events, same_event]);

        let r = search_all(
            &e,
            "{ event:name = \"cache.miss\" && event.cache.key = \"users\" }",
        )
        .await;

        check_single_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn search_selector_or_with_nested_event_filters_each_branch() {
        let event_span = cache_miss_span();
        let attr_span = sp(9, 2, Some(1), "b");
        let unrelated = sp(9, 3, Some(1), "c");
        let e = one_trace_engine(vec![event_span, attr_span, unrelated]);

        let r = search_all(&e, "{ event:name = \"cache.miss\" || .svc = \"b\" }").await;

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].span_sets[0].matched == 2);
        let spans = &r.traces[0].span_sets[0].spans;
        check!(spans.iter().any(|span| span.span_id == [1; 8]));
        check!(spans.iter().any(|span| span.span_id == [2; 8]));
        check!(!spans.iter().any(|span| span.span_id == [3; 8]));
    }

    #[tokio::test]
    async fn search_selector_applies_array_any_none_semantics_to_repeated_attrs() {
        let e = one_trace_engine(vec![
            InputSpan {
                attrs: vec![
                    Attribute::new("http.method", AttrValue::Str("GET".into())),
                    Attribute::new("http.method", AttrValue::Str("POST".into())),
                ],
                ..sp(9, 1, None, "a")
            },
            InputSpan {
                attrs: vec![Attribute::new(
                    "http.method",
                    AttrValue::Str("DELETE".into()),
                )],
                ..sp(9, 2, Some(1), "b")
            },
        ]);

        let r = search_all(&e, "{ span.http.method = \"POST\" }").await;
        check_single_matched_span(&r, 1);

        let r = search_all(&e, "{ span.http.method != \"POST\" }").await;
        check_single_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn search_selector_matches_link_attribute_scope() {
        let mut span = sp(9, 1, None, "a");
        span.links = vec![LinkRef {
            trace_id: [7; 16],
            span_id: [6; 8],
            attributes: vec![Attribute::new("link.kind", AttrValue::Str("retry".into()))],
        }];
        let e = one_trace_engine(vec![span, sp(8, 1, None, "x")]);

        let r = search_all(&e, "{ link.link.kind = \"retry\" }").await;

        check_single_trace_nine_matched_span(&r, 1);
    }

    #[tokio::test]
    async fn search_selector_matches_nested_set_parent_intrinsic() {
        let e = engine();
        let r = search_all(&e, "{ span:nestedSetParent = 1 }").await;

        check_single_trace_nine_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn search_selector_matches_status_enum_value() {
        let e = one_trace_engine(vec![
            InputSpan {
                status_code: 2,
                ..sp(9, 1, None, "a")
            },
            sp(9, 2, Some(1), "b"),
        ]);
        let r = search_all(&e, "{ span:status = error }").await;

        check_single_trace_nine_matched_span(&r, 1);
    }

    #[tokio::test]
    async fn search_selector_matches_kind_enum_value() {
        let e = one_trace_engine(vec![
            InputSpan {
                kind: 2,
                ..sp(9, 1, None, "a")
            },
            sp(9, 2, Some(1), "b"),
        ]);
        let r = search_all(&e, "{ span:kind = server }").await;

        check_single_trace_nine_matched_span(&r, 1);
    }

    #[tokio::test]
    async fn bare_service_name_selector_matches_resource_service_name() {
        let e = engine();
        let r = search_all(&e, "{ .service.name = \"a\" }").await;

        assert!(r.traces.len() == 1);
        assert!(r.traces[0].root_service_name == "a");
    }

    #[tokio::test]
    async fn parent_scope_selector_matches_direct_parent_attributes() {
        let e = engine();
        let r = search_all(&e, "{ parent.svc = \"a\" }").await;

        check_single_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn parent_scope_selector_works_inside_trace_level_and() {
        let e = engine();
        let r = search_all(&e, "{ parent.svc = \"a\" } && { .svc = \"b\" }").await;

        check_single_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn mixed_parent_and_event_selector_keeps_parent_predicate() {
        let mut wanted = sp(9, 2, Some(1), "b");
        wanted.events = vec![EventRef {
            time_since_start: nanos(50),
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut wrong_parent = sp(8, 2, Some(1), "b");
        wrong_parent.events = vec![EventRef {
            time_since_start: nanos(50),
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![sp(9, 1, None, "a"), wanted]);
        s.push_trace("t", "x", "root", vec![sp(8, 1, None, "x"), wrong_parent]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = search_all(&e, "{ parent.svc = \"a\" && event:name = \"cache.miss\" }").await;

        check_single_trace_nine_matched_span(&r, 2);
    }

    #[tokio::test]
    async fn search_limit_uses_default_for_zero_and_caps_result_count() {
        let e = engine();
        let r = e
            .search("t", "{ .svc != nil }", 0, 100_000, 1)
            .await
            .unwrap();
        assert!(r.traces.len() == 1);
    }

    #[tokio::test]
    async fn trace_by_id_path() {
        let e = engine();
        let got = e.trace_by_id("t", &[9; 16]).await.unwrap().unwrap();
        assert!(got.spans.len() == 2);
        assert!(e.trace_by_id("t", &[1; 16]).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn trace_by_id_within_returns_spans_in_window() {
        // Delegates to the store; must surface the real trace, not Ok(None).
        let e = engine();
        let got = e
            .trace_by_id_within("t", &[9; 16], 0, 100_000)
            .await
            .unwrap()
            .unwrap();
        assert!(got.spans.len() == 2);
        // A window after the trace retains no spans (but still returns the trace).
        let out = e
            .trace_by_id_within("t", &[9; 16], 100_000, 200_000)
            .await
            .unwrap()
            .unwrap();
        assert!(out.spans.is_empty());
    }

    #[tokio::test]
    async fn tag_names_and_values_delegate_to_store() {
        // Both must surface the store's non-empty results, not Ok(vec![]).
        let e = engine();
        let names = e.tag_names("t", None, 0, 100_000).await.unwrap();
        assert!(!names.is_empty());
        assert!(
            names
                .iter()
                .any(|scoped| scoped.tags.iter().any(|t| t == "svc"))
        );

        let values = e.tag_values("t", ".svc", 0, 100_000).await.unwrap();
        assert!(
            values
                == vec![
                    TypedValue {
                        type_: "string".into(),
                        value: "a".into(),
                    },
                    TypedValue {
                        type_: "string".into(),
                        value: "b".into(),
                    },
                    TypedValue {
                        type_: "string".into(),
                        value: "x".into(),
                    },
                ]
        );
    }

    #[tokio::test]
    async fn count_over_time_counts_matched_spans_per_bucket() {
        let e = one_trace_engine(vec![
            sp_at(1, 1, None, "a", 0),
            sp_at(1, 2, None, "a", 10_000),
            sp_at(1, 3, None, "a", 60_000),
            sp_at(1, 4, None, "b", 70_000),
        ]);
        let got = e
            .query_range(
                "t",
                "{ .svc = \"a\" } | count_over_time()",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(got.series.len() == 1);
        assert!(
            got.series[0].points
                == vec![
                    MetricPoint::new(0, 2.0),
                    MetricPoint::new(60_000, 1.0),
                    MetricPoint::new(120_000, 0.0)
                ]
        );
    }

    #[tokio::test]
    async fn rate_divides_bucket_count_by_step_seconds() {
        let e = one_trace_engine(vec![
            sp_at(1, 1, None, "a", 0),
            sp_at(1, 2, None, "a", 10_000),
            sp_at(1, 3, None, "a", 20_000),
        ]);
        let got = e
            .query_range(
                "t",
                "{ .svc = \"a\" } | rate()",
                0,
                10_000_000_000,
                10_000_000_000,
            )
            .await
            .unwrap();
        assert!(got.series.len() == 1);
        assert!(
            got.series[0].points
                == vec![
                    MetricPoint::new(0, 0.3),
                    MetricPoint::new(10_000_000_000, 0.0)
                ]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_attribute_emits_one_series_per_group() {
        let e = api_db_metric_engine();
        let got = sorted_series(
            &e,
            "{ .svc != nil } | count_over_time() | by(span.svc)",
            MetricWindow {
                end_ns: 120_000,
                step_ns: 60_000,
            },
        )
        .await;
        assert!(
            got == vec![
                metric_series(
                    vec![MetricLabel::new("span.svc", "api")],
                    vec![
                        MetricPoint::new(0, 2.0),
                        MetricPoint::new(60_000, 0.0),
                        MetricPoint::new(120_000, 0.0)
                    ]
                ),
                metric_series(
                    vec![MetricLabel::new("span.svc", "db")],
                    vec![
                        MetricPoint::new(0, 1.0),
                        MetricPoint::new(60_000, 1.0),
                        MetricPoint::new(120_000, 0.0)
                    ]
                ),
            ]
        );
    }

    #[tokio::test]
    async fn metric_comparison_filter_keeps_only_passing_samples() {
        let e = api_db_metric_engine();
        let got = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc) > 1",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        assert!(
            got == vec![metric_series(
                vec![MetricLabel::new("span.svc", "api")],
                vec![MetricPoint::new(0, 2.0)]
            )]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_resource_service_name_uses_root_service_column() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![sp_at(1, 1, None, "api", 0)]);
        s.push_trace(
            "t",
            "billing",
            "root",
            vec![sp_at(2, 1, None, "api", 10_000)],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = sorted_series(
            &e,
            "{ .svc = \"api\" } | count_over_time() | by(resource.service.name)",
            TWO_BUCKETS,
        )
        .await;
        assert!(
            got == vec![
                metric_series(
                    vec![MetricLabel::new("resource.service.name", "billing")],
                    vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
                ),
                metric_series(
                    vec![MetricLabel::new("resource.service.name", "checkout")],
                    vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
                ),
            ]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_dictionary_promoted_attr_decodes_labels() {
        let store = BatchSpanStore {
            batch: dictionary_metric_batch(),
        };
        let e = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        let got = sorted_series(
            &e,
            "{ span:name != nil } | count_over_time() | by(span.http.method)",
            TWO_BUCKETS,
        )
        .await;
        assert!(
            got == vec![
                metric_series(
                    vec![MetricLabel::new("span.http.method", "GET")],
                    vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
                ),
                metric_series(
                    vec![MetricLabel::new("span.http.method", "POST")],
                    vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
                ),
            ]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_event_name_intrinsic() {
        let mut miss = sp_at(1, 1, None, "api", 0);
        miss.events = vec![EventRef {
            time_since_start: nanos(50),
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut hit = sp_at(1, 2, None, "api", 10_000);
        hit.events = vec![EventRef {
            time_since_start: nanos(60),
            name: "cache.hit".into(),
            attributes: Vec::new(),
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![miss, hit]);
        let got = count_by_event_name(s).await;
        assert!(
            got == vec![
                event_name_series("cache.hit"),
                event_name_series("cache.miss"),
            ]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_event_name_uses_first_event_on_a_span() {
        let mut span = sp_at(1, 1, None, "api", 0);
        span.events = vec![
            EventRef {
                time_since_start: nanos(50),
                name: "cache.miss".into(),
                attributes: Vec::new(),
            },
            EventRef {
                time_since_start: nanos(60),
                name: "cache.hit".into(),
                attributes: Vec::new(),
            },
        ];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![span]);
        let got = count_by_event_name(s).await;
        assert!(got == vec![event_name_series("cache.miss")]);
    }

    #[tokio::test]
    async fn count_over_time_by_event_attribute_uses_first_event_attribute() {
        let mut span = sp_at(1, 1, None, "api", 0);
        span.events = vec![
            EventRef {
                time_since_start: nanos(50),
                name: "cache.lookup".into(),
                attributes: vec![Attribute::new("cache.key", AttrValue::Str("users".into()))],
            },
            EventRef {
                time_since_start: nanos(60),
                name: "cache.lookup".into(),
                attributes: vec![Attribute::new("cache.key", AttrValue::Str("orders".into()))],
            },
        ];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = sorted_series(
            &e,
            "{ .svc = \"api\" } | count_over_time() | by(event.cache.key)",
            TWO_BUCKETS,
        )
        .await;
        assert!(
            got == vec![metric_series(
                vec![MetricLabel::new("event.cache.key", "users")],
                vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
            ),]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_link_trace_id_intrinsic_uses_hex_label() {
        let mut span = sp_at(1, 1, None, "api", 0);
        span.links = vec![LinkRef {
            trace_id: [9; 16],
            span_id: [8; 8],
            attributes: Vec::new(),
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = query_one_bucket(
            &e,
            "{ link:traceID != nil } | count_over_time() | by(link:traceID)",
        )
        .await
        .series;

        assert!(
            got == vec![metric_series(
                vec![MetricLabel::new(
                    "link:traceID",
                    "09090909090909090909090909090909",
                )],
                vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
            )]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_link_span_id_uses_first_link_without_link_selector() {
        let mut span = sp_at(1, 1, None, "api", 0);
        span.links = vec![
            LinkRef {
                trace_id: [9; 16],
                span_id: [8; 8],
                attributes: Vec::new(),
            },
            LinkRef {
                trace_id: [7; 16],
                span_id: [6; 8],
                attributes: Vec::new(),
            },
        ];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root", vec![span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = sorted_series(
            &e,
            "{ .svc = \"api\" } | count_over_time() | by(link:spanID)",
            TWO_BUCKETS,
        )
        .await;
        assert!(
            got == vec![metric_series(
                vec![MetricLabel::new("link:spanID", "0808080808080808")],
                vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
            ),]
        );
    }

    #[tokio::test]
    async fn inert_stage_before_metric_aggregate_is_ignored() {
        let e = api_api_db_engine();
        let got = sorted_series(
            &e,
            "{ .svc != nil } | select(span.svc) | count_over_time() | by(span.svc)",
            TWO_BUCKETS,
        )
        .await;
        assert!(
            got == vec![
                metric_series(
                    vec![MetricLabel::new("span.svc", "api")],
                    vec![MetricPoint::new(0, 2.0), MetricPoint::new(60_000, 0.0)]
                ),
                metric_series(
                    vec![MetricLabel::new("span.svc", "db")],
                    vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
                ),
            ]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_kind_and_status_intrinsics() {
        let e = one_trace_engine(vec![
            InputSpan {
                kind: 2,
                status_code: 0,
                ..sp_at(1, 1, None, "api", 0)
            },
            InputSpan {
                kind: 2,
                status_code: 2,
                ..sp_at(1, 2, None, "api", 10_000)
            },
            InputSpan {
                kind: 3,
                status_code: 2,
                ..sp_at(1, 3, None, "api", 20_000)
            },
        ]);
        let got = sorted_series(
            &e,
            "{ .svc = \"api\" } | count_over_time() | by(span:kind, span:status)",
            TWO_BUCKETS,
        )
        .await;
        // Spans per (kind, status) pair: one each, all in the first minute.
        let expected = [
            ["client", "error"],
            ["server", "error"],
            ["server", "unset"],
        ]
        .map(|[kind, status]| {
            metric_series(
                vec![
                    MetricLabel::new("kind", kind),
                    MetricLabel::new("status", status),
                ],
                vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)],
            )
        });
        assert!(got == expected);
    }

    #[tokio::test]
    async fn count_over_time_by_status_message_intrinsic() {
        let e = one_trace_engine(vec![
            InputSpan {
                status_message: "timeout".into(),
                ..sp_at(1, 1, None, "api", 0)
            },
            InputSpan {
                status_message: "timeout".into(),
                ..sp_at(1, 2, None, "api", 10_000)
            },
            InputSpan {
                status_message: "cancelled".into(),
                ..sp_at(1, 3, None, "api", 20_000)
            },
        ]);
        let got = sorted_series(
            &e,
            "{ .svc = \"api\" } | count_over_time() | by(span:statusMessage)",
            TWO_BUCKETS,
        )
        .await;
        assert!(
            got == vec![
                metric_series(
                    vec![MetricLabel::new("statusMessage", "cancelled")],
                    vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
                ),
                metric_series(
                    vec![MetricLabel::new("statusMessage", "timeout")],
                    vec![MetricPoint::new(0, 2.0), MetricPoint::new(60_000, 0.0)]
                ),
            ]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_trace_id_intrinsic_uses_hex_label() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![sp_at(0x11, 1, None, "api", 0)]);
        s.push_trace("t", "b", "root", vec![sp_at(0x22, 1, None, "api", 10_000)]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let got = sorted_series(
            &e,
            "{ .svc = \"api\" } | count_over_time() | by(trace:id)",
            TWO_BUCKETS,
        )
        .await;
        assert!(
            got == vec![
                metric_series(
                    vec![MetricLabel::new(
                        "trace:id",
                        "11111111111111111111111111111111"
                    )],
                    vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
                ),
                metric_series(
                    vec![MetricLabel::new(
                        "trace:id",
                        "22222222222222222222222222222222"
                    )],
                    vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
                ),
            ]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_child_count_intrinsic() {
        let e = api_parent_child_engine();
        let got = sorted_series(
            &e,
            "{ .svc = \"api\" } | count_over_time() | by(span:childCount)",
            TWO_BUCKETS,
        )
        .await;
        assert!(
            got == vec![
                TraceMetricSeries {
                    label_types: BTreeMap::from([(
                        "span:childCount".into(),
                        TraceMetricLabelType::Int
                    )]),
                    labels: vec![MetricLabel::new("span:childCount", "0")],
                    points: vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)],
                    exemplars: vec![],
                },
                TraceMetricSeries {
                    label_types: BTreeMap::from([(
                        "span:childCount".into(),
                        TraceMetricLabelType::Int
                    )]),
                    labels: vec![MetricLabel::new("span:childCount", "1")],
                    points: vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)],
                    exemplars: vec![],
                },
            ]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_instrumentation_name_intrinsic() {
        let e = one_trace_engine(vec![sp_at(1, 1, None, "api", 0)]);
        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() | by(instrumentation:name)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        assert!(
            got == vec![metric_series(
                vec![MetricLabel::new("instrumentation:name", "tracer")],
                vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
            )]
        );
    }

    #[tokio::test]
    async fn count_over_time_by_nested_set_parent_intrinsic() {
        let e = api_parent_child_engine();
        let got = sorted_series(
            &e,
            "{ .svc = \"api\" } | count_over_time() | by(span:nestedSetParent)",
            TWO_BUCKETS,
        )
        .await;
        // Root span groups under nestedSetParent = -1 (Tempo root sentinel);
        // "-1" sorts before "1".
        assert!(
            got == vec![
                TraceMetricSeries {
                    label_types: BTreeMap::from([(
                        "nestedSetParent".into(),
                        TraceMetricLabelType::Int
                    )]),
                    labels: vec![MetricLabel::new("nestedSetParent", "-1")],
                    points: vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)],
                    exemplars: vec![],
                },
                TraceMetricSeries {
                    label_types: BTreeMap::from([(
                        "nestedSetParent".into(),
                        TraceMetricLabelType::Int
                    )]),
                    labels: vec![MetricLabel::new("nestedSetParent", "1")],
                    points: vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)],
                    exemplars: vec![],
                },
            ]
        );
    }

    #[tokio::test]
    async fn avg_and_sum_over_time_fold_duration_per_bucket() {
        let e = one_trace_engine(vec![
            InputSpan {
                duration: nanos(100),
                ..sp_at(1, 1, None, "api", 0)
            },
            InputSpan {
                duration: nanos(300),
                ..sp_at(1, 2, None, "api", 10_000)
            },
            InputSpan {
                duration: nanos(50),
                ..sp_at(1, 3, None, "api", 70_000)
            },
        ]);

        let avg = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | avg_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(
            avg.series[0].points
                == vec![
                    MetricPoint::new(0, 0.000_000_2),
                    MetricPoint::new(60_000, 0.000_000_05)
                ]
        );

        let sum = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | sum_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(
            sum.series[0].points
                == vec![
                    MetricPoint::new(0, 0.000_000_4),
                    MetricPoint::new(60_000, 0.000_000_05)
                ]
        );

        let min = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | min_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(
            min.series[0].points
                == vec![
                    MetricPoint::new(0, 0.000_000_1),
                    MetricPoint::new(60_000, 0.000_000_05)
                ]
        );

        let max = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | max_over_time(span:duration)",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(
            max.series[0].points
                == vec![
                    MetricPoint::new(0, 0.000_000_3),
                    MetricPoint::new(60_000, 0.000_000_05)
                ]
        );
    }

    #[tokio::test]
    async fn sum_over_time_can_fold_trace_duration_intrinsic() {
        let e = one_trace_engine(vec![
            InputSpan {
                duration: nanos(100),
                ..sp_at(1, 1, None, "api", 0)
            },
            InputSpan {
                duration: nanos(300),
                ..sp_at(1, 2, None, "api", 50)
            },
        ]);

        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | sum_over_time(trace:duration)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        assert!(got.series[0].points == vec![MetricPoint::new(0, 700.0)]);
    }

    #[tokio::test]
    async fn quantile_over_time_emits_per_quantile_series() {
        let e = one_trace_engine(vec![
            InputSpan {
                duration: nanos(100),
                ..sp_at(1, 1, None, "api", 0)
            },
            InputSpan {
                duration: nanos(200),
                ..sp_at(1, 2, None, "api", 10_000)
            },
            InputSpan {
                duration: nanos(300),
                ..sp_at(1, 3, None, "api", 20_000)
            },
            InputSpan {
                duration: nanos(400),
                ..sp_at(1, 4, None, "api", 30_000)
            },
            InputSpan {
                duration: nanos(500),
                ..sp_at(1, 5, None, "api", 40_000)
            },
        ]);
        let mut series = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | quantile_over_time(span:duration, .5, .9) | by(span.svc)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;
        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(
            series
                == vec![
                    metric_series(
                        vec![
                            MetricLabel::new("p", "0.5"),
                            MetricLabel::new("span.svc", "api")
                        ],
                        vec![
                            MetricPoint::new(0, 3.225_397_887_730_869e-07),
                            MetricPoint::new(60_000, 0.0)
                        ]
                    ),
                    metric_series(
                        vec![
                            MetricLabel::new("p", "0.9"),
                            MetricLabel::new("span.svc", "api")
                        ],
                        vec![
                            MetricPoint::new(0, 0.000_000_512),
                            MetricPoint::new(60_000, 0.0)
                        ]
                    ),
                ]
        );
    }

    // The one-minute `by(span.svc)` histogram series of `api` that counts one span in `bucket`.
    fn api_bucket_series(bucket: &str) -> TraceMetricSeries {
        TraceMetricSeries {
            label_types: BTreeMap::default(),
            labels: vec![
                MetricLabel::new("__bucket", bucket),
                MetricLabel::new("span.svc", "api"),
            ],
            points: vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)],
            exemplars: vec![],
        }
    }

    #[tokio::test]
    async fn histogram_over_time_emits_non_cumulative_logarithmic_counts() {
        let e = one_trace_engine(vec![
            InputSpan {
                duration: nanos(1_000_000),
                ..sp_at(1, 1, None, "api", 0)
            },
            InputSpan {
                duration: nanos(2_000_000_000),
                ..sp_at(1, 2, None, "api", 10_000)
            },
            InputSpan {
                duration: secs(12),
                ..sp_at(1, 3, None, "api", 20_000)
            },
        ]);
        let mut series = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | histogram_over_time(span:duration) | by(span.svc)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        series.sort_by(|a, b| a.labels.cmp(&b.labels));
        assert!(
            series
                == vec![
                    api_bucket_series("0.001048576"),
                    api_bucket_series("17.179869184"),
                    api_bucket_series("2.147483648"),
                ]
        );
    }

    #[tokio::test]
    async fn histogram_over_time_uses_tempo_buckets_with_legacy_configuration() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "a",
            "root",
            vec![InputSpan {
                duration: millis(3),
                ..sp_at(1, 1, None, "api", 0)
            }],
        );
        let engine = TraceqlEngine::new(
            Arc::new(store),
            EngineOpts {
                histogram_buckets: vec![millis(2)],
                ..EngineOpts::default()
            },
        );
        let response = query_one_bucket(&engine, "{} | histogram_over_time(span:duration)").await;
        let finite_buckets = response
            .series
            .iter()
            .filter_map(|series| {
                series
                    .labels
                    .iter()
                    .find(|label| label.key == "__bucket")
                    .map(|label| label.value.as_str())
            })
            .collect::<Vec<_>>();
        check!(finite_buckets == ["0.004194304"]);
    }

    #[tokio::test]
    async fn histogram_over_time_without_field_defaults_to_span_duration() {
        let e = one_trace_engine(vec![InputSpan {
            duration: nanos(1_000_000),
            ..sp_at(1, 1, None, "api", 0)
        }]);
        let series = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | histogram_over_time() | by(span.svc)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap()
            .series;

        assert!(series.iter().any(|s| {
            s.labels
                == vec![
                    MetricLabel::new("__bucket", "0.001048576"),
                    MetricLabel::new("span.svc", "api"),
                ]
                && s.points == vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 0.0)]
        }));
    }

    #[tokio::test]
    async fn topk_and_bottomk_rank_grouped_metric_series() {
        let e = api_db_worker_engine();

        let top = first_minute_series(
            &e,
            "{ .svc != nil } | count_over_time() | by(span.svc) | topk(2)",
        )
        .await;
        assert!(top == api_and_worker_counts());

        let bottom = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc) | bottomk(1)",
                0,
                59_999,
                60_000,
            )
            .await
            .unwrap();
        assert!(
            bottom.series
                == vec![metric_series(
                    vec![MetricLabel::new("span.svc", "db")],
                    vec![MetricPoint::new(0, 1.0)]
                )]
        );
    }

    #[tokio::test]
    async fn topk_by_ranks_grouped_metric_series() {
        let e = api_db_worker_engine();

        let top = first_minute_series(
            &e,
            "{ .svc != nil } | count_over_time() | topk(2) | by(span.svc)",
        )
        .await;
        assert!(top == api_and_worker_counts());
    }

    #[tokio::test]
    async fn spanset_grouping_before_metrics_preserves_rows_without_creating_metric_labels() {
        let e = api_api_db_engine();

        let series =
            first_minute_series(&e, "{ .svc != nil } | by(span.svc) | count_over_time()").await;

        assert!(
            series
                == vec![TraceMetricSeries {
                    label_types: BTreeMap::new(),
                    labels: Vec::new(),
                    points: vec![MetricPoint::new(0, 3.0)],
                    exemplars: Vec::new()
                }]
        );
    }

    #[tokio::test]
    async fn spanset_grouping_before_metrics_can_feed_ranked_ungrouped_metrics() {
        let e = api_db_worker_engine();

        let top = first_minute_series(
            &e,
            "{ .svc != nil } | by(span.svc) | count_over_time() | topk(2)",
        )
        .await;

        assert!(
            top == vec![TraceMetricSeries {
                label_types: BTreeMap::new(),
                labels: Vec::new(),
                points: vec![MetricPoint::new(0, 6.0)],
                exemplars: Vec::new()
            }]
        );
    }

    #[tokio::test]
    async fn scalar_filters_before_metrics_and_ordered_second_stages_compose() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(1, 1, None, "api", 1),
                sp_at(1, 2, Some(1), "api", 2),
                sp_at(1, 3, Some(1), "db", 3),
            ],
        );
        store.push_trace("t", "b", "root", vec![sp_at(2, 1, None, "worker", 4)]);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        let expected = |labels, count| {
            vec![TraceMetricSeries {
                labels,
                label_types: BTreeMap::new(),
                points: vec![MetricPoint::new(0, count)],
                exemplars: Vec::new(),
            }]
        };
        for case in [
            QueryMetricsCase {
                query: "{} | count() > 1 | count_over_time()",
                expected: expected(Vec::new(), 3.0),
            },
            QueryMetricsCase {
                query: "{} | by(span.svc) | count() > 1 | count_over_time()",
                expected: expected(Vec::new(), 2.0),
            },
            QueryMetricsCase {
                query: "{} | count() > 3 | count_over_time()",
                expected: expected(Vec::new(), 0.0),
            },
            QueryMetricsCase {
                query: "{} | count() > 1 | count_over_time() by(span.svc) | topk(2) | bottomk(1)",
                expected: expected(vec![MetricLabel::new("span.svc", "db")], 1.0),
            },
            QueryMetricsCase {
                query: "{} | count_over_time() by(span.svc) > 0 < 2 | topk(1)",
                expected: expected(vec![MetricLabel::new("span.svc", "db")], 1.0),
            },
            QueryMetricsCase {
                query: "{} | count_over_time() by(span.svc) | topk(1) > 2",
                expected: Vec::new(),
            },
        ] {
            assert!(
                engine
                    .query_range("t", case.query, 0, 9, 10)
                    .await
                    .unwrap()
                    .series
                    == case.expected,
                "{}",
                case.query
            );
        }
        for query in [
            "{} | count() + sum(duration) > 10",
            "{} | count() > sum(duration)",
        ] {
            assert!(
                engine
                    .query_range("t", &format!("{query} | count_over_time()"), 0, 9, 10)
                    .await
                    .is_err(),
                "pinned engine rejects aggregate-result expressions: {query}"
            );
        }
    }

    #[tokio::test]
    async fn rejected_spansets_keep_only_initialized_ungrouped_count_or_rate_series() {
        let engine = one_trace_engine(vec![sp_at(1, 1, None, "api", 1)]);
        let zero = vec![TraceMetricSeries {
            labels: Vec::new(),
            label_types: BTreeMap::new(),
            points: vec![MetricPoint::new(0, 0.0), MetricPoint::new(10, 0.0)],
            exemplars: Vec::new(),
        }];
        for case in [
            InitializedMetricCase {
                tenant: "t",
                query: "{} | count() > 1 | count_over_time()",
                initialized: true,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | by(name) | count() > 1 | rate()",
                initialized: true,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | count() > 1 | count_over_time() by(name)",
                initialized: false,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | count() > 1 | sum_over_time(duration)",
                initialized: false,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | count() > 1 | avg_over_time(duration)",
                initialized: false,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | count() > 1 | count_over_time() > 0",
                initialized: false,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{ span.svc = \"missing\" } | count() > 1 | count_over_time()",
                initialized: false,
            },
            InitializedMetricCase {
                tenant: "other",
                query: "{} | count() > 1 | count_over_time()",
                initialized: false,
            },
        ] {
            let query = format!("{} with(exemplars=false)", case.query);
            let actual = engine
                .query_range(case.tenant, &query, 0, 19, 10)
                .await
                .unwrap();
            assert!(
                actual.series
                    == if case.initialized {
                        zero.clone()
                    } else {
                        Vec::new()
                    },
                "tenant={} query={query}",
                case.tenant
            );
        }
    }

    #[tokio::test]
    async fn fractional_sampling_runs_before_selector_and_scaling_before_metric_filter() {
        let engine = one_trace_engine(vec![
            sp_at(1, 1, None, "api", 1),
            sp_at(1, 2, None, "api", 2),
            sp_at(1, 3, None, "db", 3),
            sp_at(1, 4, None, "worker", 4),
        ]);
        let response = engine
            .query_range(
                "t",
                "{ span.svc = \"db\" } | count_over_time() > 1 with(sample=0.5)",
                0,
                9,
                10,
            )
            .await
            .unwrap();
        assert!(
            response.series
                == vec![TraceMetricSeries {
                    labels: Vec::new(),
                    label_types: BTreeMap::new(),
                    points: vec![MetricPoint::new(0, 2.0)],
                    exemplars: Vec::new()
                }]
        );
        let exact = engine.query_range("t", "{ span.svc = \"db\" } | count_over_time() > 1 with(sample=1.0, unused=\"accepted\")", 0, 9, 10).await.unwrap();
        assert!(exact.series.is_empty());
    }

    #[tokio::test]
    async fn sampled_averages_keep_constant_values_while_additive_metrics_scale() {
        let mut store = InMemorySpanStore::new();
        let spans = (1..=4)
            .map(|id| {
                let mut span = sp_at(1, id, None, "api", i64::from(id));
                span.attrs
                    .push(Attribute::new("constant", AttrValue::Int(7)));
                span
            })
            .collect();
        store.push_trace("t", "a", "root", spans);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            MetricExpectedCase {
                metric: "avg_over_time(span.constant)",
                expected: 7.0,
            },
            MetricExpectedCase {
                metric: "avg_over_time(span.constant) < 8",
                expected: 7.0,
            },
            MetricExpectedCase {
                metric: "sum_over_time(span.constant)",
                expected: 28.0,
            },
            MetricExpectedCase {
                metric: "count_over_time()",
                expected: 4.0,
            },
            MetricExpectedCase {
                metric: "rate()",
                expected: 400_000_000.0,
            },
        ] {
            let query = format!("{{}} | {} with(sample=0.5, exemplars=false)", case.metric);
            let actual = engine.query_range("t", &query, 0, 9, 10).await.unwrap();
            assert!(
                actual.series
                    == vec![TraceMetricSeries {
                        labels: Vec::new(),
                        label_types: BTreeMap::new(),
                        points: vec![MetricPoint::new(0, case.expected)],
                        exemplars: Vec::new(),
                    }],
                "{query}"
            );
        }
        let rejected = engine
            .query_range(
                "t",
                "{} | avg_over_time(span.constant) > 8 with(sample=0.5, exemplars=false)",
                0,
                9,
                10,
            )
            .await
            .unwrap();
        assert!(rejected.series.is_empty());
    }

    #[tokio::test]
    async fn tempo_instant_metrics_pool_both_endpoints_before_rates_and_filters() {
        let mut store = InMemorySpanStore::new();
        store.push_trace(
            "t",
            "svc",
            "root",
            [-1, 0, 1, 2, 3]
                .into_iter()
                .enumerate()
                .map(|indexed_start| {
                    let index = indexed_start.0;
                    let start = indexed_start.1;
                    sp_at(1, u8::try_from(index + 1).unwrap(), None, "api", start)
                })
                .collect(),
        );
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        let expected = |value| {
            vec![TraceMetricSeries {
                labels: Vec::new(),
                label_types: BTreeMap::new(),
                points: vec![MetricPoint::new(2, value)],
                exemplars: Vec::new(),
            }]
        };
        for case in [
            QuerySeriesCase {
                query: "{} | count_over_time()",
                wanted: expected(3.0),
            },
            QuerySeriesCase {
                query: "{} | rate()",
                wanted: expected(1_500_000_000.0),
            },
            QuerySeriesCase {
                query: "{} | rate() > 1200000000",
                wanted: expected(1_500_000_000.0),
            },
            QuerySeriesCase {
                query: "{} | rate() > 1600000000",
                wanted: Vec::new(),
            },
        ] {
            let response = engine
                .query_range_with_options(
                    "t",
                    &format!("{} with(exemplars=false)", case.query),
                    0,
                    2,
                    2,
                    ScanOptions {
                        tempo_instant_metrics: true,
                        ..ScanOptions::default()
                    },
                )
                .await
                .unwrap();
            assert!(response.series == case.wanted, "{}", case.query);
        }
        let range = engine
            .query_range("t", "{} | count_over_time() with(exemplars=false)", 0, 2, 2)
            .await
            .unwrap();
        assert!(
            range.series
                == vec![TraceMetricSeries {
                    labels: Vec::new(),
                    label_types: BTreeMap::new(),
                    points: vec![MetricPoint::new(0, 2.0), MetricPoint::new(2, 1.0)],
                    exemplars: Vec::new(),
                }]
        );
        assert!(
            engine
                .query_range_with_options(
                    "t",
                    "{} | rate()",
                    0,
                    0,
                    0,
                    ScanOptions {
                        tempo_instant_metrics: true,
                        ..ScanOptions::default()
                    }
                )
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn empty_instant_count_and_rate_keep_only_initialized_ungrouped_zeroes() {
        let mut store = InMemorySpanStore::new();
        store.push_trace("t", "svc", "root", vec![sp_at(1, 1, None, "api", 1)]);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        let zero = vec![TraceMetricSeries {
            labels: Vec::new(),
            label_types: BTreeMap::new(),
            points: vec![MetricPoint::new(19, 0.0)],
            exemplars: Vec::new(),
        }];
        for InitializedMetricCase {
            tenant,
            query,
            initialized,
        } in [
            InitializedMetricCase {
                tenant: "t",
                query: "{} | count_over_time()",
                initialized: true,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | rate()",
                initialized: true,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{ span.svc = \"missing\" } | count_over_time()",
                initialized: true,
            },
            InitializedMetricCase {
                tenant: "other",
                query: "{} | count_over_time()",
                initialized: true,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | count_over_time() > 0",
                initialized: false,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | rate() > 0",
                initialized: false,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | count_over_time() by(span.svc)",
                initialized: false,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | rate() by(span.svc)",
                initialized: false,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | sum_over_time(duration)",
                initialized: false,
            },
            InitializedMetricCase {
                tenant: "t",
                query: "{} | avg_over_time(duration)",
                initialized: false,
            },
        ] {
            let response = engine
                .query_range_with_options(
                    tenant,
                    &format!("{query} with(exemplars=false)"),
                    10,
                    19,
                    9,
                    ScanOptions {
                        tempo_instant_metrics: true,
                        ..ScanOptions::default()
                    },
                )
                .await
                .unwrap();
            assert!(
                response.series
                    == if initialized {
                        zero.clone()
                    } else {
                        Vec::new()
                    },
                "{tenant}/{query}"
            );
        }
        let empty_engine =
            TraceqlEngine::new(Arc::new(InMemorySpanStore::new()), EngineOpts::default());
        let empty = empty_engine
            .query_range_with_options(
                "t",
                "{} | count_over_time() with(exemplars=false)",
                10,
                19,
                9,
                ScanOptions {
                    tempo_instant_metrics: true,
                    ..ScanOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(empty.series == zero);
        let range = engine
            .query_range(
                "t",
                "{} | count_over_time() with(exemplars=false)",
                10,
                19,
                9,
            )
            .await
            .unwrap();
        assert!(range.series.is_empty());
    }

    #[tokio::test]
    async fn public_metric_array_decoding_pools_observations_before_reduction_and_stages() {
        let mut store = InMemorySpanStore::new();
        struct MetricSeriesPoints {
            label: String,
            points: Vec<MetricPoint>,
        }
        // Three observations in one array group, one in another, and one
        // scalar decoy distinguish a weighted mean from averaging group means.
        struct GroupValueCase {
            group: i64,
            value: i64,
        }
        let spans = [
            GroupValueCase { group: 1, value: 2 },
            GroupValueCase { group: 1, value: 4 },
            GroupValueCase { group: 1, value: 8 },
            GroupValueCase {
                group: 3,
                value: 32,
            },
            GroupValueCase {
                group: 17,
                value: 16,
            },
        ]
        .into_iter()
        .enumerate()
        .map(|indexed_case| {
            let index = indexed_case.0;
            let case = indexed_case.1;
            let GroupValueCase { group, value } = case;
            let mut span = sp_at(1, u8::try_from(index + 1).unwrap(), None, "api", 1);
            span.attrs.extend([
                Attribute::new(
                    "group",
                    if group == 17 {
                        AttrValue::Int(group)
                    } else {
                        AttrValue::Array(vec![AttrValue::Int(group), AttrValue::Int(group + 1)])
                    },
                ),
                Attribute::new("value", AttrValue::Int(value)),
            ]);
            span
        })
        .collect();
        store.push_trace("t", "api", "root", spans);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        for case in [
            MetricExpectedCase {
                metric: "count_over_time()",
                expected: 4.0,
            },
            MetricExpectedCase {
                metric: "rate()",
                expected: 400_000_000.0,
            },
            MetricExpectedCase {
                metric: "sum_over_time(span.value)",
                expected: 46.0,
            },
            MetricExpectedCase {
                metric: "avg_over_time(span.value)",
                expected: 11.5,
            },
            MetricExpectedCase {
                metric: "min_over_time(span.value)",
                expected: 2.0,
            },
            MetricExpectedCase {
                metric: "max_over_time(span.value)",
                expected: 32.0,
            },
            MetricExpectedCase {
                metric: "quantile_over_time(span.value, 0.5)",
                expected: 4.0,
            },
        ] {
            let query = format!(
                "{{ span.group != nil }} | {} by(span.group) with(exemplars=false)",
                case.metric
            );
            let raw = engine.query_range("t", &query, 0, 9, 10).await.unwrap();
            assert!(raw.series.len() == 3, "{query}");
            assert!(
                raw.series
                    .iter()
                    .filter(|series| series.label_types.get("span.group")
                        == Some(&TraceMetricLabelType::Array))
                    .count()
                    == 2
            );
            let public = engine
                .query_range_with_options(
                    "t",
                    &query,
                    0,
                    9,
                    10,
                    ScanOptions {
                        tempo_frontend_labels: true,
                        ..ScanOptions::default()
                    },
                )
                .await
                .unwrap();
            assert!(public.series.len() == 2, "{query}");
            let nil = public
                .series
                .iter()
                .find(|series| {
                    series.label_types.get("span.group") == Some(&TraceMetricLabelType::Nil)
                })
                .unwrap();
            assert!(nil.labels.contains(&MetricLabel::new("span.group", "nil")));
            assert!(
                nil.points == vec![MetricPoint::new(0, case.expected)],
                "{query}"
            );
            assert!(nil.exemplars.is_empty());
        }
        for case in [
            MetricTypedExpectedCase {
                metric: "avg_over_time(span.value) by(span.group) | topk(1)",
                expected_kind: TraceMetricLabelType::Int,
                expected_value: 16.0,
            },
            MetricTypedExpectedCase {
                metric: "avg_over_time(span.value) by(span.group) > 12",
                expected_kind: TraceMetricLabelType::Int,
                expected_value: 16.0,
            },
            MetricTypedExpectedCase {
                metric: "count_over_time() by(span.group) > 3",
                expected_kind: TraceMetricLabelType::Nil,
                expected_value: 4.0,
            },
        ] {
            let query = format!("{{}} | {} with(exemplars=false)", case.metric);
            let public = engine
                .query_range_with_options(
                    "t",
                    &query,
                    0,
                    9,
                    10,
                    ScanOptions {
                        tempo_frontend_labels: true,
                        ..ScanOptions::default()
                    },
                )
                .await
                .unwrap();
            assert!(public.series.len() == 1, "{query}");
            assert!(public.series[0].label_types.get("span.group") == Some(&case.expected_kind));
            assert!(
                public.series[0].points == vec![MetricPoint::new(0, case.expected_value)],
                "{query}"
            );
        }
        let histogram = engine
            .query_range_with_options(
                "t",
                "{} | histogram_over_time(span.value) by(span.group) with(exemplars=false)",
                0,
                9,
                10,
                ScanOptions {
                    tempo_frontend_labels: true,
                    ..ScanOptions::default()
                },
            )
            .await
            .unwrap();
        let mut array_buckets = histogram
            .series
            .iter()
            .filter(|series| {
                series.label_types.get("span.group") == Some(&TraceMetricLabelType::Nil)
            })
            .map(|series| MetricSeriesPoints {
                label: series.labels[0].value.clone(),
                points: series.points.clone(),
            })
            .collect::<Vec<_>>();
        array_buckets.sort_by(|left, right| left.label.cmp(&right.label));
        assert!(
            array_buckets
                .iter()
                .map(|series| series.label.as_str())
                .collect::<Vec<_>>()
                == vec!["2", "32", "4", "8"]
        );
        assert!(
            array_buckets
                .iter()
                .all(|series| { series.points == vec![MetricPoint::new(0, 1.0)] })
        );
    }

    #[tokio::test]
    async fn frontend_decoded_nil_remains_distinct_from_a_real_nil_string() {
        let mut store = InMemorySpanStore::new();
        let spans = [
            Some(AttrValue::Array(vec![AttrValue::Int(1), AttrValue::Int(2)])),
            Some(AttrValue::Array(vec![AttrValue::Int(3), AttrValue::Int(4)])),
            Some(AttrValue::Str("nil".into())),
            None,
        ]
        .into_iter()
        .enumerate()
        .map(|indexed_value| {
            let index = indexed_value.0;
            let value = indexed_value.1;
            let mut span = sp_at(1, u8::try_from(index + 1).unwrap(), None, "api", 1);
            if let Some(value) = value {
                span.attrs.push(Attribute::new("group", value));
            }
            span
        })
        .collect();
        store.push_trace("t", "api", "root", spans);
        let engine = TraceqlEngine::new(Arc::new(store), EngineOpts::default());
        let response = engine
            .query_range_with_options(
                "t",
                "{} | count_over_time() by(span.group) with(exemplars=false)",
                0,
                9,
                10,
                ScanOptions {
                    tempo_frontend_labels: true,
                    ..ScanOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(response.series.len() == 2);
        for series in &response.series {
            assert!(series.labels == vec![MetricLabel::new("span.group", "nil")]);
            assert!(series.points == vec![MetricPoint::new(0, 2.0)]);
        }
        assert!(
            response
                .series
                .iter()
                .filter(|series| series.label_types.get("span.group")
                    == Some(&TraceMetricLabelType::Nil))
                .count()
                == 1
        );
        assert!(
            response
                .series
                .iter()
                .filter(|series| series.label_types.is_empty())
                .count()
                == 1
        );
    }

    #[tokio::test]
    async fn frontend_exemplar_decoding_preserves_selected_span_and_scalar_labels() {
        let mut store = InMemorySpanStore::new();
        let mut span = sp_at(1, 2, None, "api", 3);
        span.attrs.push(Attribute::new(
            "group",
            AttrValue::Array(vec![AttrValue::Int(1), AttrValue::Int(2)]),
        ));
        store.push_trace("t", "api", "root", vec![span]);
        let engine = TraceqlEngine::new(
            Arc::new(store),
            EngineOpts {
                max_exemplars: 1,
                ..EngineOpts::default()
            },
        );
        let query = "{ span.group > 0 && span.svc = \"api\" } | count_over_time() by(span.group)";
        let raw = engine.query_range("t", query, 0, 9, 10).await.unwrap();
        assert!(raw.series[0].exemplars.len() == 1);
        assert!(
            raw.series[0].exemplars[0].label_types.get("span.group")
                == Some(&TraceMetricLabelType::Array)
        );
        let public = engine
            .query_range_with_options(
                "t",
                query,
                0,
                9,
                10,
                ScanOptions {
                    tempo_frontend_labels: true,
                    ..ScanOptions::default()
                },
            )
            .await
            .unwrap();
        assert!(public.series.len() == 1);
        assert!(public.series[0].exemplars.len() == 1);
        let exemplar = &public.series[0].exemplars[0];
        assert!(exemplar.timestamp_ns == 3);
        assert!(exemplar.value.is_nan());
        assert!(
            exemplar.label_types
                == BTreeMap::from([("span.group".into(), TraceMetricLabelType::Nil)])
        );
        assert!(
            exemplar.labels
                == vec![
                    MetricLabel::new("trace:id", "1010101010101010101010101010101"),
                    MetricLabel::new("span.group", "nil"),
                    MetricLabel::new("span.svc", "api"),
                ]
        );
    }

    #[tokio::test]
    async fn count_over_time_carries_trace_id_exemplars() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(0x11, 0x22, None, "api", 0),
                sp_at(0x11, 0x33, None, "api", 10_000),
            ],
        );
        let e = TraceqlEngine::new(
            Arc::new(s),
            EngineOpts {
                max_exemplars: 1,
                ..EngineOpts::default()
            },
        );
        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time()",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        check!(got.series.len() == 1);
        assert!(got.series[0].exemplars.len() == 1);
        let exemplar = &got.series[0].exemplars[0];
        assert!(
            exemplar.labels
                == vec![
                    MetricLabel::new("trace:id", "11111111111111111111111111111111"),
                    MetricLabel::new(".svc", "api"),
                ]
        );
        assert!(exemplar.label_types.is_empty());
        assert!([0, 10_000].contains(&exemplar.timestamp_ns));
        assert!(exemplar.value.is_nan());
    }

    #[tokio::test]
    async fn metric_comparison_filter_removes_exemplars_for_filtered_samples() {
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "a",
            "root",
            vec![
                sp_at(0x11, 0x22, None, "api", 0),
                sp_at(0x11, 0x33, None, "api", 10_000),
                sp_at(0x44, 0x55, None, "api", 60_000),
            ],
        );
        let e = TraceqlEngine::new(
            Arc::new(s),
            EngineOpts {
                max_exemplars: 10,
                ..EngineOpts::default()
            },
        );
        let got = e
            .query_range(
                "t",
                "{ .svc != nil } | count_over_time() | by(span.svc) > 1",
                0,
                120_000,
                60_000,
            )
            .await
            .unwrap();

        check!(got.series.len() == 1);
        check!(got.series[0].labels == vec![MetricLabel::new("span.svc", "api")]);
        check!(got.series[0].points == vec![MetricPoint::new(0, 2.0)]);
        check!(got.series[0].exemplars.is_empty());
    }

    #[tokio::test]
    async fn default_options_disable_traceql_metric_exemplars() {
        let e = one_trace_engine(vec![sp_at(0x11, 0x22, None, "api", 0)]);

        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time()",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        assert!(got.series.len() == 1);
        assert!(got.series[0].exemplars.is_empty());
    }

    #[tokio::test]
    async fn query_hint_can_disable_traceql_metric_exemplars() {
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "a", "root", vec![sp_at(0x11, 0x22, None, "api", 0)]);
        let e = TraceqlEngine::new(
            Arc::new(s),
            EngineOpts {
                max_exemplars: 1,
                ..EngineOpts::default()
            },
        );

        let got = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time() with (exemplars=false)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        assert!(got.series.len() == 1);
        assert!(got.series[0].exemplars.is_empty());
    }

    #[test]
    fn f64_from_i64_matches_decimal_string_conversion() {
        // The direct conversion must be numerically identical to the previous
        // `to_string().parse()` path for representative i64 values, including a
        // large magnitude where float rounding matters.
        for value in [
            0_i64,
            1,
            -1,
            42,
            i64::MAX,
            i64::MIN,
            9_007_199_254_740_993, // 2^53 + 1, not exactly representable in f64
        ] {
            let direct = f64_from_i64(value);
            let via_string: f64 = value.to_string().parse().unwrap();
            assert!(direct.to_bits() == via_string.to_bits());
        }
    }

    fn sp_with_code(id: u8, start: i64, code: Option<i64>) -> InputSpan {
        let mut attrs = vec![Attribute::new("svc", AttrValue::Str("api".into()))];
        if let Some(code) = code {
            attrs.push(Attribute::new("code", AttrValue::Int(code)));
        }
        InputSpan {
            attrs,
            ..sp_at(1, id, None, "api", start)
        }
    }

    #[tokio::test]
    async fn absent_metric_attribute_does_not_pollute_min_and_avg() {
        // Spans whose target attribute is ABSENT must not contribute a 0 to
        // min/avg/max over the value field.
        let e = one_trace_engine(vec![
            sp_with_code(1, 0, Some(10)),
            sp_with_code(2, 10_000, Some(30)),
            sp_with_code(3, 20_000, None),
        ]);

        let min = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | min_over_time(.code)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        // min over the present values {10, 30} = 10, not dragged to 0.
        assert!(min.series[0].points == vec![MetricPoint::new(0, 10.0)]);

        let avg = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | avg_over_time(.code)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        // avg over {10, 30} = 20, not (10+30+0)/3 = 13.33.
        assert!(avg.series[0].points == vec![MetricPoint::new(0, 20.0)]);

        let max = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | max_over_time(.code)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(max.series[0].points == vec![MetricPoint::new(0, 30.0)]);
    }

    #[tokio::test]
    async fn search_response_exposes_exact_span_scalars_and_attrs() {
        // A single trace with two spans carrying distinct, non-uniform scalar
        // values so that trivial replacements (None / Some([0;8]) / Some([1;8]) /
        // i32 0/1/-1) and the nanosecond trace duration are all observable.
        let root = InputSpan {
            trace_id: [9; 16],
            span_id: [10; 8],
            parent_span_id: None,
            name: "root-op".into(),
            kind: 2,
            start_unix_nano: 0,
            duration: nanos(5_000_000),
            status_code: 0,
            status_message: String::new(),
            instrumentation_name: "tracer".into(),
            instrumentation_version: String::new(),
            attrs: vec![Attribute::new("n", AttrValue::Int(42))],
            events: Vec::new(),
            links: Vec::new(),
        };
        let child = InputSpan {
            span_id: [20; 8],
            parent_span_id: Some([10; 8]),
            kind: 3,
            attrs: vec![Attribute::new("svc", AttrValue::Str("api".into()))],
            ..root.clone()
        };
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "checkout", "root-op", vec![root, child]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let r = search_all(&e, "{ span:kind != nil }").await;
        assert!(r.traces.len() == 1);
        let trace = &r.traces[0];
        assert!(trace.duration == millis(5));

        let spans = &trace.span_sets[0].spans;
        assert!(spans.len() == 2);
        let root_span = spans.iter().find(|s| s.span_id == [10; 8]).unwrap();
        let child_span = spans.iter().find(|s| s.span_id == [20; 8]).unwrap();

        // optional_fixed_8: root has no parent (None), child's parent is [10;8],
        // which is neither [0;8] nor [1;8].
        check!(root_span.parent_span_id.is_none());
        check!(child_span.parent_span_id == Some([10; 8]));

        // i32_value: kind is 2 / 3, not 0, 1, or -1.
        check!(root_span.kind == 2);
        check!(child_span.kind == 3);

        // row_attrs: the int attribute is carried through with its exact value
        // (kills `row_attrs -> Ok(vec![])`).
        let n_attr = root_span
            .attributes
            .iter()
            .find(|attribute| attribute.key == "n")
            .map(|attribute| attribute.value.clone());
        assert!(n_attr == Some(AttrValue::Int(42)));
        let svc_attr = child_span
            .attributes
            .iter()
            .find(|attribute| attribute.key == "svc")
            .map(|attribute| attribute.value.clone());
        assert!(svc_attr == Some(AttrValue::Str("api".into())));
    }

    // ---- block-format nested attribute columns (List<List<T>>) ----

    /// Builds `attr_keys` and the four typed value columns for a single row.
    ///
    /// `attr_keys` is a `List<Utf8>`, and each typed value column is a
    /// `List<List<T>>`. The row carries four attributes: a string `s`, an int
    /// `i`, a float `f`, and a bool `b`. This function fills each attribute
    /// only in its own typed column, and leaves the other inner lists empty.
    fn block_attr_batch() -> RecordBatch {
        use arrow::array::{
            BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
        };

        // attr_keys: [["s", "i", "f", "b"]].
        let mut keys = ListBuilder::new(StringBuilder::new());
        for k in ["s", "i", "f", "b"] {
            keys.values().append_value(k);
        }
        keys.append(true);

        // attr_value (string): [[["hello"], [], [], []]].
        let mut str_values = ListBuilder::new(ListBuilder::new(StringBuilder::new()));
        str_values.values().values().append_value("hello");
        str_values.values().append(true); // s -> ["hello"]
        str_values.values().append(true); // i -> []
        str_values.values().append(true); // f -> []
        str_values.values().append(true); // b -> []
        str_values.append(true);

        // attr_value_int: [[[], [42], [], []]].
        let mut int_values = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
        int_values.values().append(true); // s -> []
        int_values.values().values().append_value(42);
        int_values.values().append(true); // i -> [42]
        int_values.values().append(true); // f -> []
        int_values.values().append(true); // b -> []
        int_values.append(true);

        // attr_value_double: [[[], [], [3.5], []]].
        let mut double_values = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
        double_values.values().append(true); // s -> []
        double_values.values().append(true); // i -> []
        double_values.values().values().append_value(3.5);
        double_values.values().append(true); // f -> [3.5]
        double_values.values().append(true); // b -> []
        double_values.append(true);

        // attr_value_bool: [[[], [], [], [true]]].
        let mut bool_values = ListBuilder::new(ListBuilder::new(BooleanBuilder::new()));
        bool_values.values().append(true); // s -> []
        bool_values.values().append(true); // i -> []
        bool_values.values().append(true); // f -> []
        bool_values.values().values().append_value(true);
        bool_values.values().append(true); // b -> [true]
        bool_values.append(true);

        let keys = keys.finish();
        let str_values = str_values.finish();
        let int_values = int_values.finish();
        let double_values = double_values.finish();
        let bool_values = bool_values.finish();

        let schema = Arc::new(Schema::new(block_attr_fields([
            &keys,
            &str_values,
            &int_values,
            &double_values,
            &bool_values,
        ])));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(keys) as ArrayRef,
                Arc::new(str_values),
                Arc::new(int_values),
                Arc::new(double_values),
                Arc::new(bool_values),
            ],
        )
        .unwrap()
    }

    struct BlockAttrValueColumns<'a> {
        string_values: Option<&'a ListArray>,
        int_values: Option<&'a ListArray>,
        double_values: Option<&'a ListArray>,
        bool_values: Option<&'a ListArray>,
    }

    fn block_attr_value_columns(batch: &RecordBatch) -> BlockAttrValueColumns<'_> {
        BlockAttrValueColumns {
            string_values: optional_list_column(batch, BLOCK_ATTR_VALUE).unwrap(),
            int_values: optional_list_column(batch, BLOCK_ATTR_VALUE_INT).unwrap(),
            double_values: optional_list_column(batch, BLOCK_ATTR_VALUE_DOUBLE).unwrap(),
            bool_values: optional_list_column(batch, BLOCK_ATTR_VALUE_BOOL).unwrap(),
        }
    }

    #[test]
    fn block_row_attrs_decodes_every_typed_value_column() {
        // Drives block_row_attrs / block_attr_values_for_key / the typed
        // *_attr_values readers / row_attr_values / optional_list_column end to
        // end. Trivial `vec![]`/`None` replacements anywhere on this path drop a
        // value and fail the equality below.
        let batch = block_attr_batch();
        let mut attrs = block_row_attrs(&batch, 0).unwrap();
        attrs.sort_by(|a, b| a.key.cmp(&b.key));
        assert!(
            attrs
                == vec![
                    Attribute::new("b".to_string(), AttrValue::Bool(true)),
                    Attribute::new("f", AttrValue::Float(3.5)),
                    Attribute::new("i", AttrValue::Int(42)),
                    Attribute::new("s", AttrValue::Str("hello".into())),
                ]
        );
    }

    #[test]
    fn block_attr_values_for_key_picks_the_populated_type_per_index() {
        // Each attr_idx has exactly one populated typed column. The `if
        // !values.is_empty()` guards select that column; removing a `!` would
        // skip the populated column and return the wrong (empty/fallthrough)
        // type.
        let batch = block_attr_batch();
        let columns = block_attr_value_columns(&batch);

        // optional_list_column must actually find the columns (kills `-> Ok(None)`).
        check!(columns.string_values.is_some());
        check!(columns.int_values.is_some());
        check!(columns.double_values.is_some());
        check!(columns.bool_values.is_some());

        let for_idx = |idx| {
            block_attr_values_for_key(
                columns.string_values,
                columns.int_values,
                columns.double_values,
                columns.bool_values,
                0,
                idx,
            )
            .unwrap()
        };
        for BlockAttrValueCase { index, expected } in [
            BlockAttrValueCase {
                index: 0,
                expected: vec![AttrValue::Str("hello".into())],
            },
            BlockAttrValueCase {
                index: 1,
                expected: vec![AttrValue::Int(42)],
            },
            BlockAttrValueCase {
                index: 2,
                expected: vec![AttrValue::Float(3.5)],
            },
            BlockAttrValueCase {
                index: 3,
                expected: vec![AttrValue::Bool(true)],
            },
        ] {
            check!(for_idx(index) == expected, "attr_idx {index}");
        }
    }

    #[test]
    fn typed_block_attr_readers_return_exact_values() {
        // Directly exercise each typed reader so trivial returns
        // (vec![] / vec![0] / vec![1] / vec![-1] / vec!["xyzzy"] / etc.) and the
        // `!values.is_null` filter are all observable.
        let batch = block_attr_batch();
        let columns = block_attr_value_columns(&batch);

        check!(
            string_attr_values(columns.string_values, 0, 0, BLOCK_ATTR_VALUE).unwrap()
                == vec!["hello".to_string()]
        );
        check!(
            i64_attr_values(columns.int_values, 0, 1, BLOCK_ATTR_VALUE_INT).unwrap() == vec![42]
        );
        check!(
            f64_attr_values(columns.double_values, 0, 2, BLOCK_ATTR_VALUE_DOUBLE).unwrap()
                == vec![3.5]
        );
        check!(
            bool_attr_values(columns.bool_values, 0, 3, BLOCK_ATTR_VALUE_BOOL).unwrap()
                == vec![true]
        );

        // An index whose inner list is empty yields an empty vec (not a trivial
        // non-empty replacement).
        check!(
            string_attr_values(columns.string_values, 0, 1, BLOCK_ATTR_VALUE)
                .unwrap()
                .is_empty()
        );
        check!(
            i64_attr_values(columns.int_values, 0, 0, BLOCK_ATTR_VALUE_INT)
                .unwrap()
                .is_empty()
        );
        check!(
            f64_attr_values(columns.double_values, 0, 0, BLOCK_ATTR_VALUE_DOUBLE)
                .unwrap()
                .is_empty()
        );
        check!(
            bool_attr_values(columns.bool_values, 0, 0, BLOCK_ATTR_VALUE_BOOL)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn row_attr_values_bounds_check_returns_none_out_of_range() {
        // `attr_idx >= row_values.len()` must short-circuit to Ok(None):
        //  * `>= -> <` would reject in-range indices instead.
        //  * `|| -> &&` would stop short-circuiting and index out of bounds.
        let batch = block_attr_batch();
        let str_values = optional_list_column(&batch, BLOCK_ATTR_VALUE).unwrap();
        // In range (idx 0) returns Some.
        assert!(
            row_attr_values(str_values, 0, 0, BLOCK_ATTR_VALUE)
                .unwrap()
                .is_some()
        );
        // Out of range (only 4 inner lists exist) returns None without panicking.
        assert!(
            row_attr_values(str_values, 0, 99, BLOCK_ATTR_VALUE)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn metric_pipeline_parts_rejects_duplicate_stages() {
        use crate::ast::{Aggregate, Pipeline};

        let by = vec![field(Scope::Span, "svc")];

        // A single aggregate is accepted.
        assert!(
            metric_pipeline_parts(&[Pipeline::Aggregate(Aggregate::CountOverTime)])
                .unwrap()
                .is_some()
        );

        // Duplicate aggregate / by / compare stages must abort the
        // parse (Ok(None)); each match guard `<slot>.is_none()` (and `!compare`)
        // is what enforces that. Replacing a guard with `true` would accept the
        // duplicate.
        let compare_stage = || Pipeline::Compare {
            selection: Box::new(SpansetExpr::Selector(Box::new(FieldExpr::Const(true)))),
            top_n: 10,
            start: None,
            end: None,
        };
        for MetricPipelineCase { name, stages } in [
            MetricPipelineCase {
                name: "aggregate",
                stages: vec![
                    Pipeline::Aggregate(Aggregate::CountOverTime),
                    Pipeline::Aggregate(Aggregate::Rate),
                ],
            },
            MetricPipelineCase {
                name: "by",
                stages: vec![
                    Pipeline::Aggregate(Aggregate::CountOverTime),
                    Pipeline::By(by.clone()),
                    Pipeline::By(by.clone()),
                ],
            },
            MetricPipelineCase {
                name: "compare",
                stages: vec![
                    Pipeline::Aggregate(Aggregate::CountOverTime),
                    compare_stage(),
                    compare_stage(),
                ],
            },
        ] {
            check!(
                metric_pipeline_parts(&stages).unwrap().is_none(),
                "duplicate {name} stage must abort the parse"
            );
        }
    }

    #[test]
    fn max_traces_returns_configured_cap() {
        // The accessor must return the configured cap (default 1000), not a
        // trivial 0 or 1.
        let e = engine();
        assert!(e.max_traces() == 1000);
        let custom = TraceqlEngine::new(
            Arc::new(InMemorySpanStore::new()),
            EngineOpts {
                max_traces: 7,
                ..EngineOpts::default()
            },
        );
        assert!(custom.max_traces() == 7);
    }

    fn field(scope: Scope, key: &str) -> Field {
        Field {
            scope,
            key: key.to_string(),
        }
    }

    #[test]
    fn metric_by_attribute_field_emits_projection_matcher() {
        // A metric by()/value field on a regular span or resource attribute must
        // produce a projection matcher so the store materializes its attr.<key>
        // column for GROUP BY (otherwise `rate() by(span.http.method)` 400s with
        // "missing column attr.http.method"). Projection-only, so it must not
        // filter. Parent/instrumentation/intrinsic stay None.
        let span = nested_projection_matcher(&field(Scope::Span, "http.method")).unwrap();
        assert!(span.scope == MatchScope::Span && span.key == "http.method");
        let res = nested_projection_matcher(&field(Scope::Resource, "service.version")).unwrap();
        assert!(res.scope == MatchScope::Resource && res.key == "service.version");
        let both = nested_projection_matcher(&field(Scope::Both, "team")).unwrap();
        assert!(both.scope == MatchScope::Both && both.key == "team");
        assert!(nested_projection_matcher(&field(Scope::Parent, "x")).is_none());
    }

    #[test]
    fn metric_field_column_maps_every_scope_to_its_column() {
        let cases = [
            // service.name short-circuits to the root-service column for
            // Both/Resource.
            MetricFieldColumnCase {
                scope: Scope::Both,
                key: "service.name",
                expected_column: COL_ROOT_SERVICE_NAME.to_string(),
            },
            MetricFieldColumnCase {
                scope: Scope::Resource,
                key: "service.name",
                expected_column: COL_ROOT_SERVICE_NAME.to_string(),
            },
            // Generic attribute scopes prefix the key.
            MetricFieldColumnCase {
                scope: Scope::Span,
                key: "http.method",
                expected_column: "attr.http.method".to_string(),
            },
            MetricFieldColumnCase {
                scope: Scope::Event,
                key: "k",
                expected_column: format!("{ATTR_PREFIX}{EVENT_ATTR_PREFIX}k"),
            },
            // Scope::Link arm.
            MetricFieldColumnCase {
                scope: Scope::Link,
                key: "k",
                expected_column: format!("{ATTR_PREFIX}{LINK_ATTR_PREFIX}k"),
            },
            // Intrinsic arms each map to a distinct column (not the `_ => Err`).
            MetricFieldColumnCase {
                scope: Scope::Intrinsic(Intrinsic::Name),
                key: "x",
                expected_column: COL_NAME.to_string(),
            },
            MetricFieldColumnCase {
                scope: Scope::Intrinsic(Intrinsic::Id),
                key: "x",
                expected_column: COL_SPAN_ID.to_string(),
            },
            MetricFieldColumnCase {
                scope: Scope::Intrinsic(Intrinsic::ParentId),
                key: "x",
                expected_column: COL_PARENT_SPAN_ID.to_string(),
            },
            MetricFieldColumnCase {
                scope: Scope::Intrinsic(Intrinsic::NestedSetLeft),
                key: "x",
                expected_column: COL_NS_LEFT.to_string(),
            },
            MetricFieldColumnCase {
                scope: Scope::Intrinsic(Intrinsic::NestedSetRight),
                key: "x",
                expected_column: COL_NS_RIGHT.to_string(),
            },
            MetricFieldColumnCase {
                scope: Scope::Intrinsic(Intrinsic::TraceRootService),
                key: "x",
                expected_column: COL_ROOT_SERVICE_NAME.to_string(),
            },
            MetricFieldColumnCase {
                scope: Scope::Intrinsic(Intrinsic::TraceRootName),
                key: "x",
                expected_column: COL_ROOT_SPAN_NAME.to_string(),
            },
            MetricFieldColumnCase {
                scope: Scope::Intrinsic(Intrinsic::InstrumentationVersion),
                key: "x",
                expected_column: COL_INSTRUMENTATION_VERSION.to_string(),
            },
            MetricFieldColumnCase {
                scope: Scope::Intrinsic(Intrinsic::EventTimeSinceStart),
                key: "x",
                expected_column: COL_EVENT_TIME_SINCE_START.to_string(),
            },
        ];
        for MetricFieldColumnCase {
            scope,
            key,
            expected_column,
        } in cases
        {
            check!(
                metric_field_column(&field(scope.clone(), key)).unwrap() == expected_column,
                "scope {scope:?} key {key:?}"
            );
        }
    }

    /// `validate_compare_selection` decides whether a `compare()` selection can
    /// use the cheap per-row evaluator or has to go through the planner. It
    /// walks the whole expression, so a selection is only simple when every
    /// leaf in it is: one unsupported leaf anywhere disqualifies the lot.
    #[test]
    fn a_compare_selection_is_simple_only_if_every_leaf_is() {
        let selection = |query: &str| {
            crate::parser::parse(query)
                .unwrap_or_else(|e| panic!("query {query:?} did not parse: {e}"))
                .root
        };
        let simple = |query: &str| validate_compare_selection(&selection(query)).is_ok();

        check!(simple(r#"{ .svc = "a" }"#), "an attribute comparison");
        check!(simple("{ span:duration > 100ms }"), "a supported intrinsic");
        check!(simple("{ name = \"x\" }"), "another supported intrinsic");
        check!(
            simple(r#"{ .a = "x" } && { .b = "y" }"#),
            "both sides simple"
        );
        check!(
            simple(r#"{ .a = "x" } || { .b = "y" }"#),
            "either side simple"
        );

        // A structural operator is never simple, wherever it sits.
        check!(
            !simple(r#"{ .a = "x" } > { .b = "y" }"#),
            "a structural selection"
        );
        check!(
            !simple(r#"{ .a = "x" } && ({ .b = "y" } > { .c = "z" })"#),
            "a structural operand inside a conjunction"
        );

        check!(
            simple("{ span:id = \"abc\" }"),
            "a supported identifier intrinsic"
        );
        check!(
            !simple("{ parent.a = \"abc\" }"),
            "parent is unsupported upstream"
        );
        check!(
            !simple(r#"{ .a = "x" } && { parent.a = "abc" }"#),
            "one unsupported leaf is enough"
        );
        check!(
            !simple(r#"{ .a = "x" && !(parent.a = "abc") }"#),
            "negation is descended into"
        );
    }

    #[test]
    fn usize_from_integer_f64_validates_and_converts() {
        let cases = [
            // A valid non-negative integer float converts to the exact usize.
            IntegerFloatConversionCase {
                value: 3.0,
                expected: Some(3),
            },
            IntegerFloatConversionCase {
                value: 0.0,
                expected: Some(0),
            },
            // Boundary on the `< 0.0` check: exactly 0.0 is accepted, but any
            // negative is rejected (distinguishes `<` from `<=`/`==`).
            IntegerFloatConversionCase {
                value: -0.5,
                expected: None,
            },
            IntegerFloatConversionCase {
                value: -1.0,
                expected: None,
            },
            // A NaN, an infinity and a fractional value are all rejected by
            // the fractional clause -- `fract()` is NaN for both non-finites.
            IntegerFloatConversionCase {
                value: f64::NAN,
                expected: None,
            },
            IntegerFloatConversionCase {
                value: f64::INFINITY,
                expected: None,
            },
            IntegerFloatConversionCase {
                value: 2.5,
                expected: None,
            },
        ];
        for IntegerFloatConversionCase { value, expected } in cases {
            check!(
                usize_from_integer_f64(value).ok() == expected,
                "input {value}"
            );
        }

        // Rejecting is not enough: the guard has to be the thing that rejects.
        // Infinity is non-negative with a zero fractional part, so without the
        // finiteness check it reaches the parse and fails there instead, with
        // a message about digits rather than about the value.
        let err = usize_from_integer_f64(f64::INFINITY)
            .unwrap_err()
            .to_string();
        check!(
            err.contains("expected non-negative integer float, got inf"),
            "got: {err}"
        );
        let err = usize_from_integer_f64(f64::NAN).unwrap_err().to_string();
        check!(
            err.contains("expected non-negative integer float, got NaN"),
            "got: {err}"
        );

        // A value that is finite, non-negative and whole can still be too
        // large to hold. That path must report the failure rather than
        // saturate, which is what a plain cast would do.
        let too_big = 1e30_f64;
        let err = usize_from_integer_f64(too_big).unwrap_err().to_string();
        check!(
            err.contains("number too large to fit in target type"),
            "the conversion failure is reported as written, got: {err}"
        );
        check!(
            !err.contains("expected non-negative integer float"),
            "it passed the guard and failed the conversion, got: {err}"
        );
    }

    #[test]
    fn metric_filter_passes_covers_every_operator() {
        use crate::ast::ComparisonOp;
        let f = |op| MetricFilter { op, value: 5.0 };
        let cases = [
            // Eq / Neq. Neq is the negation of Eq — the `!` is load-bearing.
            MetricFilterCase {
                value: 5.0,
                operator: ComparisonOp::Eq,
                expected: true,
            },
            MetricFilterCase {
                value: 6.0,
                operator: ComparisonOp::Eq,
                expected: false,
            },
            MetricFilterCase {
                value: 6.0,
                operator: ComparisonOp::Neq,
                expected: true,
            },
            MetricFilterCase {
                value: 5.0,
                operator: ComparisonOp::Neq,
                expected: false,
            },
            // Lt / Lte. Lte is `!is_gt` — both sides matter.
            MetricFilterCase {
                value: 4.0,
                operator: ComparisonOp::Lt,
                expected: true,
            },
            MetricFilterCase {
                value: 5.0,
                operator: ComparisonOp::Lt,
                expected: false,
            },
            MetricFilterCase {
                value: 5.0,
                operator: ComparisonOp::Lte,
                expected: true,
            },
            MetricFilterCase {
                value: 6.0,
                operator: ComparisonOp::Lte,
                expected: false,
            },
            // Gt / Gte. Gte is `!is_lt` — both sides matter.
            MetricFilterCase {
                value: 6.0,
                operator: ComparisonOp::Gt,
                expected: true,
            },
            MetricFilterCase {
                value: 5.0,
                operator: ComparisonOp::Gt,
                expected: false,
            },
            MetricFilterCase {
                value: 5.0,
                operator: ComparisonOp::Gte,
                expected: true,
            },
            MetricFilterCase {
                value: 4.0,
                operator: ComparisonOp::Gte,
                expected: false,
            },
        ];
        for MetricFilterCase {
            value,
            operator,
            expected,
        } in cases
        {
            check!(
                metric_filter_passes(value, f(operator)) == expected,
                "value {value} op {operator:?}"
            );
        }
    }

    fn metric_start_batch(starts: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new(COL_TRACE_ID, DataType::FixedSizeBinary(16), false),
            ArrowField::new(COL_SPAN_ID, DataType::FixedSizeBinary(8), false),
            ArrowField::new(COL_START, DataType::Int64, false),
        ]));
        let mut trace_id = FixedSizeBinaryBuilder::with_capacity(starts.len(), 16);
        let mut span_id = FixedSizeBinaryBuilder::with_capacity(starts.len(), 8);
        for _ in starts {
            trace_id.append_value([1; 16]).unwrap();
            span_id.append_value([2; 8]).unwrap();
        }
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(trace_id.finish()) as ArrayRef,
                Arc::new(span_id.finish()),
                Arc::new(Int64Array::from(starts.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn assemble_metrics_response_allows_equal_start_and_end() {
        // end_ns == start_ns is a valid single-bucket range. The `end_ns < start_ns`
        // guard must NOT fire on equality (kills `< -> <=` and `< -> ==`).
        let batch = metric_start_batch(&[0]);
        let plan = count_over_time_plan();
        let resp = assemble_metrics_response(
            &[batch],
            UnixNano(0),
            UnixNano(0),
            DurationNanos(60_000),
            &plan,
            MetricExemplarPolicy::new(0, &EngineOpts::default().histogram_buckets),
            UnixNano(0),
        )
        .unwrap();
        assert!(resp.series.len() == 1);
        assert!(resp.series[0].points == vec![MetricPoint::new(0, 1.0)]);

        // end_ns < start_ns is rejected.
        let batch = metric_start_batch(&[0]);
        assert!(
            assemble_metrics_response(
                &[batch],
                UnixNano(10),
                UnixNano(0),
                DurationNanos(60_000),
                &plan,
                MetricExemplarPolicy::new(0, &EngineOpts::default().histogram_buckets),
                UnixNano(0),
            )
            .is_err()
        );
    }

    #[test]
    fn assemble_metrics_response_range_filter_is_inclusive_at_end_and_exclusive_below_start() {
        // Rows: one below start (-10), one at start (0), one at end (60_000), one
        // above end (120_001). With step 60_000 and range [0, 60_000] there are
        // two buckets. The in-range check is `ts < start || ts > end`:
        //  * `|| -> &&` would stop skipping the below-start row.
        //  * `> -> ==` / `> -> >=` would drop the row exactly at end_ns.
        let batch = metric_start_batch(&[-10, 0, 60_000, 120_001]);
        let plan = count_over_time_plan();
        let resp = assemble_metrics_response(
            &[batch],
            UnixNano(0),
            UnixNano(60_000),
            DurationNanos(60_000),
            &plan,
            MetricExemplarPolicy::new(0, &EngineOpts::default().histogram_buckets),
            UnixNano(0),
        )
        .unwrap();
        assert!(resp.series.len() == 1);
        // bucket 0 (ts 0) -> 1, bucket 1 (ts 60_000) -> 1. The out-of-range rows
        // (-10 below start, 120_001 above end) are excluded.
        assert!(
            resp.series[0].points == vec![MetricPoint::new(0, 1.0), MetricPoint::new(60_000, 1.0)]
        );
    }

    #[test]
    fn assemble_metrics_response_builds_one_point_per_step_bucket() {
        let batch = metric_start_batch(&[9, 10, 12, 14, 15]);
        let plan = count_over_time_plan();

        let resp = assemble_metrics_response(
            &[batch],
            UnixNano(10),
            UnixNano(14),
            DurationNanos(2),
            &plan,
            MetricExemplarPolicy::new(0, &EngineOpts::default().histogram_buckets),
            UnixNano(10),
        )
        .unwrap();

        assert!(resp.series.len() == 1);
        assert!(
            resp.series[0].points
                == vec![
                    MetricPoint::new(10, 1.0),
                    MetricPoint::new(12, 1.0),
                    MetricPoint::new(14, 1.0)
                ]
        );
    }

    #[tokio::test]
    async fn count_over_time_counts_spans_regardless_of_value_field() {
        // count_over_time has no value field, so absent attributes still count.
        let e = one_trace_engine(vec![
            sp_with_code(1, 0, Some(10)),
            sp_with_code(2, 10_000, None),
            sp_with_code(3, 20_000, None),
        ]);

        let count = e
            .query_range(
                "t",
                "{ .svc = \"api\" } | count_over_time()",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();
        assert!(
            count.series[0].points == vec![MetricPoint::new(0, 3.0), MetricPoint::new(60_000, 0.0)]
        );
    }

    // ---------------------------------------------------------------------
    // compare() — Tempo attribute-comparison metric
    // ---------------------------------------------------------------------

    /// A span with an explicit status, attributes, and start time for compare tests.
    fn compare_span(id: u8, start: i64, status: i32, attrs: Vec<Attribute>) -> InputSpan {
        InputSpan {
            trace_id: [1; 16],
            span_id: [id; 8],
            parent_span_id: None,
            name: format!("op-{id}"),
            kind: 0,
            start_unix_nano: start,
            duration: nanos(100),
            status_code: status,
            status_message: String::new(),
            instrumentation_name: "tracer".into(),
            instrumentation_version: String::new(),
            attrs,
            events: Vec::new(),
            links: Vec::new(),
        }
    }

    fn series_label<'a>(series: &'a TraceMetricSeries, key: &str) -> Option<&'a str> {
        series
            .labels
            .iter()
            .find(|label| label.key == key)
            .map(|label| label.value.as_str())
    }

    // Compare counts are exact integers; sum them as i64 so assertions avoid
    // float equality (clippy::float_cmp).
    fn sum_points(points: &[MetricPoint]) -> i64 {
        use num_traits::ToPrimitive;

        points
            .iter()
            .map(|point| point.value.round().to_i64().expect("metric count fits i64"))
            .sum()
    }

    /// Total count across all buckets for the series whose `__meta_type` is
    /// `meta` and whose `attr_key` label equals `value`.
    fn compare_total(resp: &TraceMetricsResponse, meta: &str, attr_key: &str, value: &str) -> i64 {
        resp.series
            .iter()
            .filter(|series| {
                series_label(series, "__meta_type") == Some(meta)
                    && series_label(series, attr_key) == Some(value)
            })
            .map(|series| sum_points(&series.points))
            .sum()
    }

    fn meta_total(resp: &TraceMetricsResponse, meta: &str) -> i64 {
        resp.series
            .iter()
            .filter(|series| series_label(series, "__meta_type") == Some(meta))
            .map(|series| sum_points(&series.points))
            .sum()
    }

    #[tokio::test]
    async fn compare_partitions_baseline_and_selection_by_status() {
        // Three error spans (selection) and two ok spans (baseline). The selection
        // is `{ status = error }`; baseline is the matched-outer spans not in it.
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "api",
            "root",
            vec![
                compare_span(
                    1,
                    0,
                    2,
                    vec![Attribute::new("http.method", AttrValue::Str("GET".into()))],
                ),
                compare_span(
                    2,
                    0,
                    2,
                    vec![Attribute::new("http.method", AttrValue::Str("GET".into()))],
                ),
                compare_span(
                    3,
                    0,
                    2,
                    vec![Attribute::new("http.method", AttrValue::Str("POST".into()))],
                ),
                compare_span(
                    4,
                    0,
                    1,
                    vec![Attribute::new("http.method", AttrValue::Str("GET".into()))],
                ),
                compare_span(
                    5,
                    0,
                    1,
                    vec![Attribute::new("http.method", AttrValue::Str("PUT".into()))],
                ),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = query_one_bucket(&e, "{} | compare({ status = error }, 10)").await;

        // Per-group totals: 3 selection spans, 2 baseline spans.
        check!(meta_total(&resp, "selection_total") == 3);
        check!(meta_total(&resp, "baseline_total") == 2);

        let cases = [
            // Selection http.method distribution: GET=2, POST=1.
            CompareTotalCase {
                meta: "selection",
                attr: "span.http.method",
                value: "GET",
                want: 2,
            },
            CompareTotalCase {
                meta: "selection",
                attr: "span.http.method",
                value: "POST",
                want: 1,
            },
            // Baseline http.method distribution: GET=1, PUT=1.
            CompareTotalCase {
                meta: "baseline",
                attr: "span.http.method",
                value: "GET",
                want: 1,
            },
            CompareTotalCase {
                meta: "baseline",
                attr: "span.http.method",
                value: "PUT",
                want: 1,
            },
            // The status intrinsic distribution is emitted too.
            CompareTotalCase {
                meta: "selection",
                attr: "status",
                value: "error",
                want: 3,
            },
            CompareTotalCase {
                meta: "baseline",
                attr: "status",
                value: "ok",
                want: 2,
            },
        ];
        for case in cases {
            check!(
                compare_total(&resp, case.meta, case.attr, case.value) == case.want,
                "{} {}={}",
                case.meta,
                case.attr,
                case.value
            );
        }

        // Every value series carries exactly the __meta_type + one attr label.
        for series in &resp.series {
            let meta = series_label(series, "__meta_type").unwrap();
            if meta.ends_with("_total") {
                assert!(series.labels.len() == 1);
            } else {
                assert!(
                    series.labels.len() == 2,
                    "value series labels: {:?}",
                    series.labels
                );
            }
        }
    }

    #[tokio::test]
    async fn compare_top_n_truncates_least_frequent_values() {
        // Four distinct values for span.path with frequencies 4,3,2,1; topN=2 keeps
        // only the two most frequent (a=4, b=3) in the selection group.
        let mut spans = Vec::new();
        let mut id = 1u8;
        for case in [
            PathFrequencyCase {
                path: "a",
                frequency: 4,
            },
            PathFrequencyCase {
                path: "b",
                frequency: 3,
            },
            PathFrequencyCase {
                path: "c",
                frequency: 2,
            },
            PathFrequencyCase {
                path: "d",
                frequency: 1,
            },
        ] {
            for _ in 0..case.frequency {
                spans.push(compare_span(
                    id,
                    0,
                    2,
                    vec![Attribute::new("path", AttrValue::Str(case.path.into()))],
                ));
                id += 1;
            }
        }
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "api", "root", spans);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = query_one_bucket(&e, "{} | compare({ status = error }, 2)").await;

        // Exactly the two most-frequent path values survive topN=2.
        let kept: std::collections::BTreeSet<&str> = resp
            .series
            .iter()
            .filter(|series| {
                series_label(series, "__meta_type") == Some("selection")
                    && series.labels.iter().any(|label| label.key == "span.path")
            })
            .filter_map(|series| series_label(series, "span.path"))
            .collect();
        assert!(kept == ["a", "b"].into_iter().collect());
        // All five error spans are still counted in the selection total.
        assert!(meta_total(&resp, "selection_total") == 10);
    }

    #[tokio::test]
    async fn compare_emits_zero_totals_for_empty_group() {
        // No span matches the selection, so the selection group is empty but a
        // zero-valued selection_total series is still emitted (Grafana needs a
        // denominator for both groups).
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "api",
            "root",
            vec![compare_span(
                1,
                0,
                1,
                vec![Attribute::new("k", AttrValue::Str("v".into()))],
            )],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = query_one_bucket(&e, "{} | compare({ status = error }, 10)").await;

        check!(meta_total(&resp, "baseline_total") == 1);
        check!(meta_total(&resp, "selection_total") == 0);
        // The lone span lands in baseline.
        check!(compare_total(&resp, "baseline", "span.k", "v") == 1);
    }

    #[tokio::test]
    async fn compare_selection_window_restricts_selection_group() {
        // Two error spans, one inside the [start,end]=[5000,15000] sub-window and
        // one outside it. With the window, only the in-window error span joins the
        // selection group; the out-of-window error span falls to baseline.
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "api",
            "root",
            vec![
                compare_span(
                    1,
                    10_000,
                    2,
                    vec![Attribute::new("z", AttrValue::Str("in".into()))],
                ),
                compare_span(
                    2,
                    30_000,
                    2,
                    vec![Attribute::new("z", AttrValue::Str("out".into()))],
                ),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = query_one_bucket(&e, "{} | compare({ status = error }, 10, 5000, 15000)").await;

        check!(meta_total(&resp, "selection_total") == 1);
        check!(meta_total(&resp, "baseline_total") == 1);
        check!(compare_total(&resp, "selection", "span.z", "in") == 1);
        check!(compare_total(&resp, "baseline", "span.z", "out") == 1);
    }

    #[tokio::test]
    async fn compare_accepts_structural_selection() {
        let e = engine();
        let result = e
            .query_range(
                "t",
                "{} | compare({ .svc != nil } >> { .svc != nil }, 10)",
                0,
                60_000,
                60_000,
            )
            .await;
        assert!(result.is_ok(), "{result:?}");
    }

    #[tokio::test]
    async fn compare_accepts_event_link_and_parent_selection_scopes() {
        let e = engine();
        for query in [
            "{} | compare({ event:name != nil }, 10)",
            "{} | compare({ link.traceID != nil }, 10)",
            "{} | compare({ parent.svc != nil }, 10)",
        ] {
            let result = e.query_range("t", query, 0, 60_000, 60_000).await;
            assert!(
                result.is_ok(),
                "query should support compare selection scope: {query}: {result:?}"
            );
        }
    }

    // ---- MAJOR 1: resource.service.name is the trace-root service only ----

    /// Builds a single-row batch in the block shape of the REAL store.
    ///
    /// The `attr_keys` and typed-value list columns carry three attributes: a
    /// per-span `__resource.service.name`, which the code MUST ignore, a plain
    /// resource attribute `__resource.cluster`, and a span attribute
    /// `http.method`. The batch also carries the standard scalar columns that
    /// `compare_row` reads: name, status, kind, duration, root-service, and
    /// start. The root-service column is the canonical
    /// `resource.service.name` source.
    fn compare_block_batch() -> RecordBatch {
        use arrow::array::{
            BooleanBuilder, Float64Builder, Int64Builder, ListBuilder, StringBuilder,
        };

        // attr_keys: [["__resource.service.name", "__resource.cluster",
        //              "http.method"]]. All three are string-typed.
        let keys_list = [
            "__resource.service.name",
            "__resource.cluster",
            "http.method",
        ];
        let mut keys = ListBuilder::new(StringBuilder::new());
        for k in keys_list {
            keys.values().append_value(k);
        }
        keys.append(true);

        // attr_value (string): the per-span resource service is "span-svc" (this
        // value must NOT surface); cluster=eu; http.method=GET.
        let mut str_values = ListBuilder::new(ListBuilder::new(StringBuilder::new()));
        str_values.values().values().append_value("span-svc");
        str_values.values().append(true); // __resource.service.name -> ["span-svc"]
        str_values.values().values().append_value("eu");
        str_values.values().append(true); // __resource.cluster -> ["eu"]
        str_values.values().values().append_value("GET");
        str_values.values().append(true); // http.method -> ["GET"]
        str_values.append(true);

        // The other typed columns are empty inner lists for every key.
        let empty_list = |count: usize| {
            let mut int_values = ListBuilder::new(ListBuilder::new(Int64Builder::new()));
            let mut double_values = ListBuilder::new(ListBuilder::new(Float64Builder::new()));
            let mut bool_values = ListBuilder::new(ListBuilder::new(BooleanBuilder::new()));
            for _ in 0..count {
                int_values.values().append(true);
                double_values.values().append(true);
                bool_values.values().append(true);
            }
            int_values.append(true);
            double_values.append(true);
            bool_values.append(true);
            EmptyTypedValues {
                int_values: int_values.finish(),
                double_values: double_values.finish(),
                bool_values: bool_values.finish(),
            }
        };
        let empty_values = empty_list(keys_list.len());

        let keys = keys.finish();
        let str_values = str_values.finish();

        let mut fields = block_attr_fields([
            &keys,
            &str_values,
            &empty_values.int_values,
            &empty_values.double_values,
            &empty_values.bool_values,
        ]);
        fields.extend([
            ArrowField::new(COL_ROOT_SERVICE_NAME, DataType::Utf8, true),
            ArrowField::new(COL_NAME, DataType::Utf8, true),
            ArrowField::new(COL_STATUS_CODE, DataType::Int32, false),
            ArrowField::new(COL_KIND, DataType::Int32, false),
            ArrowField::new(COL_DURATION, DataType::Int64, false),
            ArrowField::new(COL_START, DataType::Int64, false),
        ]);
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(keys) as ArrayRef,
                Arc::new(str_values),
                Arc::new(empty_values.int_values),
                Arc::new(empty_values.double_values),
                Arc::new(empty_values.bool_values),
                // Trace-root service name (the canonical resource.service.name).
                Arc::new(StringArray::from(vec!["root-svc"])),
                Arc::new(StringArray::from(vec!["op"])),
                Arc::new(Int32Array::from(vec![2])), // status = error
                Arc::new(Int32Array::from(vec![2])), // kind = server
                Arc::new(Int64Array::from(vec![100])),
                Arc::new(Int64Array::from(vec![0])),
            ],
        )
        .unwrap()
    }

    fn compare_range() -> MetricsRange {
        MetricsRange {
            scan_start: UnixNano(0),
            scan_end: UnixNano(60_000),
            output_start: UnixNano(0),
            step: DurationNanos(60_000),
            instant: false,
        }
    }

    fn selector(fe: FieldExpr) -> SpansetExpr {
        SpansetExpr::Selector(Box::new(fe))
    }

    fn compare_start_batch(starts: &[i64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![ArrowField::new(
            COL_START,
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(starts.to_vec())) as ArrayRef],
        )
        .unwrap()
    }

    #[test]
    fn compare_row_emits_single_root_resource_service_name() {
        // MAJOR 1: a single span row must surface `resource.service.name` exactly
        // once, equal to the trace-root service (COL_ROOT_SERVICE_NAME), never the
        // per-span `__resource.service.name` block attr.
        let batch = compare_block_batch();
        let row = compare_row(&batch, 0, UnixNano(0)).unwrap();
        let service: Vec<&String> = row
            .attrs
            .iter()
            .filter(|attribute| attribute.key == "resource.service.name")
            .map(|attribute| &attribute.value)
            .collect();
        assert!(service == vec![&"root-svc".to_string()]);
        // The per-span resource service value never leaks in.
        assert!(
            !row.attrs
                .iter()
                .any(|attribute| attribute.value == "span-svc")
        );
    }

    #[test]
    fn compare_block_batch_carries_span_and_resource_labels() {
        // MINOR 7: drive the real block-attr production path through
        // assemble_compare_response/compare_row. The series must carry BOTH a
        // span.<k> label and resource.<k> labels (including resource.service.name
        // = the trace-root service, MAJOR 1).
        let compare = CompareSpec {
            selection: selector(FieldExpr::Const(false)),
            top_n: 10,
            start: None,
            end: None,
        };
        let resp = assemble_compare_response(
            &[compare_block_batch()],
            &compare,
            compare_range(),
            256,
            None,
        )
        .unwrap();

        // The lone span falls to baseline (selection is Const(false)).
        for case in [
            AttributeValueCase {
                attr: "span.http.method",
                value: "GET",
            },
            AttributeValueCase {
                attr: "resource.cluster",
                value: "eu",
            },
            AttributeValueCase {
                attr: "resource.service.name",
                value: "root-svc",
            },
        ] {
            check!(
                compare_total(&resp, "baseline", case.attr, case.value) == 1,
                "baseline {}={}",
                case.attr,
                case.value
            );
        }
        // Exactly one resource.service.name value series exists, and it is the
        // trace-root service (no duplicate from the per-span block attr).
        let svc_values: std::collections::BTreeSet<&str> = resp
            .series
            .iter()
            .filter_map(|series| series_label(series, "resource.service.name"))
            .collect();
        assert!(svc_values == ["root-svc"].into_iter().collect());
    }

    #[test]
    fn assemble_compare_response_allows_equal_start_and_rejects_reversed_range() {
        let compare = CompareSpec {
            selection: selector(FieldExpr::Const(false)),
            top_n: 10,
            start: None,
            end: None,
        };

        let equal_range = MetricsRange {
            scan_start: UnixNano(0),
            scan_end: UnixNano(0),
            output_start: UnixNano(0),
            step: DurationNanos(60_000),
            instant: false,
        };
        let resp =
            assemble_compare_response(&[compare_block_batch()], &compare, equal_range, 256, None)
                .unwrap();
        assert!(meta_total(&resp, "baseline_total") == 1);

        let reversed_range = MetricsRange {
            scan_start: UnixNano(60_000),
            scan_end: UnixNano(0),
            output_start: UnixNano(60_000),
            step: DurationNanos(60_000),
            instant: false,
        };
        assert!(
            assemble_compare_response(
                &[compare_block_batch()],
                &compare,
                reversed_range,
                256,
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn assemble_compare_response_builds_one_point_per_step_bucket() {
        let compare = CompareSpec {
            selection: selector(FieldExpr::Const(false)),
            top_n: 10,
            start: None,
            end: None,
        };
        let range = MetricsRange {
            scan_start: UnixNano(10),
            scan_end: UnixNano(14),
            output_start: UnixNano(10),
            step: DurationNanos(2),
            instant: false,
        };
        let resp = assemble_compare_response(
            &[compare_start_batch(&[9, 10, 12, 14, 15])],
            &compare,
            range,
            256,
            None,
        )
        .unwrap();
        let baseline_total = resp
            .series
            .iter()
            .find(|series| series_label(series, "__meta_type") == Some("baseline_total"))
            .unwrap();

        assert!(
            baseline_total.points
                == vec![
                    MetricPoint::new(10, 1.0),
                    MetricPoint::new(12, 1.0),
                    MetricPoint::new(14, 1.0)
                ]
        );
    }

    #[test]
    fn instant_compare_pools_both_endpoints_and_excludes_outside_rows() {
        let compare = CompareSpec {
            selection: selector(FieldExpr::Const(false)),
            top_n: 10,
            start: None,
            end: None,
        };
        let response = assemble_compare_response(
            &[compare_start_batch(&[-1, 0, 1, 2, 3])],
            &compare,
            MetricsRange {
                scan_start: UnixNano(0),
                scan_end: UnixNano(2),
                output_start: UnixNano(2),
                step: DurationNanos(2),
                instant: true,
            },
            256,
            None,
        )
        .unwrap();
        let baseline = response
            .series
            .iter()
            .find(|series| series_label(series, "__meta_type") == Some("baseline_total"))
            .unwrap();
        assert!(baseline.points == vec![MetricPoint::new(2, 3.0)]);
        assert!(
            response
                .series
                .iter()
                .all(|series| series.points.len() == 1)
        );
    }

    // ---- MAJOR 3: != / !~ on an absent attribute stays Baseline ----

    #[test]
    fn compare_neq_concrete_on_absent_attr_is_baseline() {
        // MAJOR 3: a span lacking `.foo` with selection `{ .foo != "bar" }` must
        // NOT be pulled into the selection group — an absent attr matches only
        // `= nil`, mirroring the planner SQL (`NULL != v` excludes).
        let row = CompareRow {
            ts: UnixNano(0),
            attrs: vec![DisplayAttribute::new("span.other", "x")],
            raw_span_attrs: vec![Attribute::new("other", AttrValue::Str("x".into()))],
            raw_resource_attrs: Vec::new(),
            name: None,
            status_code: None,
            status_message: None,
            kind: None,
            duration: None,
            columns: std::collections::BTreeMap::new(),
        };
        let neq = CompareSpec {
            selection: selector(FieldExpr::Comparison {
                lhs: field(Scope::Span, "foo"),
                op: ComparisonOp::Neq,
                rhs: Value::Str("bar".into()),
            }),
            top_n: 10,
            start: None,
            end: None,
        };
        let regexes = CompareRegexCache::new();
        assert!(compare_group_for_row(&row, &neq, &regexes, None) == CompareGroup::Baseline);

        // `= nil` on the same absent attr DOES match (the only matching op).
        let eq_nil = CompareSpec {
            selection: selector(FieldExpr::Comparison {
                lhs: field(Scope::Span, "foo"),
                op: ComparisonOp::Eq,
                rhs: Value::Nil,
            }),
            ..neq.clone()
        };
        assert!(compare_group_for_row(&row, &eq_nil, &regexes, None) == CompareGroup::Selection);
    }

    #[test]
    fn compare_row_selection_evaluator_obeys_boolean_and_presence_semantics() {
        let row = CompareRow {
            ts: UnixNano(0),
            attrs: vec![DisplayAttribute::new("span.present", "yes")],
            raw_span_attrs: vec![Attribute::new("present", AttrValue::Str("yes".into()))],
            raw_resource_attrs: vec![
                Attribute::new("region", AttrValue::Str("eu".into())),
                Attribute::new("cluster", AttrValue::Str("prod".into())),
            ],
            name: None,
            status_code: None,
            status_message: None,
            kind: None,
            duration: None,
            columns: std::collections::BTreeMap::new(),
        };
        let present = FieldExpr::Comparison {
            lhs: field(Scope::Span, "present"),
            op: ComparisonOp::Eq,
            rhs: Value::Str("yes".into()),
        };
        let missing = FieldExpr::Comparison {
            lhs: field(Scope::Span, "missing"),
            op: ComparisonOp::Eq,
            rhs: Value::Str("yes".into()),
        };
        let regexes = CompareRegexCache::new();

        assert!(compare_field_present(&field(Scope::Span, "present"), &row));
        assert!(!compare_field_present(&field(Scope::Span, "missing"), &row));
        let resource_values = compare_row_attr_values(&row, &Scope::Resource, "region");
        check!(resource_values == vec![&AttrValue::Str("eu".into())]);
        check!(!field_expr_matches_row(
            &FieldExpr::And(Box::new(present.clone()), Box::new(missing.clone())),
            &row,
            &regexes,
        ));
        check!(field_expr_matches_row(
            &FieldExpr::Or(Box::new(present.clone()), Box::new(missing.clone())),
            &row,
            &regexes,
        ));
        check!(!field_expr_matches_row(
            &FieldExpr::Not(Box::new(present.clone())),
            &row,
            &regexes,
        ));
        check!(!spanset_matches_row(
            &SpansetExpr::And(
                Box::new(selector(present.clone())),
                Box::new(selector(missing.clone())),
            ),
            &row,
            &regexes,
        ));
        check!(spanset_matches_row(
            &SpansetExpr::Or(Box::new(selector(present)), Box::new(selector(missing))),
            &row,
            &regexes,
        ));
    }

    #[test]
    fn compare_attr_neq_requires_every_repeated_value_to_differ() {
        let get = AttrValue::Str("GET".into());
        let post = AttrValue::Str("POST".into());
        let values = vec![&get, &post];
        let regexes = CompareRegexCache::new();

        assert!(!compare_attr_values_match(
            &values,
            ComparisonOp::Neq,
            &Value::Str("GET".into()),
            &regexes,
        ));
        assert!(compare_attr_values_match(
            &values,
            ComparisonOp::Neq,
            &Value::Str("PUT".into()),
            &regexes,
        ));
    }

    #[test]
    fn compare_value_match_covers_numeric_and_bool_attrs() {
        let regexes = CompareRegexCache::new();

        let cases = [
            AttrComparisonCase {
                attr: AttrValue::Int(42),
                rhs: Value::Int(42),
            },
            AttrComparisonCase {
                attr: AttrValue::Int(42),
                rhs: Value::Duration(42),
            },
            AttrComparisonCase {
                attr: AttrValue::Float(1.5),
                rhs: Value::Float(1.5),
            },
            AttrComparisonCase {
                attr: AttrValue::Bool(true),
                rhs: Value::Bool(true),
            },
        ];
        for case in cases {
            check!(
                compare_value_match(&case.attr, ComparisonOp::Eq, &case.rhs, &regexes),
                "{:?} should equal {:?}",
                case.attr,
                case.rhs
            );
        }
    }

    #[test]
    fn compare_intrinsic_present_covers_supported_intrinsics_and_empty_values() {
        let populated = CompareRow {
            ts: UnixNano(0),
            attrs: Vec::new(),
            raw_span_attrs: Vec::new(),
            raw_resource_attrs: Vec::new(),
            name: Some("op".into()),
            status_code: Some(2),
            status_message: Some("failed".into()),
            kind: Some(3),
            duration: Some(100),
            columns: std::collections::BTreeMap::new(),
        };
        for intrinsic in [
            Intrinsic::Name,
            Intrinsic::Status,
            Intrinsic::StatusMessage,
            Intrinsic::Kind,
            Intrinsic::Duration,
        ] {
            assert!(compare_intrinsic_present(&populated, &intrinsic));
        }

        let empty = CompareRow {
            name: Some(String::new()),
            status_message: Some(String::new()),
            status_code: None,
            kind: None,
            duration: None,
            columns: std::collections::BTreeMap::new(),
            ..populated
        };
        for intrinsic in [
            Intrinsic::Name,
            Intrinsic::Status,
            Intrinsic::StatusMessage,
            Intrinsic::Kind,
            Intrinsic::Duration,
        ] {
            assert!(!compare_intrinsic_present(&empty, &intrinsic));
        }
    }

    #[test]
    fn compare_intrinsic_matches_covers_status_message_kind_and_duration() {
        let row = CompareRow {
            ts: UnixNano(0),
            attrs: Vec::new(),
            raw_span_attrs: Vec::new(),
            raw_resource_attrs: Vec::new(),
            name: None,
            status_code: None,
            status_message: Some("failed".into()),
            kind: Some(3),
            duration: Some(100),
            columns: std::collections::BTreeMap::new(),
        };
        let regexes = CompareRegexCache::new();

        let cases = [
            IntrinsicComparisonCase {
                intrinsic: Intrinsic::StatusMessage,
                op: ComparisonOp::Eq,
                rhs: Value::Str("failed".into()),
            },
            IntrinsicComparisonCase {
                intrinsic: Intrinsic::Kind,
                op: ComparisonOp::Eq,
                rhs: Value::Str("client".into()),
            },
            IntrinsicComparisonCase {
                intrinsic: Intrinsic::Duration,
                op: ComparisonOp::Gt,
                rhs: Value::Duration(50),
            },
        ];
        for case in cases {
            check!(
                compare_intrinsic_matches(&row, &case.intrinsic, case.op, &case.rhs, &regexes),
                "{:?} {:?} {:?}",
                case.intrinsic,
                case.op,
                case.rhs
            );
        }
    }

    #[test]
    fn compare_scalar_helpers_cover_enum_int_negated_regex_and_numeric_neq() {
        let mut regexes = CompareRegexCache::new();
        regexes.insert("op-.*".into(), regex::Regex::new("^(?:op-.*)$").unwrap());

        for case in [
            StatusEnumCase {
                code: 0,
                value: Value::Str("unset".into()),
            },
            StatusEnumCase {
                code: 1,
                value: Value::Str("ok".into()),
            },
            StatusEnumCase {
                code: 2,
                value: Value::Int(2),
            },
        ] {
            check!(
                enum_cmp(case.code, ComparisonOp::Eq, &case.value, status_enum_value),
                "status code {} vs {:?}",
                case.code,
                case.value
            );
        }
        check!(!string_cmp("op-1", ComparisonOp::Nre, "op-.*", &regexes));

        for case in [
            IntegerComparisonCase {
                lhs: 42,
                op: ComparisonOp::Neq,
                rhs: 7,
                expected: true,
            },
            IntegerComparisonCase {
                lhs: 3,
                op: ComparisonOp::Lt,
                rhs: 5,
                expected: true,
            },
            IntegerComparisonCase {
                lhs: 5,
                op: ComparisonOp::Lt,
                rhs: 5,
                expected: false,
            },
            IntegerComparisonCase {
                lhs: 5,
                op: ComparisonOp::Lte,
                rhs: 5,
                expected: true,
            },
            IntegerComparisonCase {
                lhs: 7,
                op: ComparisonOp::Gt,
                rhs: 5,
                expected: true,
            },
            IntegerComparisonCase {
                lhs: 5,
                op: ComparisonOp::Gt,
                rhs: 5,
                expected: false,
            },
            IntegerComparisonCase {
                lhs: 5,
                op: ComparisonOp::Gte,
                rhs: 5,
                expected: true,
            },
        ] {
            check!(
                num_cmp(case.lhs, case.op, case.rhs) == case.expected,
                "num_cmp({}, {:?}, {})",
                case.lhs,
                case.op,
                case.rhs
            );
        }

        for case in [
            FloatComparisonCase {
                lhs: 1.0,
                op: ComparisonOp::Eq,
                rhs: 2.0,
                expected: false,
            },
            FloatComparisonCase {
                lhs: 1.0,
                op: ComparisonOp::Neq,
                rhs: 2.0,
                expected: true,
            },
            FloatComparisonCase {
                lhs: 1.0,
                op: ComparisonOp::Lt,
                rhs: 2.0,
                expected: true,
            },
            FloatComparisonCase {
                lhs: 2.0,
                op: ComparisonOp::Lt,
                rhs: 2.0,
                expected: false,
            },
            FloatComparisonCase {
                lhs: 2.0,
                op: ComparisonOp::Lte,
                rhs: 2.0,
                expected: true,
            },
            FloatComparisonCase {
                lhs: 3.0,
                op: ComparisonOp::Gt,
                rhs: 2.0,
                expected: true,
            },
            FloatComparisonCase {
                lhs: 2.0,
                op: ComparisonOp::Gt,
                rhs: 2.0,
                expected: false,
            },
            FloatComparisonCase {
                lhs: 2.0,
                op: ComparisonOp::Gte,
                rhs: 2.0,
                expected: true,
            },
        ] {
            check!(
                float_cmp(case.lhs, case.op, case.rhs) == case.expected,
                "float_cmp({}, {:?}, {})",
                case.lhs,
                case.op,
                case.rhs
            );
        }

        check!(!bool_cmp(true, ComparisonOp::Eq, false));
        check!(bool_cmp(true, ComparisonOp::Neq, false));
    }

    #[test]
    fn compare_kind_enum_helpers_cover_all_names() {
        let cases = [
            KindNameCase {
                name: "unspecified",
                code: 0,
            },
            KindNameCase {
                name: "internal",
                code: 1,
            },
            KindNameCase {
                name: "server",
                code: 2,
            },
            KindNameCase {
                name: "client",
                code: 3,
            },
            KindNameCase {
                name: "producer",
                code: 4,
            },
            KindNameCase {
                name: "consumer",
                code: 5,
            },
        ];

        for case in cases {
            assert!(kind_enum_value(case.name) == Some(case.code));
            assert!(kind_enum_name(case.code) == case.name);
        }
        assert!(kind_enum_value("unknown") == None);
        assert!(kind_enum_name(-1) == "unspecified");
    }

    #[test]
    fn compare_row_filters_promoted_event_and_link_attrs() {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new(
                format!("{ATTR_PREFIX}{EVENT_ATTR_PREFIX}exception.type"),
                DataType::Utf8,
                true,
            ),
            ArrowField::new(
                format!("{ATTR_PREFIX}{LINK_ATTR_PREFIX}trace_id"),
                DataType::Utf8,
                true,
            ),
            ArrowField::new(format!("{ATTR_PREFIX}http.method"), DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["IOError"])) as ArrayRef,
                Arc::new(StringArray::from(vec!["abcd"])),
                Arc::new(StringArray::from(vec!["GET"])),
            ],
        )
        .unwrap();

        let row = compare_row(&batch, 0, UnixNano(0)).unwrap();

        assert!(row.attrs == vec![DisplayAttribute::new("span.http.method", "GET")]);
        assert!(
            row.raw_span_attrs
                == vec![
                    Attribute::new(
                        format!("{EVENT_ATTR_PREFIX}exception.type"),
                        AttrValue::Str("IOError".into()),
                    ),
                    Attribute::new(
                        format!("{LINK_ATTR_PREFIX}trace_id"),
                        AttrValue::Str("abcd".into()),
                    ),
                    Attribute::new("http.method", AttrValue::Str("GET".into())),
                ]
        );
    }

    // ---- MAJOR 4: baseline and selection share one value set per attribute ----

    #[tokio::test]
    async fn compare_emits_shared_value_set_across_groups() {
        // MAJOR 4: `span.region` has value "eu" in BOTH groups and "us" only in
        // the selection. The chosen value set is the selection top-N; for each
        // chosen value BOTH a baseline and a selection series are emitted, so a
        // selection-only value ("us") gets a zero-filled baseline series too.
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "api",
            "root",
            vec![
                // selection (status=error): region eu, eu, us.
                compare_span(
                    1,
                    0,
                    2,
                    vec![Attribute::new("region", AttrValue::Str("eu".into()))],
                ),
                compare_span(
                    2,
                    0,
                    2,
                    vec![Attribute::new("region", AttrValue::Str("eu".into()))],
                ),
                compare_span(
                    3,
                    0,
                    2,
                    vec![Attribute::new("region", AttrValue::Str("us".into()))],
                ),
                // baseline (status=ok): region eu.
                compare_span(
                    4,
                    0,
                    1,
                    vec![Attribute::new("region", AttrValue::Str("eu".into()))],
                ),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());
        let resp = query_one_bucket(&e, "{} | compare({ status = error }, 10)").await;

        // Both groups expose the SAME span.region value set (the selection top-N).
        let value_set = |meta: &str| -> std::collections::BTreeSet<String> {
            resp.series
                .iter()
                .filter(|series| series_label(series, "__meta_type") == Some(meta))
                .filter_map(|series| series_label(series, "span.region").map(str::to_string))
                .collect()
        };
        let selection_set = value_set("selection");
        let baseline_set = value_set("baseline");
        check!(selection_set == ["eu".to_string(), "us".to_string()].into_iter().collect());
        check!(baseline_set == selection_set);

        let cases = [
            // The selection-only value "us" has a zero-filled baseline series.
            RegionCountCase {
                meta: "selection",
                value: "us",
                want: 1,
            },
            RegionCountCase {
                meta: "baseline",
                value: "us",
                want: 0,
            },
            // The shared value "eu": selection 2, baseline 1.
            RegionCountCase {
                meta: "selection",
                value: "eu",
                want: 2,
            },
            RegionCountCase {
                meta: "baseline",
                value: "eu",
                want: 1,
            },
        ];
        for case in cases {
            check!(
                compare_total(&resp, case.meta, "span.region", case.value) == case.want,
                "{} span.region={}",
                case.meta,
                case.value
            );
        }
    }

    // ---- MAJOR 2: per-attribute value cardinality is bounded ----

    /// Builds an `n`-row batch with one promoted string column `attr.path`.
    ///
    /// The column value is unique per row, `/p/<i>`, and every row starts at
    /// time 0. This batch drives the high-cardinality accumulation path.
    fn unique_path_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            ArrowField::new(COL_START, DataType::Int64, false),
            ArrowField::new(format!("{ATTR_PREFIX}path"), DataType::Utf8, true),
        ]));
        let starts = Int64Array::from(vec![0_i64; n]);
        let paths = StringArray::from((0..n).map(|i| format!("/p/{i}")).collect::<Vec<_>>());
        RecordBatch::try_new(schema, vec![Arc::new(starts), Arc::new(paths)]).unwrap()
    }

    #[test]
    fn compare_bounds_per_attribute_value_cardinality() {
        // MAJOR 2: feed 1000 per-span-unique span.path values. Unbounded, the
        // accumulator would hold one bucket-Vec per value. With top_n=10 the cap
        // clamps to COMPARE_MAX_VALUES_PER_ATTR (256), so at most 256 distinct
        // span.path values are tracked per (group, attr) BEFORE truncation, and
        // the emitted series obey top_n=10 AFTER.
        let batch = unique_path_batch(1000);
        let compare = CompareSpec {
            // Const(true): every span joins the selection group.
            selection: selector(FieldExpr::Const(true)),
            top_n: 10,
            start: None,
            end: None,
        };
        let range = compare_range();
        let bucket_count = 2;

        // Pre-truncation: distinct span.path values per group are capped.
        let accumulation =
            accumulate_compare_counts(&[batch], &compare, range, bucket_count, 256, None).unwrap();
        let counts = accumulation.counts;
        let totals = accumulation.totals;
        let distinct_paths = counts
            .keys()
            .filter(|key| key.group == CompareGroup::Selection && key.attr_key == "span.path")
            .count();
        assert!(
            distinct_paths == 256,
            "tracked {distinct_paths} distinct values, expected cap 256"
        );
        // All 1000 spans are still counted in the per-group total (the cap only
        // limits tracked distinct VALUES, never the per-group span totals).
        assert!(totals[&CompareGroup::Selection].iter().sum::<u64>() == 1000);

        let configured_counts = accumulate_compare_counts(
            &[unique_path_batch(1000)],
            &compare,
            range,
            bucket_count,
            17,
            None,
        )
        .unwrap()
        .counts;
        assert!(
            configured_counts
                .keys()
                .filter(|key| key.group == CompareGroup::Selection && key.attr_key == "span.path")
                .count()
                == 17
        );

        // Post-truncation: the emitted span.path series obey top_n.
        let resp =
            assemble_compare_response(&[unique_path_batch(1000)], &compare, range, 256, None)
                .unwrap();
        let path_series = resp
            .series
            .iter()
            .filter(|series| {
                series_label(series, "__meta_type") == Some("selection")
                    && series.labels.iter().any(|label| label.key == "span.path")
            })
            .count();
        assert!(path_series <= compare.top_n);
    }

    // ---- MINOR 5: event/link attrs do not leak as span.__event.* ----

    #[tokio::test]
    async fn compare_does_not_leak_event_attrs_as_span_labels() {
        // MINOR 5: the outer query references an event attr, so the scan
        // materializes an `attr.__event.exception.type` column. It must NOT appear
        // in the span distribution as `span.__event.exception.type`.
        let mut span = compare_span(
            1,
            0,
            2,
            vec![Attribute::new("http.method", AttrValue::Str("GET".into()))],
        );
        span.events = vec![EventRef {
            time_since_start: nanos(10),
            name: "exception".into(),
            attributes: vec![Attribute::new(
                "exception.type",
                AttrValue::Str("IOError".into()),
            )],
        }];
        let mut s = InMemorySpanStore::new();
        s.push_trace("t", "api", "root", vec![span]);
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = query_one_bucket(
            &e,
            "{ event.exception.type != nil } | compare({ status = error }, 10)",
        )
        .await;

        // No series carries a span.__event.* / span.__link.* label.
        let leaked = resp.series.iter().any(|series| {
            series.labels.iter().any(|label| {
                label.key.starts_with("span.__event.") || label.key.starts_with("span.__link.")
            })
        });
        assert!(
            !leaked,
            "event/link attrs leaked into the span distribution"
        );
    }

    // ---- MINOR 6: selection regex is compiled once and reused ----

    #[tokio::test]
    async fn compare_regex_selection_matches_with_shared_cache() {
        // MINOR 6: a `=~` selection still partitions correctly when the regex is
        // precompiled once per query. Spans whose name matches `op-[12]` join the
        // selection; the rest stay baseline. (Correctness check for the cached
        // regex path; the cache merely avoids per-row recompilation.)
        let mut s = InMemorySpanStore::new();
        s.push_trace(
            "t",
            "api",
            "root",
            vec![
                compare_span(
                    1,
                    0,
                    2,
                    vec![Attribute::new("k", AttrValue::Str("a".into()))],
                ),
                compare_span(
                    2,
                    0,
                    2,
                    vec![Attribute::new("k", AttrValue::Str("b".into()))],
                ),
                compare_span(
                    3,
                    0,
                    2,
                    vec![Attribute::new("k", AttrValue::Str("c".into()))],
                ),
            ],
        );
        let e = TraceqlEngine::new(Arc::new(s), EngineOpts::default());

        let resp = e
            .query_range(
                "t",
                "{} | compare({ name =~ \"op-[12]\" }, 10)",
                0,
                60_000,
                60_000,
            )
            .await
            .unwrap();

        // op-1 and op-2 match the regex (selection=2); op-3 does not (baseline=1).
        assert!(meta_total(&resp, "selection_total") == 2);
        assert!(meta_total(&resp, "baseline_total") == 1);
    }
}

mod accumulate_compare_counts;
mod apply_metric_filter;
mod apply_rank;
mod assemble_compare_response;
mod assemble_metrics_response;
mod assemble_search_response;
mod attr_value_display;
mod block_attr_keys;
mod block_attr_value;
mod block_attr_value_bool;
mod block_attr_value_double;
mod block_attr_value_int;
mod block_attr_values_for_key;
mod block_row_attrs;
mod block_row_attrs_where;
mod block_row_scoped_attrs;
mod bool_attr_values;
mod bool_cmp;
mod build_compare_series;
mod bytes_to_hex;
mod collect_field_expr_regexes;
mod collect_planned_batches;
mod collect_selection_regexes;
mod compare_attr_values_match;
mod compare_by_attr;
mod compare_comparison_matches;
mod compare_counts;
mod compare_field_class;
mod compare_field_present;
mod compare_group;
mod compare_group_for_row;
mod compare_intrinsic_matches;
mod compare_intrinsic_present;
mod compare_points;
mod compare_regex_cache;
mod compare_row;
mod compare_row_attr_values;
mod compare_row_in_selection_window;
mod compare_span_identities;
mod compare_spec;
mod compare_total_series;
mod compare_totals;
mod compare_value_match;
mod compare_value_series;
mod count_over_time_plan;
mod deduplicate_search_spans;
mod engine_opts;
mod enum_cmp;
mod extend_metric_projection_matchers;
mod f64_attr_values;
mod f64_from_i64;
mod f64_from_u64;
mod f64_from_usize;
mod field_expr_matches_row;
mod fixed_16;
mod fixed_8;
mod fixed_bytes;
mod float_cmp;
mod hinted_max_exemplars;
mod histogram_points;
mod histogram_series_for_group;
mod i32_value;
mod i64_attr_values;
mod i64_value;
mod is_inert_metric_stage;
mod kind_enum_name;
mod meta_type_key;
mod metric_bucket;
mod metric_exemplar;
mod metric_exemplars;
mod metric_field_column;
mod metric_field_number;
mod metric_filter;
mod metric_filter_passes;
mod metric_function;
mod metric_label_key;
mod metric_label_value;
mod metric_labels;
mod metric_nested_projection_matchers;
mod metric_numeric_value;
mod metric_pipeline_parts;
mod metric_plan;
mod metric_plan_for;
mod metric_plan_with_compare;
mod metric_series_for_group;
mod metric_timing;
mod metrics_range;
mod num_cmp;
mod optional_fixed_8;
mod optional_list_column;
mod push_scoped_attr;
mod quantile_label;
mod rank_limit;
mod resource_attr_prefix;
mod row_attr_values;
mod row_attrs;
mod sample_exemplar_rows;
mod search_options;
mod span_start_column;
mod spanset_matches_row;
mod status_enum_name;
mod string_array_value;
mod string_attr_values;
mod string_cmp;
mod string_value;
mod trace_acc;
mod traceql_engine;
mod u64_from_i64;
mod unsupported_metric_pipeline;
mod usize_from_integer_f64;
mod validate_compare_field_expr;
mod validate_compare_selection;

use accumulate_compare_counts::{CompareAccumulation, accumulate_compare_counts};
use apply_metric_filter::apply_metric_filter;
use apply_rank::apply_rank;
use assemble_compare_response::assemble_compare_response;
use assemble_metrics_response::assemble_metrics_response;
pub(crate) use assemble_search_response::assemble_search_response;
use attr_value_display::attr_value_display;
use block_attr_keys::BLOCK_ATTR_KEYS;
use block_attr_value::BLOCK_ATTR_VALUE;
use block_attr_value_bool::BLOCK_ATTR_VALUE_BOOL;
use block_attr_value_double::BLOCK_ATTR_VALUE_DOUBLE;
use block_attr_value_int::BLOCK_ATTR_VALUE_INT;
use block_attr_values_for_key::block_attr_values_for_key;
use block_row_attrs::block_row_attrs;
pub use block_row_attrs_where::block_row_attrs_where;
use block_row_scoped_attrs::block_row_scoped_attrs;
use bool_attr_values::bool_attr_values;
use bool_cmp::bool_cmp;
use build_compare_series::build_compare_series;
pub use bytes_to_hex::bytes_to_hex;
use collect_field_expr_regexes::collect_field_expr_regexes;
use collect_planned_batches::collect_planned_batches;
use collect_selection_regexes::collect_selection_regexes;
use compare_attr_values_match::compare_attr_values_match;
use compare_by_attr::CompareByAttr;
use compare_comparison_matches::compare_comparison_matches;
use compare_counts::{CompareAttributeKey, CompareCountKey, CompareCounts};
use compare_field_class::{CompareFieldClass, compare_field_class};
use compare_field_present::compare_field_present;
use compare_group::CompareGroup;
use compare_group_for_row::compare_group_for_row;
use compare_intrinsic_matches::compare_intrinsic_matches;
use compare_intrinsic_present::compare_intrinsic_present;
use compare_points::compare_points;
use compare_regex_cache::CompareRegexCache;
pub(crate) use compare_row::{CompareRow, DisplayAttribute, compare_row};
use compare_row_attr_values::compare_row_attr_values;
use compare_row_in_selection_window::compare_row_in_selection_window;
use compare_span_identities::{SpanIdentity, compare_span_identities};
use compare_spec::CompareSpec;
use compare_total_series::compare_total_series;
use compare_totals::CompareTotals;
use compare_value_match::compare_value_match;
use compare_value_series::compare_value_series;
use count_over_time_plan::count_over_time_plan;
use deduplicate_search_spans::deduplicate_search_spans;
pub use engine_opts::EngineOpts;
use enum_cmp::enum_cmp;
use extend_metric_projection_matchers::extend_metric_projection_matchers;
use f64_attr_values::f64_attr_values;
use f64_from_i64::f64_from_i64;
use f64_from_u64::f64_from_u64;
use f64_from_usize::f64_from_usize;
use field_expr_matches_row::field_expr_matches_row;
use fixed_8::fixed_8;
use fixed_16::fixed_16;
use float_cmp::float_cmp;
use hinted_max_exemplars::hinted_max_exemplars;
use histogram_points::histogram_points;
use histogram_series_for_group::histogram_series_for_group;
use i32_value::i32_value;
use i64_attr_values::i64_attr_values;
use i64_value::i64_value;
use is_inert_metric_stage::is_inert_metric_stage;
pub(crate) use kind_enum_name::kind_enum_name;
use meta_type_key::META_TYPE_KEY;
use metric_bucket::MetricBucket;
use metric_exemplar::metric_exemplar;
use metric_exemplars::metric_exemplars;
use metric_field_column::metric_field_column;
use metric_field_number::{MetricFieldNumber, metric_field_number};
use metric_filter::{MetricFilter, metric_filter};
use metric_filter_passes::metric_filter_passes;
use metric_function::MetricFunction;
use metric_label_key::metric_label_key;
use metric_label_value::metric_label_value;
use metric_labels::{MetricLabels, metric_labels};
use metric_nested_projection_matchers::metric_nested_projection_matchers;
use metric_numeric_value::metric_numeric_value;
use metric_pipeline_parts::metric_pipeline_parts;
use metric_plan::{MetricPlan, metric_plan};
use metric_plan_for::metric_plan_for;
use metric_plan_with_compare::metric_plan_with_compare;
use metric_series_for_group::metric_series_for_group;
use metric_timing::{ExemplarRange, MetricExemplarPolicy, MetricGrid};
use metrics_range::MetricsRange;
use num_cmp::num_cmp;
use optional_fixed_8::optional_fixed_8;
use optional_list_column::optional_list_column;
use push_scoped_attr::push_scoped_attr;
use quantile_label::quantile_label;
use rank_limit::rank_limit;
use resource_attr_prefix::RESOURCE_ATTR_PREFIX;
use row_attr_values::row_attr_values;
use row_attrs::row_attrs;
use sample_exemplar_rows::sample_exemplar_rows;
pub use search_options::SearchOptions;
use span_start_column::span_start_column;
use spanset_matches_row::spanset_matches_row;
pub(crate) use status_enum_name::status_enum_name;
use string_array_value::string_array_value;
use string_attr_values::string_attr_values;
use string_cmp::string_cmp;
use string_value::string_value;
use trace_acc::TraceAcc;
pub use traceql_engine::TraceqlEngine;
use u64_from_i64::u64_from_i64;
use unsupported_metric_pipeline::unsupported_metric_pipeline;
use usize_from_integer_f64::usize_from_integer_f64;
use validate_compare_field_expr::validate_compare_field_expr;
use validate_compare_selection::validate_compare_selection;

use crate::span_enum_codes::{kind_enum_value, status_enum_value};

mod field_comparison;
pub(crate) use field_comparison::field_comparison_column_values;

mod metric_log2_bucket_value;
use metric_log2_bucket_value::metric_log2_bucket_value;

mod metric_duration_label;
pub(crate) use metric_duration_label::metric_duration_label;

mod scalar_expression_value;
pub(crate) use field_comparison::{field_values, scalar_matches};
pub(crate) use scalar_expression_value::{evaluate_typed_predicate, scalar_expression_value};
