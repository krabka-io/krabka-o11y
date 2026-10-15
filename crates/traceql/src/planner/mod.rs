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

macro_rules! rank_pipeline {
    ($rank:ident) => {
        $rank @ (Pipeline::TopK(_) | Pipeline::BottomK(_))
    };
}

macro_rules! numeric_aggregate {
    ($aggregate:ident) => {
        $aggregate @ (Aggregate::Sum(_) | Aggregate::Avg(_) | Aggregate::Min(_) | Aggregate::Max(_))
    };
}

// Rank Direction
#[derive(Clone, Copy)]
pub(crate) enum RankDirection {
    Top,
    Bottom,
}

// Rank Filter
#[derive(Clone, Copy)]
pub(crate) struct RankFilter {
    pub(crate) op: ComparisonOp,
    pub(crate) value: f64,
}

impl RankFilter {
    pub(crate) fn new(op: ComparisonOp, value: f64) -> Self {
        Self { op, value }
    }
}

// Rank Limit
#[derive(Clone, Copy)]
pub(crate) struct RankLimit {
    pub(crate) direction: RankDirection,
    pub(crate) k: usize,
}

/// The direction and size of a `topk` or `bottomk` stage, or `None` for any
/// other pipeline stage.
pub(crate) fn rank_stage_limit(pipeline: &Pipeline) -> Option<RankLimit> {
    match pipeline {
        Pipeline::TopK(k) => Some(RankLimit {
            direction: RankDirection::Top,
            k: *k,
        }),
        Pipeline::BottomK(k) => Some(RankLimit {
            direction: RankDirection::Bottom,
            k: *k,
        }),
        _ => None,
    }
}

pub(crate) fn rank_limit(pipeline: &Pipeline) -> Result<RankLimit> {
    rank_stage_limit(pipeline).ok_or_else(|| {
        TraceqlError::Unsupported(format!("expected topk/bottomk, got {pipeline:?}"))
    })
}

// Ungrouped Rank Parts
pub(crate) struct UngroupedRankParts<'a> {
    pub(crate) aggregate: &'a Aggregate,
    pub(crate) rank: &'a Pipeline,
    pub(crate) filter: Option<RankFilter>,
}

// Ungrouped Rank Pipeline Parts
pub(crate) fn ungrouped_rank_pipeline_parts(
    pipeline: &[Pipeline],
) -> Option<UngroupedRankParts<'_>> {
    match pipeline {
        [Pipeline::Aggregate(agg), rank_pipeline!(rank)] => Some(UngroupedRankParts {
            aggregate: agg,
            rank,
            filter: None,
        }),
        [
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank_pipeline!(rank),
        ]
        | [
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
            Pipeline::Filter { op, value },
        ] => Some(UngroupedRankParts {
            aggregate: agg,
            rank,
            filter: Some(RankFilter::new(*op, *value)),
        }),
        _ => None,
    }
}

// Ungrouped Rank Pipeline Sql
pub(crate) fn ungrouped_rank_pipeline_sql(
    spanset_sql: &str,
    pipeline: &[Pipeline],
) -> Result<Option<String>> {
    let Some(parts) = ungrouped_rank_pipeline_parts(pipeline) else {
        return Ok(None);
    };
    if !is_search_preserving_aggregate(parts.aggregate) {
        return Ok(None);
    }
    let rank = rank_limit(parts.rank)?;
    if rank.k == 0 {
        return Ok(Some(ungrouped_rank_sql(spanset_sql, rank)));
    }
    let Some(filter) = parts.filter else {
        return Ok(Some(ungrouped_rank_sql(spanset_sql, rank)));
    };
    Ok(Some(aggregate_filter_sql_query_any(
        spanset_sql,
        parts.aggregate,
        filter.op,
        filter.value,
    )?))
}

// Ungrouped Rank Sql
pub(crate) fn ungrouped_rank_sql(spanset_sql: &str, rank: RankLimit) -> String {
    if rank.k == 0 {
        return format!("SELECT * FROM ({spanset_sql}) AS q WHERE FALSE");
    }
    format!("SELECT * FROM ({spanset_sql}) AS q")
}

// Grouped Rank Pipeline Parts
pub(crate) struct GroupedRankPipelineParts<'a> {
    pub(crate) aggregate: &'a Aggregate,
    pub(crate) by: &'a [Field],
    pub(crate) rank: &'a Pipeline,
    pub(crate) pre_filter: Option<RankFilter>,
    pub(crate) post_filter: Option<RankFilter>,
}

pub(crate) fn grouped_rank_pipeline_parts(
    pipeline: &[Pipeline],
) -> Option<GroupedRankPipelineParts<'_>> {
    match pipeline {
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            rank_pipeline!(rank),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
        ]
        | [
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
            Pipeline::By(by),
        ] => Some(GroupedRankPipelineParts {
            aggregate: agg,
            by,
            rank,
            pre_filter: None,
            post_filter: None,
        }),
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
            rank_pipeline!(rank),
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank_pipeline!(rank),
        ]
        | [
            Pipeline::Aggregate(agg),
            Pipeline::Filter { op, value },
            rank_pipeline!(rank),
            Pipeline::By(by),
        ] => Some(GroupedRankPipelineParts {
            aggregate: agg,
            by,
            rank,
            pre_filter: Some(RankFilter::new(*op, *value)),
            post_filter: None,
        }),
        [
            Pipeline::Aggregate(agg),
            Pipeline::By(by),
            rank_pipeline!(rank),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::By(by),
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
            Pipeline::By(by),
            Pipeline::Filter { op, value },
        ]
        | [
            Pipeline::Aggregate(agg),
            rank_pipeline!(rank),
            Pipeline::Filter { op, value },
            Pipeline::By(by),
        ] => Some(GroupedRankPipelineParts {
            aggregate: agg,
            by,
            rank,
            pre_filter: None,
            post_filter: Some(RankFilter::new(*op, *value)),
        }),
        _ => None,
    }
}

// Grouped Rank Pipeline Sql
pub(crate) fn grouped_rank_pipeline_sql(
    spanset_sql: &str,
    pipeline: &[Pipeline],
) -> Result<Option<String>> {
    let Some(parts) = grouped_rank_pipeline_parts(pipeline) else {
        return Ok(None);
    };
    if !is_search_preserving_aggregate(parts.aggregate) {
        return Ok(None);
    }
    Ok(Some(grouped_rank_sql(
        spanset_sql,
        parts.by,
        &aggregate_rank_expr_sql(parts.aggregate)?,
        rank_limit(parts.rank)?,
        parts.pre_filter,
        parts.post_filter,
    )?))
}

// Grouped Rank Sql
pub(crate) fn grouped_rank_sql(
    spanset_sql: &str,
    by: &[Field],
    expr: &str,
    rank: RankLimit,
    pre_filter: Option<RankFilter>,
    post_filter: Option<RankFilter>,
) -> Result<String> {
    let GroupJoinSql {
        group_exprs,
        join_pred,
    } = group_join_sql(by);
    let direction = match rank.direction {
        RankDirection::Top => "DESC",
        RankDirection::Bottom => "ASC",
    };
    let having = if let Some(filter) = pre_filter {
        format!(
            " HAVING {}",
            aggregate_filter_sql(expr, filter.op, filter.value)?
        )
    } else {
        String::new()
    };
    let passing_source = if let Some(filter) = post_filter {
        let pred = aggregate_filter_sql("rank_value", filter.op, filter.value)?;
        format!("SELECT * FROM ranked WHERE {pred}")
    } else {
        "SELECT * FROM ranked".to_string()
    };
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         ranked AS (SELECT {group_exprs}, {expr} AS rank_value FROM matched GROUP BY {group_exprs} \
                    {having} ORDER BY rank_value {direction} LIMIT {}), \
         passing AS ({passing_source}) \
         SELECT matched.* FROM matched JOIN passing ON {join_pred}",
        rank.k
    ))
}

// Aggregate Rank Expr Sql
pub(crate) fn aggregate_rank_expr_sql(agg: &Aggregate) -> Result<String> {
    match agg {
        Aggregate::Count => Ok("COUNT(*)".to_string()),
        Aggregate::Sum(_) | Aggregate::Avg(_) | Aggregate::Min(_) | Aggregate::Max(_) => {
            aggregate_expr_sql(agg)
        }
        _ => Err(TraceqlError::Unsupported(format!(
            "aggregate {agg:?} is not supported in search ranking"
        ))),
    }
}

// Structural Base Op
pub(crate) fn structural_base_op(op: StructuralOp) -> StructuralOp {
    match op {
        StructuralOp::NegDescendant | StructuralOp::UnionDescendant => StructuralOp::Descendant,
        StructuralOp::NegAncestor | StructuralOp::UnionAncestor => StructuralOp::Ancestor,
        StructuralOp::NegChild | StructuralOp::UnionChild => StructuralOp::Child,
        StructuralOp::NegParent | StructuralOp::UnionParent => StructuralOp::Parent,
        StructuralOp::NegSibling | StructuralOp::UnionSibling => StructuralOp::Sibling,
        StructuralOp::Descendant
        | StructuralOp::Ancestor
        | StructuralOp::Child
        | StructuralOp::Parent
        | StructuralOp::Sibling => op,
    }
}

// Structural Is Negated
pub(crate) fn structural_is_negated(op: StructuralOp) -> bool {
    matches!(
        op,
        StructuralOp::NegDescendant
            | StructuralOp::NegAncestor
            | StructuralOp::NegChild
            | StructuralOp::NegParent
            | StructuralOp::NegSibling
    )
}

// Structural Is Union
pub(crate) fn structural_is_union(op: StructuralOp) -> bool {
    matches!(
        op,
        StructuralOp::UnionDescendant
            | StructuralOp::UnionAncestor
            | StructuralOp::UnionChild
            | StructuralOp::UnionParent
            | StructuralOp::UnionSibling
    )
}

// Structural Predicate Sql
pub(crate) fn structural_predicate_sql(op: StructuralOp) -> String {
    let trace = selector::ident(COL_TRACE_ID);
    let left = selector::ident(COL_NS_LEFT);
    let right = selector::ident(COL_NS_RIGHT);
    let parent = selector::ident(COL_PARENT_ID);
    let span_id = selector::ident(COL_SPAN_ID);
    let trace_eq = format!("b.{trace} = a.{trace}");
    match op {
        StructuralOp::Descendant => {
            format!("{trace_eq} AND b.{left} > a.{left} AND b.{right} < a.{right}")
        }
        StructuralOp::Ancestor => {
            format!("{trace_eq} AND b.{left} < a.{left} AND b.{right} > a.{right}")
        }
        StructuralOp::Child => format!("{trace_eq} AND b.{parent} = a.{left}"),
        StructuralOp::Parent => format!("{trace_eq} AND a.{parent} = b.{left}"),
        StructuralOp::Sibling => {
            format!("{trace_eq} AND b.{parent} = a.{parent} AND b.{span_id} != a.{span_id}")
        }
        StructuralOp::NegDescendant
        | StructuralOp::NegAncestor
        | StructuralOp::NegChild
        | StructuralOp::NegParent
        | StructuralOp::NegSibling
        | StructuralOp::UnionDescendant
        | StructuralOp::UnionAncestor
        | StructuralOp::UnionChild
        | StructuralOp::UnionParent
        | StructuralOp::UnionSibling => unreachable!("mode variants are normalized first"),
    }
}

// Is Inert Pipeline Stage
pub(crate) fn is_inert_pipeline_stage(stage: &Pipeline) -> bool {
    matches!(
        stage,
        Pipeline::Select(_) | Pipeline::Coalesce | Pipeline::With(_)
    )
}

// Is Search Preserving Aggregate
pub(crate) fn is_search_preserving_aggregate(agg: &Aggregate) -> bool {
    matches!(
        agg,
        Aggregate::Count
            | Aggregate::Sum(_)
            | Aggregate::Avg(_)
            | Aggregate::Min(_)
            | Aggregate::Max(_)
    )
}

// Is Search Preserving Pipeline Stage
pub(crate) fn is_search_preserving_pipeline_stage(stage: &Pipeline) -> bool {
    matches!(
        stage,
        Pipeline::By(_)
            | Pipeline::Select(_)
            | Pipeline::Coalesce
            | Pipeline::With(_)
            | Pipeline::Aggregate(
                Aggregate::Count
                    | Aggregate::Sum(_)
                    | Aggregate::Avg(_)
                    | Aggregate::Min(_)
                    | Aggregate::Max(_)
            )
    )
}

// Aggregate Expr Sql
pub(crate) fn aggregate_expr_sql(agg: &Aggregate) -> Result<String> {
    let (field, func) = match agg {
        Aggregate::Sum(field) => (field, "SUM"),
        Aggregate::Avg(field) => (field, "AVG"),
        Aggregate::Min(field) => (field, "MIN"),
        Aggregate::Max(field) => (field, "MAX"),
        _ => {
            return Err(TraceqlError::Unsupported(format!(
                "aggregate {agg:?} is not supported in scalar filters"
            )));
        }
    };
    Ok(format!(
        "{func}({})",
        selector::ident(&selector::field_to_column(field))
    ))
}

// Aggregate Filter Sql
pub(crate) fn aggregate_filter_sql(expr: &str, op: ComparisonOp, value: f64) -> Result<String> {
    if !value.is_finite() {
        return Err(TraceqlError::Plan(
            "pipeline filter value is not finite".into(),
        ));
    }
    let op = match op {
        ComparisonOp::Eq => "=",
        ComparisonOp::Neq => "!=",
        ComparisonOp::Lt => "<",
        ComparisonOp::Lte => "<=",
        ComparisonOp::Gt => ">",
        ComparisonOp::Gte => ">=",
        ComparisonOp::Re | ComparisonOp::Nre => {
            return Err(TraceqlError::Unsupported(
                "regex filter on pipeline scalar is not supported".into(),
            ));
        }
    };
    Ok(format!("{expr} {op} {value}"))
}

// Aggregate Filter Sql Query
pub(crate) fn aggregate_filter_sql_query(
    spanset_sql: &str,
    agg: &Aggregate,
    op: ComparisonOp,
    value: f64,
) -> Result<String> {
    aggregate_filter_sql_query_any(spanset_sql, agg, op, value)
}

// Aggregate Filter Sql Query Any
pub(crate) fn aggregate_filter_sql_query_any(
    spanset_sql: &str,
    agg: &Aggregate,
    op: ComparisonOp,
    value: f64,
) -> Result<String> {
    let expr = match agg {
        Aggregate::Count => "COUNT(*)".to_string(),
        _ => aggregate_expr_sql(agg)?,
    };
    aggregate_filter_query_any_sql(spanset_sql, &expr, op, value)
}

// Aggregate Projection Field
pub(crate) fn aggregate_projection_field(agg: &Aggregate) -> Option<&Field> {
    match agg {
        Aggregate::Sum(field)
        | Aggregate::Avg(field)
        | Aggregate::Min(field)
        | Aggregate::Max(field)
        | Aggregate::SumOverTime(field)
        | Aggregate::AvgOverTime(field)
        | Aggregate::MinOverTime(field)
        | Aggregate::MaxOverTime(field)
        | Aggregate::HistogramOverTime(field)
        | Aggregate::QuantileOverTime { field, .. } => Some(field),
        Aggregate::Expression { .. }
        | Aggregate::Count
        | Aggregate::Rate
        | Aggregate::CountOverTime => None,
    }
}

// Group Join Sql
/// The SQL that groups `matched` rows by the `by` fields and joins them back
/// to the `passing` groups.
pub(crate) struct GroupJoinSql {
    /// The comma-separated grouping columns, for `SELECT` and `GROUP BY`.
    pub(crate) group_exprs: String,
    /// The `matched.col = passing.col AND ...` join predicate.
    pub(crate) join_pred: String,
}

pub(crate) fn group_join_sql(by: &[Field]) -> GroupJoinSql {
    let group_cols = by
        .iter()
        .map(|field| selector::ident(&selector::field_to_column(field)))
        .collect::<Vec<_>>();
    let join_pred = group_cols
        .iter()
        .map(|col| format!("matched.{col} = passing.{col}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    GroupJoinSql {
        group_exprs: group_cols.join(", "),
        join_pred,
    }
}

// Collect Nested Selectors
pub(crate) fn collect_nested_selectors(expr: &SpansetExpr, out: &mut Vec<FieldExpr>) {
    match expr {
        SpansetExpr::Selector(fe) if selector::has_nested_scope(fe) => {
            if !out.iter().any(|existing| existing == fe.as_ref()) {
                out.push((**fe).clone());
            }
        }
        SpansetExpr::Selector(_) => {}
        SpansetExpr::And(lhs, rhs)
        | SpansetExpr::Or(lhs, rhs)
        | SpansetExpr::Structural { lhs, rhs, .. } => {
            collect_nested_selectors(lhs, out);
            collect_nested_selectors(rhs, out);
        }
    }
}

// Collect Table
pub(crate) async fn collect_table(ctx: &SessionContext, table: &str) -> Result<Vec<RecordBatch>> {
    Ok(ctx.table(table).await?.collect().await?)
}

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
        result::{AttrValue, Attribute, EventRef, MetricPoint},
        span_columns::{COL_NAME, InputSpan},
        testkit::span::SpanFixture,
    };

    struct QueryExpectedNames {
        query: &'static str,
        expected: Vec<&'static str>,
    }

    struct AggregatePrefixCase {
        aggregate: Aggregate,
        prefix: &'static str,
    }

    /// The fixture span, starting `id` nanoseconds after the epoch rather than
    /// a thousand nanoseconds later.
    fn span_starting_at_id(fixture: SpanFixture<'_>) -> InputSpan {
        let start_unix_nano = i64::from(fixture.id);
        InputSpan {
            start_unix_nano,
            ..fixture.input_span()
        }
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
            span_starting_at_id(SpanFixture {
                id: self.id,
                trace: self.trace,
                name: self.name,
                duration_nanos: self.duration_nanos,
                attrs: vec![Attribute::new("svc", AttrValue::Str(self.svc.into()))],
                ..SpanFixture::default()
            })
        }
    }

    fn store_with_traces(traces: Vec<Vec<InputSpan>>) -> InMemorySpanStore {
        let mut store = InMemorySpanStore::new();
        for spans in traces {
            store.push_trace("t", "svc", "root", spans);
        }
        store
    }

    // One trace of 1ns spans, numbered from 1, each named `<svc>-<suffix>`
    // and carrying the `svc` attribute its name starts with.
    fn prefixed_svc_store(span_names: &[&'static str]) -> InMemorySpanStore {
        let spans = (1..)
            .zip(span_names.iter().copied())
            .map(|indexed_name| {
                let id = indexed_name.0;
                let name = indexed_name.1;
                let split_name = name.split_once('-').expect("span name is `<svc>-<suffix>`");
                let svc = split_name.0;
                SvcSpan {
                    id,
                    name,
                    duration_nanos: 1,
                    svc,
                    ..SvcSpan::default()
                }
                .build()
            })
            .collect();
        store_with_traces(vec![spans])
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
        spans.extend((4..).zip(CACHE_SPAN_NAMES).map(|indexed_name| {
            let id = indexed_name.0;
            let name = indexed_name.1;
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
            vec![
                span_starting_at_id(SpanFixture {
                    id: 1,
                    name: "api-a",
                    duration_nanos: 20,
                    ..SpanFixture::default()
                }),
                span_starting_at_id(SpanFixture {
                    id: 2,
                    name: "api-b",
                    duration_nanos: 40,
                    ..SpanFixture::default()
                }),
            ],
            vec![span_starting_at_id(SpanFixture {
                id: 3,
                trace: 2,
                name: "db-a",
                duration_nanos: 200,
                ..SpanFixture::default()
            })],
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
                span_starting_at_id(SpanFixture {
                    id: 1,
                    name: "equal",
                    duration_nanos: 10,
                    attrs: vec![
                        Attribute::new("lhs", AttrValue::Int(3)),
                        Attribute::new("rhs", AttrValue::Int(3)),
                    ],
                    ..SpanFixture::default()
                }),
                span_starting_at_id(SpanFixture {
                    id: 2,
                    name: "different",
                    duration_nanos: 20,
                    attrs: vec![
                        Attribute::new("lhs", AttrValue::Int(4)),
                        Attribute::new("rhs", AttrValue::Int(3)),
                    ],
                    ..SpanFixture::default()
                }),
                span_starting_at_id(SpanFixture {
                    id: 3,
                    name: "missing",
                    duration_nanos: 30,
                    attrs: vec![Attribute::new("lhs", AttrValue::Int(3))],
                    ..SpanFixture::default()
                }),
                span_starting_at_id(SpanFixture {
                    id: 4,
                    name: "wrong-type",
                    duration_nanos: 40,
                    attrs: vec![
                        Attribute::new("text", AttrValue::Str("3".into())),
                        Attribute::new("rhs", AttrValue::Int(3)),
                    ],
                    ..SpanFixture::default()
                }),
            ],
        );
        for case in [
            QueryExpectedNames {
                query: "{ .lhs = .rhs }",
                expected: vec!["equal"],
            },
            QueryExpectedNames {
                query: "{ .lhs > .rhs }",
                expected: vec!["different"],
            },
            QueryExpectedNames {
                query: "{ .lhs != .rhs }",
                expected: vec!["different"],
            },
            QueryExpectedNames {
                query: "{ .text = .rhs }",
                expected: vec![],
            },
            QueryExpectedNames {
                query: "{ name = \"equal\" } && { .lhs = .rhs }",
                expected: vec!["equal"],
            },
            QueryExpectedNames {
                query: "{ .lhs = .rhs || name = \"wrong-type\" }",
                expected: vec!["equal", "wrong-type"],
            },
        ] {
            assert!(names(&planned(case.query, &store).await.unwrap()) == case.expected);
        }
        let engine = crate::TraceqlEngine::new(Arc::new(store), crate::EngineOpts::default());
        let metrics = engine
            .query_range("t", "{ .lhs = .rhs } | count_over_time()", 0, 100, 100)
            .await
            .unwrap();
        assert!(
            metrics.series[0].points == vec![MetricPoint::new(0, 1.0), MetricPoint::new(100, 0.0)]
        );
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
        for case in [
            AggregatePrefixCase {
                aggregate: Aggregate::Sum(field.clone()),
                prefix: "SUM(",
            },
            AggregatePrefixCase {
                aggregate: Aggregate::Avg(field.clone()),
                prefix: "AVG(",
            },
            AggregatePrefixCase {
                aggregate: Aggregate::Min(field.clone()),
                prefix: "MIN(",
            },
            AggregatePrefixCase {
                aggregate: Aggregate::Max(field),
                prefix: "MAX(",
            },
        ] {
            assert!(
                aggregate_expr_sql(&case.aggregate)
                    .unwrap()
                    .starts_with(case.prefix)
            );
            assert!(
                aggregate_rank_expr_sql(&case.aggregate)
                    .unwrap()
                    .starts_with(case.prefix)
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
                span_starting_at_id(SpanFixture {
                    id: 1,
                    name: "root",
                    duration_nanos: 50,
                    attrs: vec![Attribute::new("http.method", AttrValue::Str("GET".into()))],
                    ..SpanFixture::default()
                }),
                span_starting_at_id(SpanFixture {
                    id: 2,
                    name: "db",
                    duration_nanos: 50,
                    attrs: vec![Attribute::new("http.method", AttrValue::Str("POST".into()))],
                    ..SpanFixture::default()
                }),
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
            vec![
                span_starting_at_id(SpanFixture {
                    id: 1,
                    name: "short",
                    duration_nanos: 50,
                    ..SpanFixture::default()
                }),
                span_starting_at_id(SpanFixture {
                    id: 2,
                    name: "long",
                    duration_nanos: 150,
                    ..SpanFixture::default()
                }),
            ],
        );
        let out = planned("{ span:duration > 100 }", &store).await.unwrap();
        assert!(out.iter().map(RecordBatch::num_rows).sum::<usize>() == 1);
        assert!(first_name(&out) == "long");
    }

    #[tokio::test]
    async fn grouped_pipeline_filters_by_nested_event_intrinsic() {
        let mut miss_one = span_starting_at_id(SpanFixture {
            id: 1,
            name: "miss-one",
            duration_nanos: 50,
            ..SpanFixture::default()
        });
        miss_one.events = vec![EventRef {
            time_since_start: nanos(10),
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut miss_two = span_starting_at_id(SpanFixture {
            id: 2,
            name: "miss-two",
            duration_nanos: 50,
            ..SpanFixture::default()
        });
        miss_two.events = vec![EventRef {
            time_since_start: nanos(20),
            name: "cache.miss".into(),
            attributes: Vec::new(),
        }];
        let mut hit = span_starting_at_id(SpanFixture {
            id: 3,
            name: "hit",
            duration_nanos: 50,
            ..SpanFixture::default()
        });
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
        let mut one = SvcSpan {
            id: 1,
            name: "one",
            duration_nanos: 50,
            svc: "api",
            ..SvcSpan::default()
        }
        .build();
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
        let mut two = SvcSpan {
            id: 2,
            name: "two",
            duration_nanos: 50,
            svc: "api",
            ..SvcSpan::default()
        }
        .build();
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
                span_starting_at_id(SpanFixture {
                    id: 1,
                    name: "a-only",
                    duration_nanos: 50,
                    attrs: vec![Attribute::new("a", AttrValue::Int(1))],
                    ..SpanFixture::default()
                }),
                span_starting_at_id(SpanFixture {
                    id: 2,
                    name: "b-only",
                    duration_nanos: 50,
                    attrs: vec![Attribute::new("b", AttrValue::Int(2))],
                    ..SpanFixture::default()
                }),
                span_starting_at_id(SpanFixture {
                    id: 3,
                    name: "both",
                    duration_nanos: 50,
                    attrs: vec![
                        Attribute::new("a", AttrValue::Int(1)),
                        Attribute::new("b", AttrValue::Int(2)),
                    ],
                    ..SpanFixture::default()
                }),
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
                span_starting_at_id(SpanFixture {
                    id: 1,
                    name: "one",
                    duration_nanos: 50,
                    attrs: vec![Attribute::new("name", AttrValue::Str("abc".into()))],
                    ..SpanFixture::default()
                }),
                span_starting_at_id(SpanFixture {
                    id: 2,
                    name: "two",
                    duration_nanos: 50,
                    attrs: vec![Attribute::new("name", AttrValue::Str("xabc".into()))],
                    ..SpanFixture::default()
                }),
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
                span_starting_at_id(SpanFixture {
                    id: 1,
                    trace: 1,
                    name: "a-only",
                    duration_nanos: 50,
                    attrs: vec![Attribute::new("a", AttrValue::Int(1))],
                    ..SpanFixture::default()
                }),
                span_starting_at_id(SpanFixture {
                    id: 2,
                    trace: 1,
                    name: "b-only",
                    duration_nanos: 50,
                    attrs: vec![Attribute::new("b", AttrValue::Int(2))],
                    ..SpanFixture::default()
                }),
            ],
        );
        store.push_trace(
            "t",
            "svc",
            "root",
            vec![span_starting_at_id(SpanFixture {
                id: 3,
                trace: 2,
                name: "other-a",
                duration_nanos: 50,
                attrs: vec![Attribute::new("a", AttrValue::Int(1))],
                ..SpanFixture::default()
            })],
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
                SvcSpan {
                    id: 1,
                    trace: 9,
                    name: "root",
                    duration_nanos: 1,
                    svc: "a",
                }
                .build(),
                span_starting_at_id(SpanFixture {
                    id: 2,
                    parent: Some(1),
                    trace: 9,
                    name: "child-x",
                    duration_nanos: 1,
                    attrs: vec![Attribute::new("svc", AttrValue::Str("b".into()))],
                }),
                span_starting_at_id(SpanFixture {
                    id: 4,
                    parent: Some(2),
                    trace: 9,
                    name: "grand-y",
                    duration_nanos: 1,
                    attrs: vec![Attribute::new("svc", AttrValue::Str("c".into()))],
                }),
                span_starting_at_id(SpanFixture {
                    id: 3,
                    parent: Some(1),
                    trace: 9,
                    name: "child-z",
                    duration_nanos: 1,
                    attrs: vec![Attribute::new("svc", AttrValue::Str("b".into()))],
                }),
            ],
        );
        store.push_trace(
            "t",
            "svc",
            "other-root",
            vec![
                SvcSpan {
                    id: 5,
                    trace: 8,
                    name: "other-root",
                    duration_nanos: 1,
                    svc: "a",
                }
                .build(),
                span_starting_at_id(SpanFixture {
                    id: 6,
                    parent: Some(5),
                    trace: 8,
                    name: "other-child",
                    duration_nanos: 1,
                    attrs: vec![Attribute::new("svc", AttrValue::Str("d".into()))],
                }),
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
        let store = prefixed_svc_store(&["api-a", "api-b", "db-a"]);
        let out = planned("{ .svc != nil } | count() | by(span.svc) > 1", &store)
            .await
            .unwrap();
        assert!(names(&out) == vec!["api-a".to_string(), "api-b".to_string()]);
    }

    #[tokio::test]
    async fn count_filter_accepts_literal_arithmetic_threshold() {
        let store = prefixed_svc_store(&["api-a", "api-b", "api-c", "db-a"]);

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
                SvcSpan {
                    id: 1,
                    trace: 1,
                    name: "api-a",
                    duration_nanos: 1,
                    svc: "api",
                }
                .build(),
                SvcSpan {
                    id: 2,
                    trace: 1,
                    name: "api-b",
                    duration_nanos: 1,
                    svc: "api",
                }
                .build(),
                SvcSpan {
                    id: 3,
                    trace: 2,
                    name: "db-a",
                    duration_nanos: 1,
                    svc: "db",
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
                    name: "db-a",
                    duration_nanos: 40,
                    svc: "db",
                    ..SvcSpan::default()
                }
                .build(),
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
        let mut spans = api_db_spans();
        spans.push(
            SvcSpan {
                id: 4,
                name: "db-b",
                duration_nanos: 400,
                svc: "db",
                ..SvcSpan::default()
            }
            .build(),
        );
        let store = store_with_traces(vec![spans]);

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

mod grouped_sql;
pub(crate) mod nested_projection_matcher;
mod pipeline_sql;
mod plan_query;
mod planned_spanset;
mod planner_context;
mod registration;
mod spanset_sql;

fn aggregate_filter_query_any_sql(
    spanset_sql: &str,
    expression: &str,
    op: ComparisonOp,
    value: f64,
) -> Result<String> {
    let trace = selector::ident(COL_TRACE_ID);
    let pred = aggregate_filter_sql(expression, op, value)?;
    Ok(format!(
        "WITH matched AS ({spanset_sql}), \
         passing AS (SELECT {trace} FROM matched GROUP BY {trace} HAVING {pred}) \
         SELECT matched.* FROM matched JOIN passing ON matched.{trace} = passing.{trace}"
    ))
}
use grouped_sql::{grouped_aggregate_sql, grouped_no_filter_by, grouped_pipeline_sql};
use nested_projection_matcher::nested_projection_matcher;
use pipeline_sql::{
    pipeline_nested_projection_matchers, pipeline_to_sql, scan_options_with_pipeline_projections,
};
pub(crate) use plan_query::plan_query;
pub(crate) use planned_spanset::PlannedSpanset;
pub(crate) use planner_context::PlannerContext;
use registration::{
    register_batches, register_field_comparison_columns, register_nested_selector_tables,
};
use spanset_sql::{plan_spanset_sql, spanset_to_sql};

mod apply_expression_pipeline;

mod expression_name;

mod sample_metric_scan;
