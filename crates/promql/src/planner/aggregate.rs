//! `LogicalPlan` lowering for the simple `PromQL` aggregations.
//!
//! The aggregations are `sum | avg | min | max | count | group` with
//! `by(...)`/`without(...)`.
//!
//! The recursive instant planner [`crate::engine`] hands this module an inner
//! [`LogicalPlan`] whose output carries one row for each input series. That row
//! holds a set of `Utf8` label columns, a `Float64` `value` column, and, for the
//! instant-selector shape, `timestamp`/`sample_timestamp` index columns. This
//! module wraps that input in a `DataFusion`
//! [`Aggregate`](LogicalPlan::Aggregate) that collapses the rows into per-group
//! results. The aggregate maps Prometheus grouping semantics onto GROUP BY:
//!
//! - `by (l...)` groups by exactly the listed label columns that are present in
//!   the input. A `by` label absent from every input series does not appear.
//!   Prometheus does the same and drops empty grouping labels.
//! - `without (l...)` groups by every input label column except the listed
//!   ones and except `__name__`. Prometheus `without` always drops the metric
//!   name.
//! - `by ()` collapses all series into a single group.
//!
//! Most per-op value aggregates use `DataFusion`'s built-in aggregate
//! expressions, which match Prometheus float semantics exactly. This includes
//! NaN propagation for `sum`/`avg`. Those aggregates are `sum`, `avg`, `count`
//! cast to `Float64`, and `group` as the constant `1.0`. `min`/`max` are the
//! exception: Arrow's built-in `min`/`max` order floats with `total_cmp` and so
//! propagate NaN, but Prometheus and the tree-walking interpreter ignore NaN.
//!
//! A group's extremum is over its non-NaN samples, and the result is NaN only
//! when every sample is NaN. So `min`/`max` lower onto the NaN-ignoring
//! [`prom_min_udaf`]/[`prom_max_udaf`] UDAFs instead. The result columns are the
//! grouping label columns plus the aggregated `value` column. The caller
//! reattaches the eval timestamp during result assembly.

use std::collections::BTreeSet;

use datafusion::{
    functions_aggregate::expr_fn::{avg, count, max, sum},
    logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, cast, col, lit},
};

use crate::{
    PromqlError,
    error::Result,
    functions::{prom_max_udaf, prom_min_udaf},
    planner::leaf::{SAMPLE_TIME_COLUMN, TIME_COLUMN, VALUE_COLUMN},
};

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{Float64Array, Int64Array, StringArray},
        datatypes::{DataType, Field, Schema},
        record_batch::RecordBatch,
    };
    use datafusion::{catalog::MemTable, prelude::SessionContext};

    use super::*;

    /// Builds a leaf plan over an in-memory table like an instant-selector output.
    ///
    /// The table has the `job` and `group` labels plus
    /// `timestamp`/`value`/`sample_timestamp`.
    async fn selector_like_leaf(ctx: &SessionContext, rows: &[(&str, &str, f64)]) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, false),
            Field::new("job", DataType::Utf8, false),
            Field::new(TIME_COLUMN, DataType::Int64, false),
            Field::new(VALUE_COLUMN, DataType::Float64, false),
            Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false),
        ]));
        let groups = StringArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>());
        let jobs = StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>());
        let ts = Int64Array::from(rows.iter().map(|_| 0_i64).collect::<Vec<_>>());
        let value = Float64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>());
        let sample_ts = Int64Array::from(rows.iter().map(|_| 0_i64).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(groups),
                Arc::new(jobs),
                Arc::new(ts),
                Arc::new(value),
                Arc::new(sample_ts),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("agg_leaf", Arc::new(table)).unwrap();
        ctx.table("agg_leaf")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap()
    }

    /// Like [`selector_like_leaf`], but with a nullable `value` column.
    ///
    /// A `None` row models the "no value" NULL cell of a rate or `*_over_time`
    /// UDF. This drives the pre-aggregate NULL filter: the planner must drop
    /// such rows before grouping, exactly as the interpreter omits no-value
    /// series.
    async fn nullable_leaf(
        ctx: &SessionContext,
        rows: &[(&str, &str, Option<f64>)],
    ) -> LogicalPlan {
        let schema = Arc::new(Schema::new(vec![
            Field::new("group", DataType::Utf8, false),
            Field::new("job", DataType::Utf8, false),
            Field::new(TIME_COLUMN, DataType::Int64, false),
            Field::new(VALUE_COLUMN, DataType::Float64, true),
            Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false),
        ]));
        let groups = StringArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>());
        let jobs = StringArray::from(rows.iter().map(|r| r.1).collect::<Vec<_>>());
        let ts = Int64Array::from(rows.iter().map(|_| 0_i64).collect::<Vec<_>>());
        let value = Float64Array::from(rows.iter().map(|r| r.2).collect::<Vec<_>>());
        let sample_ts = Int64Array::from(rows.iter().map(|_| 0_i64).collect::<Vec<_>>());
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(groups),
                Arc::new(jobs),
                Arc::new(ts),
                Arc::new(value),
                Arc::new(sample_ts),
            ],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("agg_leaf", Arc::new(table)).unwrap();
        ctx.table("agg_leaf")
            .await
            .unwrap()
            .into_optimized_plan()
            .unwrap()
    }

    async fn run(plan: LogicalPlan, ctx: &SessionContext) -> Vec<(Vec<(String, String)>, f64)> {
        let batches = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut out = Vec::new();
        for batch in &batches {
            let value = batch
                .column_by_name(AGGREGATE_VALUE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                let mut labels = Vec::new();
                for (index, field) in batch.schema().fields().iter().enumerate() {
                    if field.name() == AGGREGATE_VALUE_COLUMN {
                        continue;
                    }
                    let column = batch
                        .column(index)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap();
                    labels.push((field.name().clone(), column.value(row).to_string()));
                }
                labels.sort();
                out.push((labels, value.value(row)));
            }
        }
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    #[tokio::test]
    async fn sum_by_collapses_to_group_labels() {
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(
            &ctx,
            &[
                ("prod", "api", 1.0),
                ("prod", "db", 2.0),
                ("canary", "api", 4.0),
            ],
        )
        .await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(
            got == vec![
                (vec![("group".to_string(), "canary".to_string())], 4.0),
                (vec![("group".to_string(), "prod".to_string())], 3.0),
            ]
        );
    }

    #[tokio::test]
    async fn sum_without_drops_listed_and_name() {
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(
            &ctx,
            &[
                ("prod", "api", 1.0),
                ("prod", "db", 2.0),
                ("canary", "api", 4.0),
            ],
        )
        .await;
        // without (job) -> group by `group`.
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::Without(vec!["job".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(
            got == vec![
                (vec![("group".to_string(), "canary".to_string())], 4.0),
                (vec![("group".to_string(), "prod".to_string())], 3.0),
            ]
        );
    }

    #[tokio::test]
    async fn sum_by_empty_collapses_all() {
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(
            &ctx,
            &[
                ("prod", "api", 1.0),
                ("prod", "db", 2.0),
                ("canary", "api", 4.0),
            ],
        )
        .await;
        let plan =
            plan_simple_aggregate(leaf, SimpleAggregateOp::Sum, &Grouping::By(vec![])).unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(got == vec![(vec![], 7.0)]);
    }

    #[tokio::test]
    async fn count_and_group_yield_floats() {
        let cases = [
            (
                SimpleAggregateOp::Count,
                [
                    ("prod", "api", 1.0),
                    ("prod", "db", 2.0),
                    ("canary", "api", 4.0),
                ],
                vec![
                    (vec![("group".to_string(), "canary".to_string())], 1.0),
                    (vec![("group".to_string(), "prod".to_string())], 2.0),
                ],
            ),
            (
                SimpleAggregateOp::Group,
                [
                    ("prod", "api", 9.0),
                    ("prod", "db", 2.0),
                    ("canary", "api", 4.0),
                ],
                vec![
                    (vec![("group".to_string(), "canary".to_string())], 1.0),
                    (vec![("group".to_string(), "prod".to_string())], 1.0),
                ],
            ),
        ];
        for (op, rows, want) in cases {
            let ctx = SessionContext::new();
            let leaf = selector_like_leaf(&ctx, &rows).await;
            let plan =
                plan_simple_aggregate(leaf, op, &Grouping::By(vec!["group".into()])).unwrap();
            let got = run(plan, &ctx).await;
            assert2::assert!(got == want);
        }
    }

    #[tokio::test]
    async fn empty_input_by_empty_yields_no_group() {
        // `sum by ()` over zero input rows must yield zero groups (Prometheus
        // empty vector), not SQL's single global-aggregate row.
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(&ctx, &[]).await;
        let plan =
            plan_simple_aggregate(leaf, SimpleAggregateOp::Sum, &Grouping::By(vec![])).unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(got.is_empty());
    }

    #[tokio::test]
    async fn empty_input_by_label_yields_no_group() {
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(&ctx, &[]).await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Count,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(got.is_empty());
    }

    #[tokio::test]
    async fn sum_propagates_nan() {
        let ctx = SessionContext::new();
        let leaf =
            selector_like_leaf(&ctx, &[("prod", "api", 1.0), ("prod", "db", f64::NAN)]).await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(got.len() == 1);
        assert2::assert!(got[0].1.is_nan());
    }

    #[tokio::test]
    async fn min_max_ignore_nan_over_mixed_group() {
        // A group mixing genuine NaN with finite samples: Prometheus takes the
        // extremum over the non-NaN values (NaN ignored), unlike Arrow's built-in
        // min/max which propagate NaN.
        for (op, want) in [
            (SimpleAggregateOp::Min, 1.0_f64),
            (SimpleAggregateOp::Max, 3.0_f64),
        ] {
            let ctx = SessionContext::new();
            let leaf = selector_like_leaf(
                &ctx,
                &[
                    ("prod", "api", f64::NAN),
                    ("prod", "db", 3.0),
                    ("prod", "x", 1.0),
                    ("prod", "y", f64::NAN),
                ],
            )
            .await;
            let plan =
                plan_simple_aggregate(leaf, op, &Grouping::By(vec!["group".into()])).unwrap();
            let got = run(plan, &ctx).await;
            assert2::assert!(got.len() == 1);
            assert2::assert!(got[0].1.to_bits() == want.to_bits());
        }
    }

    #[tokio::test]
    async fn min_max_over_all_nan_group_yield_nan_and_keep_series() {
        // Every sample in the group is NaN: Prometheus keeps the series with a
        // NaN result (it does not drop the group).
        for op in [SimpleAggregateOp::Min, SimpleAggregateOp::Max] {
            let ctx = SessionContext::new();
            let leaf =
                selector_like_leaf(&ctx, &[("prod", "api", f64::NAN), ("prod", "db", f64::NAN)])
                    .await;
            let plan =
                plan_simple_aggregate(leaf, op, &Grouping::By(vec!["group".into()])).unwrap();
            let got = run(plan, &ctx).await;
            assert2::assert!(got.len() == 1);
            assert2::assert!(got[0].1.is_nan());
        }
    }

    #[tokio::test]
    async fn all_null_group_yields_no_row() {
        // Every member of group g="x" is a NULL (no-value) row; the pre-aggregate
        // filter drops them, so the group forms no result row at all — matching
        // the interpreter, which never forms a group with no value-bearing sample.
        let ctx = SessionContext::new();
        let leaf = nullable_leaf(
            &ctx,
            &[
                ("x", "api", None),
                ("x", "db", None),
                ("y", "api", Some(3.0)),
            ],
        )
        .await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        // Only group y survives; the all-NULL group x produces no row.
        assert2::assert!(got == vec![(vec![("group".to_string(), "y".to_string())], 3.0)]);
    }

    #[tokio::test]
    async fn count_skips_null_rows() {
        // A group mixing NULL (no-value) rows with value-bearing rows: `count`
        // counts only the value-bearing series (NULLs dropped pre-aggregate), and
        // a genuine NaN value is non-null so it IS counted.
        let ctx = SessionContext::new();
        let leaf = nullable_leaf(
            &ctx,
            &[
                ("prod", "api", Some(1.0)),
                ("prod", "db", None),
                ("prod", "x", Some(f64::NAN)),
                ("prod", "y", None),
            ],
        )
        .await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Count,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        // 2 value-bearing rows (1.0 and the genuine NaN); the two NULLs are
        // dropped before counting.
        assert2::assert!(got == vec![(vec![("group".to_string(), "prod".to_string())], 2.0)]);
    }

    #[tokio::test]
    async fn sum_drops_null_keeps_genuine_nan() {
        // A NULL (no-value) member is excluded from the sum; a genuine NaN member
        // is kept and propagates, so the group's sum is NaN (not the value of the
        // single finite member, and not absent).
        let ctx = SessionContext::new();
        let leaf = nullable_leaf(
            &ctx,
            &[
                ("prod", "api", Some(2.0)),
                ("prod", "db", None),
                ("prod", "x", Some(f64::NAN)),
            ],
        )
        .await;
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, &ctx).await;
        assert2::assert!(got.len() == 1);
        assert2::assert!(got[0].1.is_nan());
    }
}

// === split-modules: generated submodules ===
mod aggregate_value_column;
mod all_group_column;
mod grouping;
mod input_label_columns;
mod plan_simple_aggregate;
mod resolve_group_labels;
mod simple_aggregate_op;

pub use aggregate_value_column::AGGREGATE_VALUE_COLUMN;
use all_group_column::ALL_GROUP_COLUMN;
pub use grouping::Grouping;
use input_label_columns::input_label_columns;
pub use plan_simple_aggregate::plan_simple_aggregate;
use resolve_group_labels::resolve_group_labels;
pub use simple_aggregate_op::SimpleAggregateOp;
