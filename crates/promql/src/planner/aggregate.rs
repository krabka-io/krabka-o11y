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

    /// One input series of the `agg_leaf` table.
    #[derive(Clone, Copy)]
    struct LeafRow<'a> {
        group: &'a str,
        job: &'a str,
        /// `None` is the NULL "no value" cell.
        sample_value: Option<f64>,
    }

    impl<'a> LeafRow<'a> {
        /// A row in `group` with an empty `job` and no value.
        const fn group(group: &'a str) -> Self {
            Self {
                group,
                job: "",
                sample_value: None,
            }
        }

        const fn job(self, job: &'a str) -> Self {
            Self { job, ..self }
        }

        const fn sample(self, sample_value: f64) -> Self {
            Self {
                sample_value: Some(sample_value),
                ..self
            }
        }
    }

    /// Two `prod` series (1 and 2) and one `canary` series (4).
    const PROD_AND_CANARY: [LeafRow<'static>; 3] = [
        LeafRow::group("prod").job("api").sample(1.0),
        LeafRow::group("prod").job("db").sample(2.0),
        LeafRow::group("canary").job("api").sample(4.0),
    ];

    /// Builds a leaf plan over an in-memory table like an instant-selector output.
    ///
    /// The table has the `job` and `group` labels plus
    /// `timestamp`/`value`/`sample_timestamp`.
    async fn selector_like_leaf(session: &SessionContext, rows: &[LeafRow<'_>]) -> LogicalPlan {
        AggLeaf::from_rows(rows, Field::new(VALUE_COLUMN, DataType::Float64, false))
            .register(session)
            .await
    }

    /// Like [`selector_like_leaf`], but with a nullable `value` column.
    ///
    /// A `None` row models the "no value" NULL cell of a rate or `*_over_time`
    /// UDF. This drives the pre-aggregate NULL filter: the planner must drop
    /// such rows before grouping, exactly as the interpreter omits no-value
    /// series.
    async fn nullable_leaf(session: &SessionContext, rows: &[LeafRow<'_>]) -> LogicalPlan {
        AggLeaf::from_rows(rows, Field::new(VALUE_COLUMN, DataType::Float64, true))
            .register(session)
            .await
    }

    /// The columns of the `agg_leaf` table behind [`selector_like_leaf`] and
    /// [`nullable_leaf`]: one row per `groups`/`jobs`/`sample_values` index, at time 0.
    struct AggLeaf<'a> {
        groups: Vec<&'a str>,
        jobs: Vec<&'a str>,
        sample_values: Vec<Option<f64>>,
        /// How the `value` column is declared.
        value_field: Field,
    }

    impl<'a> AggLeaf<'a> {
        fn from_rows(rows: &[LeafRow<'a>], value_field: Field) -> Self {
            Self {
                groups: rows.iter().map(|row| row.group).collect(),
                jobs: rows.iter().map(|row| row.job).collect(),
                sample_values: rows.iter().map(|row| row.sample_value).collect(),
                value_field,
            }
        }

        /// Registers the table on `ctx` and returns a plan that scans it.
        async fn register(self, session: &SessionContext) -> LogicalPlan {
            let rows = self.sample_values.len();
            let schema = Arc::new(Schema::new(vec![
                Field::new("group", DataType::Utf8, false),
                Field::new("job", DataType::Utf8, false),
                Field::new(TIME_COLUMN, DataType::Int64, false),
                self.value_field,
                Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false),
            ]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringArray::from(self.groups)),
                    Arc::new(StringArray::from(self.jobs)),
                    Arc::new(Int64Array::from(vec![0_i64; rows])),
                    Arc::new(Float64Array::from(self.sample_values)),
                    Arc::new(Int64Array::from(vec![0_i64; rows])),
                ],
            )
            .unwrap();
            let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
            session.register_table("agg_leaf", Arc::new(table)).unwrap();
            session
                .table("agg_leaf")
                .await
                .unwrap()
                .into_optimized_plan()
                .unwrap()
        }
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

    /// Checks that `sum by (group)` over `leaf` yields one group whose sum is NaN.
    /// Runs `op` grouped `by (group)` over a selector-like leaf of `rows`.
    async fn aggregate_by_group(
        op: SimpleAggregateOp,
        rows: &[LeafRow<'_>],
    ) -> Vec<(Vec<(String, String)>, f64)> {
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(&ctx, rows).await;
        let plan = plan_simple_aggregate(leaf, op, &Grouping::By(vec!["group".into()])).unwrap();
        run(plan, &ctx).await
    }

    async fn assert_sum_by_group_is_nan(leaf: LogicalPlan, ctx: &SessionContext) {
        let plan = plan_simple_aggregate(
            leaf,
            SimpleAggregateOp::Sum,
            &Grouping::By(vec!["group".into()]),
        )
        .unwrap();
        let got = run(plan, ctx).await;
        assert2::assert!(got.len() == 1);
        assert2::assert!(got[0].1.is_nan());
    }

    /// `sum by (group)` and `sum without (job)` both collapse to the `group`
    /// label.
    #[tokio::test]
    async fn sum_by_and_without_collapse_to_group_labels() {
        // without (job) -> group by `group`.
        for grouping in [
            Grouping::By(vec!["group".into()]),
            Grouping::Without(vec!["job".into()]),
        ] {
            let ctx = SessionContext::new();
            let leaf = selector_like_leaf(&ctx, &PROD_AND_CANARY).await;
            let plan = plan_simple_aggregate(leaf, SimpleAggregateOp::Sum, &grouping).unwrap();
            let got = run(plan, &ctx).await;
            assert2::assert!(
                got == vec![
                    (vec![("group".to_string(), "canary".to_string())], 4.0),
                    (vec![("group".to_string(), "prod".to_string())], 3.0),
                ],
                "{grouping:?}"
            );
        }
    }

    #[tokio::test]
    async fn sum_by_empty_collapses_all() {
        let ctx = SessionContext::new();
        let leaf = selector_like_leaf(&ctx, &PROD_AND_CANARY).await;
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
                PROD_AND_CANARY,
                vec![
                    (vec![("group".to_string(), "canary".to_string())], 1.0),
                    (vec![("group".to_string(), "prod".to_string())], 2.0),
                ],
            ),
            (
                SimpleAggregateOp::Group,
                [
                    LeafRow::group("prod").job("api").sample(9.0),
                    LeafRow::group("prod").job("db").sample(2.0),
                    LeafRow::group("canary").job("api").sample(4.0),
                ],
                vec![
                    (vec![("group".to_string(), "canary".to_string())], 1.0),
                    (vec![("group".to_string(), "prod".to_string())], 1.0),
                ],
            ),
        ];
        for (op, rows, want) in cases {
            let got = aggregate_by_group(op, &rows).await;
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
        let leaf = selector_like_leaf(
            &ctx,
            &[
                LeafRow::group("prod").job("api").sample(1.0),
                LeafRow::group("prod").job("db").sample(f64::NAN),
            ],
        )
        .await;
        assert_sum_by_group_is_nan(leaf, &ctx).await;
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
            let got = aggregate_by_group(
                op,
                &[
                    LeafRow::group("prod").job("api").sample(f64::NAN),
                    LeafRow::group("prod").job("db").sample(3.0),
                    LeafRow::group("prod").job("x").sample(1.0),
                    LeafRow::group("prod").job("y").sample(f64::NAN),
                ],
            )
            .await;
            assert2::assert!(got.len() == 1);
            assert2::assert!(got[0].1.to_bits() == want.to_bits());
        }
    }

    #[tokio::test]
    async fn min_max_over_all_nan_group_yield_nan_and_keep_series() {
        // Every sample in the group is NaN: Prometheus keeps the series with a
        // NaN result (it does not drop the group).
        for op in [SimpleAggregateOp::Min, SimpleAggregateOp::Max] {
            let got = aggregate_by_group(
                op,
                &[
                    LeafRow::group("prod").job("api").sample(f64::NAN),
                    LeafRow::group("prod").job("db").sample(f64::NAN),
                ],
            )
            .await;
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
                LeafRow::group("x").job("api"),
                LeafRow::group("x").job("db"),
                LeafRow::group("y").job("api").sample(3.0),
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
                LeafRow::group("prod").job("api").sample(1.0),
                LeafRow::group("prod").job("db"),
                LeafRow::group("prod").job("x").sample(f64::NAN),
                LeafRow::group("prod").job("y"),
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
                LeafRow::group("prod").job("api").sample(2.0),
                LeafRow::group("prod").job("db"),
                LeafRow::group("prod").job("x").sample(f64::NAN),
            ],
        )
        .await;
        assert_sum_by_group_is_nan(leaf, &ctx).await;
    }
}

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
