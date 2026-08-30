//! Leaf source and `LogicalPlan` assembly for the per-row scalar-math operator path.
//!
//! The path covers `abs`, `ceil`, …, the trig and hyperbolic family, `sgn`,
//! `round`, and the `clamp` family.
//!
//! The selector, rate, and `*_over_time` paths are not handed an evaluated input.
//! This module is handed the already-evaluated inner instant vector: one float
//! value per matched series, with genuine NaN preserved. The engine sources those
//! samples from a NaN-preserving bare-selector selection, or it assembles a
//! nested plannable inner expression. This module materializes the samples as a
//! leaf table with one row per series. The table carries the label columns of the
//! series plus a `value` column. The module then projects
//!
//! `Projection(labels-without-__name__..., prom_<fn>([bounds...,] value) AS value)`
//!
//! over that table.
//!
//! The projection drops the metric name `__name__` and the result-metadata labels
//! `__type__` and `__unit__`. Every scalar-math function drops them, as the
//! interpreter function `labels_without_metric_name` does. The module keeps every
//! result row and suppresses no NaN. `f(NaN)` and `sqrt(-1)` render as `NaN`,
//! exactly as the interpreter keeps every float sample.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::MemTable,
    execution::FunctionRegistry,
    logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col, lit},
    prelude::SessionContext,
};
use krabka_blockstore::Labels;

use crate::{
    PromqlError, error::Result, extension::planner::prom_session_context, functions::ScalarMathOp,
};

#[cfg(test)]
mod tests {
    use arrow::array::Float64Array as Float64ArrayT;
    use assert2::check;

    use super::*;

    fn labeled(name: &str, l: &str, value: f64) -> LabeledValue {
        let mut labels = Labels::new();
        labels.insert("__name__", name);
        labels.insert("l", l);
        LabeledValue {
            labels,
            ts_ms: 0,
            value,
        }
    }

    async fn run(
        samples: Vec<LabeledValue>,
        op: ScalarMathOp,
        bounds: &[f64],
    ) -> Vec<(String, f64)> {
        let plan = plan_scalar_math(samples, op, bounds).await.unwrap();
        let batches = plan
            .ctx
            .execute_logical_plan(plan.plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let mut got = Vec::new();
        for batch in &batches {
            // `__name__` must be gone; the projection carries only `l` + `value`.
            assert2::assert!(batch.column_by_name("__name__").is_none());
            let l = batch
                .column_by_name("l")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let value = batch
                .column_by_name(VALUE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64ArrayT>()
                .unwrap();
            for row in 0..batch.num_rows() {
                got.push((l.value(row).to_string(), value.value(row)));
            }
        }
        got.sort_by(|a, b| a.0.cmp(&b.0));
        got
    }

    #[tokio::test]
    async fn abs_drops_name_and_keeps_label() {
        let got = run(
            vec![labeled("m", "x", -3.0), labeled("m", "y", 4.0)],
            ScalarMathOp::Abs,
            &[],
        )
        .await;
        assert2::assert!(got == vec![("x".to_string(), 3.0), ("y".to_string(), 4.0)]);
    }

    #[tokio::test]
    async fn sqrt_negative_preserves_nan_row() {
        let got = run(vec![labeled("m", "x", -1.0)], ScalarMathOp::Sqrt, &[]).await;
        check!(got.len() == 1);
        check!(got[0].0 == "x");
        check!(got[0].1.is_nan());
    }

    #[tokio::test]
    async fn genuine_nan_value_survives() {
        let got = run(vec![labeled("m", "x", f64::NAN)], ScalarMathOp::Sin, &[]).await;
        assert2::assert!(got.len() == 1);
        assert2::assert!(got[0].1.is_nan());
    }

    #[tokio::test]
    async fn clamp_min_greater_handled_by_bounds() {
        let got = run(
            vec![labeled("m", "x", 5.0), labeled("m", "y", -5.0)],
            ScalarMathOp::Clamp,
            &[0.0, 3.0],
        )
        .await;
        assert2::assert!(got == vec![("x".to_string(), 3.0), ("y".to_string(), 0.0)]);
    }

    #[tokio::test]
    async fn round_uses_to_nearest_bound() {
        let got = run(vec![labeled("m", "x", 12.0)], ScalarMathOp::Round, &[5.0]).await;
        assert2::assert!(got == vec![("x".to_string(), 10.0)]);
    }
}

mod build_leaf_batch;
mod is_metadata_label;
mod labeled_value;
mod leaf_schema;
mod metadata_labels;
mod plan_scalar_math;
mod sample_time_column;
mod scalar_math_plan;
mod value_column;

use build_leaf_batch::build_leaf_batch;
use is_metadata_label::is_metadata_label;
pub use labeled_value::LabeledValue;
use leaf_schema::leaf_schema;
use metadata_labels::METADATA_LABELS;
pub use plan_scalar_math::plan_scalar_math;
pub use sample_time_column::SAMPLE_TIME_COLUMN;
pub use scalar_math_plan::ScalarMathPlan;
pub use value_column::VALUE_COLUMN;
