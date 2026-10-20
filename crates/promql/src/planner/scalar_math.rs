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
//! `Projection(labels..., prom_<fn>([bounds...,] value) AS value)`
//!
//! over that table.
//!
//! The projection keeps `__name__` for delayed removal at the query boundary,
//! while dropping `__type__` and `__unit__`. The module keeps every result row
//! and suppresses no NaN. `f(NaN)` and `sqrt(-1)` render as `NaN`, exactly as
//! the interpreter keeps every float sample.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{ArrayRef, BinaryArray, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    execution::FunctionRegistry,
    logical_expr::{Expr, LogicalPlan, LogicalPlanBuilder, col, lit},
    prelude::SessionContext,
};

use crate::{
    PromqlError, PromqlLabels as Labels, error::Result, extension::planner::prom_session_context,
    functions::ScalarMathOp, planner::leaf::leaf_scan,
};

#[cfg(test)]
mod tests {
    use arrow::array::Float64Array as Float64ArrayT;
    use assert2::check;

    use super::*;

    struct ExpectedJobValue {
        job: &'static str,
        value: f64,
    }

    fn assert_job_values(got: &[crate::planner::JobValue], expected: &[ExpectedJobValue]) {
        assert2::assert!(got.len() == expected.len());
        for (actual, expected) in got.iter().zip(expected) {
            check!(actual.job == expected.job);
            check!(actual.value == expected.value);
        }
    }

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
    ) -> Vec<crate::planner::JobValue> {
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
            assert2::assert!(batch.column_by_name("__name__").is_some());
            let l = batch
                .column_by_name("l")
                .unwrap()
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let value = batch
                .column_by_name(VALUE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64ArrayT>()
                .unwrap();
            for row in 0..batch.num_rows() {
                got.push(crate::planner::JobValue {
                    job: std::str::from_utf8(l.value(row)).unwrap().to_owned(),
                    value: value.value(row),
                });
            }
        }
        got.sort_by(|a, b| a.job.cmp(&b.job));
        got
    }

    #[tokio::test]
    async fn abs_defers_name_drop_and_keeps_label() {
        let got = run(
            vec![labeled("m", "x", -3.0), labeled("m", "y", 4.0)],
            ScalarMathOp::Abs,
            &[],
        )
        .await;
        assert_job_values(
            &got,
            &[
                ExpectedJobValue {
                    job: "x",
                    value: 3.0,
                },
                ExpectedJobValue {
                    job: "y",
                    value: 4.0,
                },
            ],
        );
    }

    #[tokio::test]
    async fn sqrt_negative_preserves_nan_row() {
        let got = run(vec![labeled("m", "x", -1.0)], ScalarMathOp::Sqrt, &[]).await;
        check!(got.len() == 1);
        check!(got[0].job == "x");
        check!(got[0].value.is_nan());
    }

    #[tokio::test]
    async fn genuine_nan_value_survives() {
        let got = run(vec![labeled("m", "x", f64::NAN)], ScalarMathOp::Sin, &[]).await;
        assert2::assert!(got.len() == 1);
        assert2::assert!(got[0].value.is_nan());
    }

    #[tokio::test]
    async fn clamp_min_greater_handled_by_bounds() {
        let got = run(
            vec![labeled("m", "x", 5.0), labeled("m", "y", -5.0)],
            ScalarMathOp::Clamp,
            &[0.0, 3.0],
        )
        .await;
        assert_job_values(
            &got,
            &[
                ExpectedJobValue {
                    job: "x",
                    value: 3.0,
                },
                ExpectedJobValue {
                    job: "y",
                    value: 0.0,
                },
            ],
        );
    }

    #[tokio::test]
    async fn round_uses_to_nearest_bound() {
        let got = run(vec![labeled("m", "x", 12.0)], ScalarMathOp::Round, &[5.0]).await;
        assert2::assert!(
            got == vec![crate::planner::JobValue {
                job: "x".to_string(),
                value: 10.0
            }]
        );
    }
}

mod build_leaf_batch;
mod plan_scalar_math;
mod scalar_math_plan;

use build_leaf_batch::build_leaf_batch;
pub use plan_scalar_math::plan_scalar_math;
pub use scalar_math_plan::ScalarMathPlan;

/// One already-evaluated inner instant-vector sample.
pub struct LabeledValue {
    pub labels: Labels,
    pub ts_ms: i64,
    pub value: f64,
}

/// Result-metadata labels dropped by every scalar-math function.
pub(crate) const METADATA_LABELS: [&str; 2] = ["__type__", "__unit__"];

/// Leaf-batch column that carries the per-series sample timestamp.
pub const SAMPLE_TIME_COLUMN: &str = "sample_timestamp";

/// Leaf-batch and projection column that carries the per-series float value.
pub const VALUE_COLUMN: &str = "value";

pub(crate) fn leaf_schema(label_names: &[String]) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(label_names.len() + 2);
    for name in label_names {
        fields.push(Field::new(name, DataType::Binary, true));
    }
    fields.push(Field::new(VALUE_COLUMN, DataType::Float64, true));
    fields.push(Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false));
    Arc::new(Schema::new(fields))
}

pub(crate) fn is_metadata_label(name: &str) -> bool {
    METADATA_LABELS.contains(&name)
}
