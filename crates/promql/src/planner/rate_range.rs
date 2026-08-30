//! Leaf source and `LogicalPlan` assembly for the rate-family range-selector
//! operator path.
//!
//! This module is the matrix-selector sibling of [`super::leaf`]. The instant
//! path selects one sample per series inside the lookback window. The rate path
//! instead materializes a full range window `(t - range, t]` per series and
//! folds it through [`RangeManipulate`] into windowed `RangeArray` columns. It
//! then projects a rate-family [`datafusion::logical_expr::ScalarUDF`] over
//! those columns and returns one float per series.
//!
//! The assembled chain is
//! `<leaf over MetricStore> -> SeriesDivide -> SeriesNormalize -> RangeManipulate
//! -> Projection(labels..., prom_<fn>(timestamp, timestamp_range, value_range,
//! range_ms) AS value)`.
//!
//! # Window semantics (vs. the instant path)
//!
//! A range selector does not apply the 5m lookback. The window is exactly
//! `(eval_time - range, eval_time]`, left-open and right-closed. This matches
//! Prometheus' matrix-selector semantics and the interpreter's
//! `range_function_sample_from_series`. The caller fetches samples over
//! `(eval_time - range, eval_time]` and passes the window's range width as
//! `range_ms`. `RangeManipulate` re-derives the per-step window, and the UDF
//! re-derives `range_start = eval_timestamp - range_ms`.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::MemTable,
    execution::FunctionRegistry,
    logical_expr::{Expr, Extension, LogicalPlan, LogicalPlanBuilder, col, lit},
    prelude::SessionContext,
};
use krabka_blockstore::{Labels, SeriesFingerprint};
use krabka_units::prelude::*;

use crate::{
    PromqlError,
    error::Result,
    extension::{
        normalize::SeriesNormalize,
        planner::prom_session_context,
        range_manipulate::{RANGE_SUFFIX, RangeManipulate},
        series_divide::SeriesDivide,
    },
};

#[cfg(test)]
mod tests {
    use arrow::array::{Float64Array, StringArray};
    use assert2::check;

    use super::*;

    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-9
    }

    fn labeled(job: &str, ts_ms: i64, value: f64) -> LabeledSample {
        let mut labels = Labels::new();
        labels.insert("job", job);
        LabeledSample {
            fp: labels.fingerprint(),
            labels,
            ts_ms,
            value,
        }
    }

    /// `rate(counter[5m])` over the engine's canonical counter window returns
    /// 5/300 through the full operator chain. The window runs 0..240s in steps
    /// of 1.0, with the eval at t=300s. This matches
    /// `extrapolate::rate_extrapolates_counter_window`.
    #[tokio::test]
    async fn rate_range_plan_reproduces_counter_window() {
        let samples = vec![
            labeled("a", 0, 0.0),
            labeled("a", 60_000, 1.0),
            labeled("a", 120_000, 2.0),
            labeled("a", 180_000, 3.0),
            labeled("a", 240_000, 4.0),
        ];
        let plan = plan_rate_range_selector(samples, 300_000, millis(300_000), RateUdfKind::Rate)
            .await
            .unwrap();
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
            let job = batch
                .column_by_name("job")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let value = batch
                .column_by_name(RATE_VALUE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                got.push((job.value(row).to_string(), value.value(row)));
            }
        }
        check!(got.len() == 1);
        check!(got[0].0 == "a");
        check!(approx_eq(got[0].1, 5.0 / 300.0));
    }

    /// `increase` reset correction flows through the chain: 1,2,1 -> 2.0.
    #[tokio::test]
    async fn increase_range_plan_corrects_reset() {
        let samples = vec![
            labeled("a", 0, 1.0),
            labeled("a", 60_000, 2.0),
            labeled("a", 120_000, 1.0),
        ];
        let plan =
            plan_rate_range_selector(samples, 120_000, millis(120_000), RateUdfKind::Increase)
                .await
                .unwrap();
        let batches = plan
            .ctx
            .execute_logical_plan(plan.plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let value = batches[0]
            .column_by_name(RATE_VALUE_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert2::assert!(approx_eq(value.value(0), 2.0));
    }

    /// A single-sample window has no rate. The UDF returns NULL rather than a
    /// NaN sentinel, so the assembler drops the series and the aggregates skip
    /// it.
    #[tokio::test]
    async fn single_sample_window_yields_null() {
        use arrow::array::Array;

        let samples = vec![labeled("a", 60_000, 1.0)];
        let plan = plan_rate_range_selector(samples, 60_000, millis(60_000), RateUdfKind::Rate)
            .await
            .unwrap();
        let batches = plan
            .ctx
            .execute_logical_plan(plan.plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let value = batches[0]
            .column_by_name(RATE_VALUE_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert2::assert!(value.is_null(0));
    }
}

// === split-modules: generated submodules ===
mod build_leaf_batch;
mod labeled_sample;
mod leaf_schema;
mod plan_rate_range_selector;
mod rate_range_plan;
mod rate_udf_kind;
mod rate_value_column;
mod time_column;
mod value_column;

use build_leaf_batch::build_leaf_batch;
pub use labeled_sample::LabeledSample;
use leaf_schema::leaf_schema;
pub use plan_rate_range_selector::plan_rate_range_selector;
pub use rate_range_plan::RateRangePlan;
pub use rate_udf_kind::RateUdfKind;
pub use rate_value_column::RATE_VALUE_COLUMN;
pub use time_column::TIME_COLUMN;
pub use value_column::VALUE_COLUMN;
