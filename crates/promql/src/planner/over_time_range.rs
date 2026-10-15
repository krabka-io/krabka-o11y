//! Leaf source and `LogicalPlan` assembly for the `*_over_time` operator path.
//!
//! This module is the `*_over_time` sibling of [`super::rate_range`]. It uses
//! the same `<leaf over MetricStore> -> SeriesDivide -> SeriesNormalize ->
//! RangeManipulate` plumbing. Only the final projection is different. A
//! rate-family UDF takes `(timestamp, timestamp_range, value_range, range_ms)`.
//! This module instead projects an `*_over_time` UDF that takes `(timestamp,
//! timestamp_range, value_range)`, with a leading `phi` literal for
//! `quantile_over_time`.
//!
//! The assembled chain is
//! `<leaf over MetricStore> -> SeriesDivide -> SeriesNormalize -> RangeManipulate
//! -> Projection(labels..., prom_<fn>_over_time([phi,] timestamp, timestamp_range,
//! value_range) AS value)`.
//!
//! # Window semantics
//!
//! The window is the same as the rate path: exactly `(eval_time - range,
//! eval_time]`, left-open and right-closed, with no 5m lookback. This matches
//! Prometheus matrix-selector semantics and the interpreter's
//! `over_time_sample_from_series`, which filters on `range_start < ts <=
//! range_end`.

use std::collections::BTreeMap;

use datafusion::{
    logical_expr::{Expr, LogicalPlan, lit},
    prelude::SessionContext,
};
use krabka_blockstore::SeriesFingerprint;

use super::LabeledSeries;
use crate::{
    PromqlLabels as Labels, error::Result, extension::planner::prom_session_context,
    functions::OverTimeFamily,
};

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;
    use crate::planner::{RangeWindowGrid, approx_eq, first_batch_values, job_values};

    /// Runs `fold` at `eval_time_ms` over the window `(0, eval_time_ms]`.
    async fn run(
        samples: Vec<LabeledSeries>,
        eval_time_ms: i64,
        fold: OverTimeFold,
    ) -> Vec<crate::planner::JobValue> {
        let windows = RangeWindowGrid::since_epoch(eval_time_ms);
        let plan = plan_over_time_range_selector(samples, windows, fold)
            .await
            .unwrap();
        job_values(&plan.ctx, plan.plan, OVER_TIME_VALUE_COLUMN).await
    }

    /// `avg_over_time` over the engine's basic window (3,5 -> 4.0) runs the full chain.
    #[tokio::test]
    async fn avg_over_time_plan_reduces_window() {
        let samples = vec![
            LabeledSeries::with_job("a")
                .at(60_000, 3.0)
                .at(120_000, 5.0),
        ];
        let got = run(
            samples,
            120_000,
            OverTimeFold {
                family: OverTimeFamily::Avg,
                phi: 0.0,
            },
        )
        .await;
        check!(got.len() == 1);
        check!(got[0].job == "a");
        check!(approx_eq(got[0].value, 4.0));
    }

    #[yare::parameterized(
        sum = { "sum_over_time", Some(OverTimeFamily::Sum) },
        avg = { "avg_over_time", Some(OverTimeFamily::Avg) },
        count = { "count_over_time", Some(OverTimeFamily::Count) },
        min = { "min_over_time", Some(OverTimeFamily::Min) },
        max = { "max_over_time", Some(OverTimeFamily::Max) },
        stddev = { "stddev_over_time", Some(OverTimeFamily::Stddev) },
        stdvar = { "stdvar_over_time", Some(OverTimeFamily::Stdvar) },
        last = { "last_over_time", Some(OverTimeFamily::Last) },
        present = { "present_over_time", Some(OverTimeFamily::Present) },
        quantile = { "quantile_over_time", Some(OverTimeFamily::Quantile) },
        mad_unsupported = { "mad_over_time", None },
        first_unsupported = { "first_over_time", None },
        ts_of_min_unsupported = { "ts_of_min_over_time", None },
        rate_unsupported = { "rate", None },
    )]
    fn over_time_family_lookup_covers_operator_families(name: &str, want: Option<OverTimeFamily>) {
        assert2::assert!(over_time_family_from_function_name(name) == want);
    }

    /// `quantile_over_time(0.5, ...)` over 2,4,4,4,5,5,7,9 gives the median 4.5.
    #[tokio::test]
    async fn quantile_over_time_plan_threads_phi() {
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let mut series = LabeledSeries::with_job("a");
        for index in 0..values.len() {
            series = series.at((i64::try_from(index).unwrap() + 1) * 60_000, values[index]);
        }
        let samples = vec![series];
        let got = run(
            samples,
            480_000,
            OverTimeFold {
                family: OverTimeFamily::Quantile,
                phi: 0.5,
            },
        )
        .await;
        assert2::assert!(got.len() == 1);
        assert2::assert!(approx_eq(got[0].value, 4.5));
    }

    /// `present_over_time` gives 1.0 when the window has samples.
    #[tokio::test]
    async fn present_over_time_plan_signals_presence() {
        let samples = vec![LabeledSeries::with_job("a").at(60_000, 42.0)];
        let got = run(
            samples,
            120_000,
            OverTimeFold {
                family: OverTimeFamily::Present,
                phi: 0.0,
            },
        )
        .await;
        assert2::assert!(approx_eq(got[0].value, 1.0));
    }

    /// An empty window emits NULL, not a NaN sentinel.
    ///
    /// The assembler drops the series and aggregates skip it.
    #[tokio::test]
    async fn empty_window_emits_null() {
        use arrow::array::Array;

        // A sample on the left edge (ts == range_start) is excluded by the
        // left-open window, leaving the window empty.
        let samples = vec![LabeledSeries::with_job("a").at(0, 5.0)];
        let plan = plan_over_time_range_selector(
            samples,
            RangeWindowGrid::since_epoch(120_000),
            OverTimeFold {
                family: OverTimeFamily::Sum,
                phi: 0.0,
            },
        )
        .await
        .unwrap();
        let value = first_batch_values(&plan.ctx, plan.plan, OVER_TIME_VALUE_COLUMN).await;
        assert2::assert!(value.len() == 1);
        assert2::assert!(value.is_null(0));
    }
}
mod over_time_fold;
mod over_time_range_plan;
mod plan_over_time_range_selector;
pub use over_time_fold::OverTimeFold;
pub use over_time_range_plan::OverTimeRangePlan;
pub use plan_over_time_range_selector::plan_over_time_range_selector;

/// Projection output column name with the `*_over_time` UDF result.
pub const OVER_TIME_VALUE_COLUMN: &str = "value";

/// Leaf-batch column with the per-sample timestamp in epoch milliseconds.
pub const TIME_COLUMN: &str = "timestamp";

/// Leaf-batch column with the per-sample float value.
pub const VALUE_COLUMN: &str = "value";

/// Resolves a matrix-selector `*_over_time` function name to its [`OverTimeFamily`].
///
/// [`OverTimeFamily`] is the operator path, the float UDF chain. This function
/// returns `None` for the experimental members `mad_over_time`,
/// `first_over_time`, and the `ts_of_*_over_time` family. Those members have no
/// operator-leaf UDF and route through the engine's shared
/// `apply_outer_range_fn` kernel instead. This function also returns `None` for
/// any function outside the `*_over_time` set.
#[must_use]
pub fn over_time_family_from_function_name(name: &str) -> Option<OverTimeFamily> {
    match name {
        "sum_over_time" => Some(OverTimeFamily::Sum),
        "avg_over_time" => Some(OverTimeFamily::Avg),
        "count_over_time" => Some(OverTimeFamily::Count),
        "min_over_time" => Some(OverTimeFamily::Min),
        "max_over_time" => Some(OverTimeFamily::Max),
        "stddev_over_time" => Some(OverTimeFamily::Stddev),
        "stdvar_over_time" => Some(OverTimeFamily::Stdvar),
        "last_over_time" => Some(OverTimeFamily::Last),
        "present_over_time" => Some(OverTimeFamily::Present),
        "quantile_over_time" => Some(OverTimeFamily::Quantile),
        _ => None,
    }
}
