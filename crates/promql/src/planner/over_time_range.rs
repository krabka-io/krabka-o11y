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
    use arrow::array::{BinaryArray, Float64Array};
    use assert2::check;

    use super::*;
    use crate::planner::RangeWindowGrid;

    fn approx_eq(left: f64, right: f64) -> bool {
        (left - right).abs() < 1e-9
    }

    /// Runs `fold` at `eval_time_ms` over the window `(0, eval_time_ms]`.
    async fn run(
        samples: Vec<LabeledSeries>,
        eval_time_ms: i64,
        fold: OverTimeFold,
    ) -> Vec<(String, f64)> {
        let windows = RangeWindowGrid::since_epoch(eval_time_ms);
        let plan = plan_over_time_range_selector(samples, windows, fold)
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
                .downcast_ref::<BinaryArray>()
                .unwrap();
            let value = batch
                .column_by_name(OVER_TIME_VALUE_COLUMN)
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                got.push((
                    String::from_utf8(job.value(row).to_vec()).unwrap(),
                    value.value(row),
                ));
            }
        }
        got
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
        check!(got[0].0 == "a");
        check!(approx_eq(got[0].1, 4.0));
    }

    #[test]
    fn over_time_family_lookup_covers_operator_families() {
        let cases = [
            ("sum_over_time", Some(OverTimeFamily::Sum)),
            ("avg_over_time", Some(OverTimeFamily::Avg)),
            ("count_over_time", Some(OverTimeFamily::Count)),
            ("min_over_time", Some(OverTimeFamily::Min)),
            ("max_over_time", Some(OverTimeFamily::Max)),
            ("stddev_over_time", Some(OverTimeFamily::Stddev)),
            ("stdvar_over_time", Some(OverTimeFamily::Stdvar)),
            ("last_over_time", Some(OverTimeFamily::Last)),
            ("present_over_time", Some(OverTimeFamily::Present)),
            ("quantile_over_time", Some(OverTimeFamily::Quantile)),
            ("mad_over_time", None),
            ("first_over_time", None),
            ("ts_of_min_over_time", None),
            ("rate", None),
        ];
        for (name, want) in cases {
            assert2::assert!(over_time_family_from_function_name(name) == want);
        }
    }

    /// `quantile_over_time(0.5, ...)` over 2,4,4,4,5,5,7,9 gives the median 4.5.
    #[tokio::test]
    async fn quantile_over_time_plan_threads_phi() {
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let series = values
            .iter()
            .enumerate()
            .fold(LabeledSeries::with_job("a"), |series, (i, v)| {
                series.at((i64::try_from(i).unwrap() + 1) * 60_000, *v)
            });
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
        assert2::assert!(approx_eq(got[0].1, 4.5));
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
        assert2::assert!(approx_eq(got[0].1, 1.0));
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
        let batches = plan
            .ctx
            .execute_logical_plan(plan.plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let value = batches[0]
            .column_by_name(OVER_TIME_VALUE_COLUMN)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert2::assert!(value.len() == 1);
        assert2::assert!(value.is_null(0));
    }
}

mod over_time_family_from_function_name;
mod over_time_fold;
mod over_time_range_plan;
mod over_time_value_column;
mod plan_over_time_range_selector;
mod time_column;
mod value_column;

pub use over_time_family_from_function_name::over_time_family_from_function_name;
pub use over_time_fold::OverTimeFold;
pub use over_time_range_plan::OverTimeRangePlan;
pub use over_time_value_column::OVER_TIME_VALUE_COLUMN;
pub use plan_over_time_range_selector::plan_over_time_range_selector;
pub use time_column::TIME_COLUMN;
pub use value_column::VALUE_COLUMN;
