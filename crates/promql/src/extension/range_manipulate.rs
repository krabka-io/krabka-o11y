//! `RangeManipulate`: materialize range vectors over a step grid.
//!
//! # Output-schema / column contract
//!
//! `RangeManipulate` reads one series' time-sorted `(timestamp, value)` batch
//! from downstream of [`SeriesNormalize`] and folds the samples into
//! per-eval-step windows. [`build_extended_range_schema`] builds the output
//! schema. Its columns, in order:
//!
//! 1. Every label column of the input, which is any column that is neither the
//!    time index nor the value column. Each label column passes through
//!    unchanged and keeps its original relative order. For each eval step these
//!    columns repeat the series' label values, one row per eval step.
//! 2. The eval `timestamp` column, which reuses the input time-index column
//!    name. It is `Int64` and scalar: one value for each aligned step on the
//!    `[start, end]` grid with stride `interval`. This is the instant `t` that
//!    each window closes on. Downstream rate-family UDFs read this column as
//!    the evaluation timestamp.
//! 3. A `<time_index>_range` column, for example `timestamp_range`. It is a
//!    `RangeArray` encoded as `Dictionary<Int64, List<Int64>>`. Cell `i` holds
//!    the sample timestamps that fall in the window that closes at eval step
//!    `i`.
//! 4. A `<value>_range` column, for example `value_range`. It is a `RangeArray`
//!    encoded as `Dictionary<Int64, List<Float64>>`. Cell `i` holds the sample
//!    values that align 1:1 with the timestamps in `<time_index>_range` cell
//!    `i`.
//!
//! The two `RangeArray` columns are always row-aligned with each other and with
//! the eval `timestamp` column. Decode them with
//! [`RangeArray::try_from_dict_array`].
//!
//! # Window semantics
//!
//! For eval timestamp `t` and range duration `range`, the window holds a sample
//! at timestamp `ts` if and only if `t - range < ts <= t`. The window is
//! left-open and right-closed: `(t - range, t]`. A sample exactly on the right
//! boundary, where `ts == t`, is included. A sample exactly on the left edge,
//! where `ts == t - range`, is excluded. This matches `PromQL` range-selector
//! semantics. An empty window produces an empty `RangeArray` cell with zero
//! length.

use std::{fmt, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, UInt32Array},
    compute::take,
    datatypes::{DataType, Field, Schema, SchemaRef},
    record_batch::RecordBatch,
};
use datafusion::{
    common::{DataFusionError, Result as DfResult},
    logical_expr::{Expr, LogicalPlan, UserDefinedLogicalNodeCore},
    physical_expr::EquivalenceProperties,
    physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties},
};

use crate::range_array::RangeArray;

#[cfg(test)]
mod tests {
    use arrow::{
        array::{Array, AsArray, DictionaryArray, Float64Array, Int64Array, StringArray},
        compute::concat_batches,
        datatypes::{ArrowPrimitiveType, DataType, Field, Float64Type, Int64Type, Schema},
    };
    use assert2::check;
    use datafusion::{
        common::{plan_err, tree_node::TreeNodeRecursion},
        datasource::memory::MemorySourceConfig,
        physical_plan::collect,
        prelude::SessionContext,
    };

    use super::*;

    fn series_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("job", DataType::Utf8, false),
            Field::new("timestamp", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]))
    }

    fn series_batch(ts: Vec<i64>, val: Vec<f64>) -> (RecordBatch, Arc<Schema>) {
        let schema = series_schema();
        let job = StringArray::from(vec!["a"; ts.len()]);
        let ts = Int64Array::from(ts);
        let val = Float64Array::from(val);
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(job), Arc::new(ts), Arc::new(val)],
        )
        .unwrap();
        (batch, schema)
    }

    /// Decodes a `RangeArray` dict column into one `Vec` of its `T` values per
    /// cell.
    fn range_cells<T: ArrowPrimitiveType>(batch: &RecordBatch, name: &str) -> Vec<Vec<T::Native>> {
        let dict = batch
            .column_by_name(name)
            .unwrap()
            .as_any()
            .downcast_ref::<DictionaryArray<Int64Type>>()
            .unwrap();
        let range = RangeArray::try_from_dict_array(dict).unwrap();
        (0..range.len())
            .map(|cell| {
                range
                    .get(cell)
                    .unwrap()
                    .as_primitive::<T>()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    fn timestamp_cells(batch: &RecordBatch, name: &str) -> Vec<Vec<i64>> {
        range_cells::<Int64Type>(batch, name)
    }

    fn value_cells(batch: &RecordBatch, name: &str) -> Vec<Vec<f64>> {
        range_cells::<Float64Type>(batch, name)
    }

    /// Settings over the `timestamp` and `value` columns, on a one-instant grid
    /// at zero with a one-millisecond window; each test overrides the grid.
    fn timestamp_value_settings() -> RangeManipulateSettings {
        RangeManipulateSettings {
            start_ms: 0,
            end_ms: 0,
            interval_ms: 1,
            range_ms: 1,
            time_index: "timestamp".into(),
            field_column: "value".into(),
        }
    }

    async fn run(batch: RecordBatch, settings: RangeManipulateSettings) -> RecordBatch {
        let schema = batch.schema();
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let exec = RangeManipulateExec::new(settings, mem);
        let out_schema = exec.output_schema.clone();
        let ctx = SessionContext::new();
        let out = collect(Arc::new(exec), ctx.task_ctx()).await.unwrap();
        concat_batches(&out_schema, &out).unwrap()
    }

    #[test]
    fn physical_node_owns_no_expressions() {
        let (batch, schema) = series_batch(vec![0], vec![1.0]);
        let mem = MemorySourceConfig::try_new_exec(&[vec![batch]], schema, None).unwrap();
        let exec = RangeManipulateExec::new(timestamp_value_settings(), mem);
        // The plan owns no expression, so a visitor that fails must never run.
        let walk = exec.apply_expressions(&mut |_| plan_err!("visited an expression"));
        check!(let Ok(TreeNodeRecursion::Continue) = walk);
    }

    #[test]
    fn extended_schema_layout_matches_contract() {
        let schema = build_extended_range_schema(&series_schema(), "timestamp", "value");
        let names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        check!(names == vec!["job", "timestamp", "timestamp_range", "value_range"]);
        check!(schema.field_with_name("timestamp").unwrap().data_type() == &DataType::Int64);
        check!(schema.field_with_name("job").unwrap().data_type() == &DataType::Utf8);
        // The range columns are dictionaries of lists.
        assert2::assert!(matches!(
            schema
                .field_with_name("timestamp_range")
                .unwrap()
                .data_type(),
            DataType::Dictionary(_, _)
        ));
        assert2::assert!(matches!(
            schema.field_with_name("value_range").unwrap().data_type(),
            DataType::Dictionary(_, _)
        ));
    }

    #[tokio::test]
    async fn right_boundary_sample_is_included() {
        // Sample exactly at eval timestamp t must land in the window.
        let (batch, _) = series_batch(vec![100], vec![1.0]);
        let out = run(
            batch,
            RangeManipulateSettings {
                start_ms: 100,
                end_ms: 100,
                interval_ms: 60,
                range_ms: 60,
                ..timestamp_value_settings()
            },
        )
        .await;
        let cells = timestamp_cells(&out, "timestamp_range");
        assert2::assert!(cells == vec![vec![100_i64]]);
    }

    #[tokio::test]
    async fn left_edge_sample_is_excluded() {
        // Sample at t - range must be excluded (left-open). With t=100, range=60
        // the left edge is 40; a sample at 40 is out, a sample at 41 is in.
        let (batch, _) = series_batch(vec![40, 41], vec![1.0, 2.0]);
        let out = run(
            batch,
            RangeManipulateSettings {
                start_ms: 100,
                end_ms: 100,
                interval_ms: 60,
                range_ms: 60,
                ..timestamp_value_settings()
            },
        )
        .await;
        let ts_cells = timestamp_cells(&out, "timestamp_range");
        let val_cells = value_cells(&out, "value_range");
        assert2::assert!(ts_cells == vec![vec![41_i64]]);
        assert2::assert!(val_cells == vec![vec![2.0_f64]]);
    }

    #[tokio::test]
    async fn empty_window_produces_empty_cell() {
        // No samples in (40, 100]; the window must be empty, not absent.
        let (batch, _) = series_batch(vec![10, 20], vec![1.0, 2.0]);
        let out = run(
            batch,
            RangeManipulateSettings {
                start_ms: 100,
                end_ms: 100,
                interval_ms: 60,
                range_ms: 60,
                ..timestamp_value_settings()
            },
        )
        .await;
        let ts_cells = timestamp_cells(&out, "timestamp_range");
        assert2::assert!(ts_cells == vec![Vec::<i64>::new()]);
    }

    #[tokio::test]
    async fn multiple_eval_steps_fold_overlapping_windows() {
        // range=60, interval=30. Samples every 25ms.
        let (batch, _) = series_batch(vec![0, 25, 50, 75, 100], vec![0.0, 1.0, 2.0, 3.0, 4.0]);
        let out = run(
            batch,
            RangeManipulateSettings {
                start_ms: 60,
                end_ms: 120,
                interval_ms: 30,
                range_ms: 60,
                ..timestamp_value_settings()
            },
        )
        .await;

        // Eval steps: 60, 90, 120.
        let eval = out
            .column_by_name("timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        check!((0..eval.len()).map(|i| eval.value(i)).collect::<Vec<_>>() == vec![60, 90, 120]);

        let ts_cells = timestamp_cells(&out, "timestamp_range");
        let val_cells = value_cells(&out, "value_range");
        // (0, 60]  -> 25, 50            (0 excluded: 60-60=0, left-open)
        // (30, 90] -> 50, 75
        // (60, 120]-> 75, 100
        check!(ts_cells == vec![vec![25, 50], vec![50, 75], vec![75, 100]]);
        check!(val_cells == vec![vec![1.0, 2.0], vec![2.0, 3.0], vec![3.0, 4.0]]);

        // Labels carried through, one row per eval step.
        let job = out
            .column_by_name("job")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        check!(job.len() == 3);
        check!((0..job.len()).all(|i| job.value(i) == "a"));
    }

    #[tokio::test]
    async fn windows_share_offsets_across_value_and_timestamp() {
        // The two RangeArray columns must be row-aligned: same cell lengths.
        let (batch, _) = series_batch(vec![10, 20, 30], vec![1.0, 2.0, 3.0]);
        let out = run(
            batch,
            RangeManipulateSettings {
                start_ms: 30,
                end_ms: 30,
                interval_ms: 30,
                range_ms: 30,
                ..timestamp_value_settings()
            },
        )
        .await;
        let ts_cells = timestamp_cells(&out, "timestamp_range");
        let val_cells = value_cells(&out, "value_range");
        // (0, 30] -> 10, 20, 30
        assert2::assert!(ts_cells == vec![vec![10, 20, 30]]);
        assert2::assert!(val_cells == vec![vec![1.0, 2.0, 3.0]]);
    }

    #[tokio::test]
    async fn empty_input_series_yields_no_rows() {
        // A series with no samples projects nothing: no labels to repeat and no
        // windows to emit, so the output batch has the extended schema but zero
        // rows.
        let (batch, _) = series_batch(vec![], vec![]);
        let out = run(
            batch,
            RangeManipulateSettings {
                start_ms: 0,
                end_ms: 120,
                interval_ms: 60,
                range_ms: 60,
                ..timestamp_value_settings()
            },
        )
        .await;
        assert2::assert!(out.num_rows() == 0);
        let out_schema = out.schema();
        let names: Vec<&str> = out_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert2::assert!(names == vec!["job", "timestamp", "timestamp_range", "value_range"]);
    }
}

mod build_extended_range_schema;
mod range_array_type;
mod range_manipulate_exec;
mod range_manipulate_type;
mod range_suffix;
mod step_windows;

pub use build_extended_range_schema::build_extended_range_schema;
use range_array_type::range_array_type;
pub use range_manipulate_exec::RangeManipulateExec;
pub use range_manipulate_type::{RangeManipulate, RangeManipulateSettings};
pub use range_suffix::RANGE_SUFFIX;
use step_windows::StepWindows;
