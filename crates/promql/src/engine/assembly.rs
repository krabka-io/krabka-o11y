use std::collections::BTreeMap;

use arrow::{
    array::{Array, BinaryArray, Float64Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use krabka_blockstore::SeriesFingerprint;

use super::step_vectors::GridPoint;
use crate::{
    PromqlError, PromqlLabels as Labels,
    error::Result,
    planner::{
        StepGrid, aggregate::AGGREGATE_VALUE_COLUMN, leaf, over_time_range, rate_range, scalar_math,
    },
    result::{FloatSample, InstantSample, QueryResult, SampleValue},
};

mod assemble_aggregate_batches;
mod assemble_float_vector_batches;
mod assemble_range_fold_grid;
mod assemble_scalar_math_batches;
mod assemble_selector_batches;
mod assemble_selector_grid;
mod labels_from_batch;
mod row_fingerprints;

pub(super) use assemble_aggregate_batches::assemble_aggregate_batches;
use assemble_float_vector_batches::assemble_float_vector_batches;
pub(super) use assemble_range_fold_grid::assemble_range_fold_grid;
pub(super) use assemble_scalar_math_batches::assemble_scalar_math_batches;
pub(super) use assemble_selector_batches::assemble_selector_batches;
pub(super) use assemble_selector_grid::assemble_selector_grid;
use labels_from_batch::{labels_from_batch, labels_from_batch_columns};
use row_fingerprints::row_fingerprints;

fn float64_value_column(batch: &RecordBatch) -> Result<&Float64Array> {
    batch
        .column_by_name(leaf::VALUE_COLUMN)
        .and_then(|column| column.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| PromqlError::Exec("planner leaf missing Float64 value column".to_string()))
}

/// Reconstructs a [`Labels`] set from the string label columns of one row of a
/// rate-range projection output batch.
///
/// The rate projection carries only label (`Utf8`) columns plus the float
/// `value` result column, so every non-`value` `Utf8` column is a label.
pub(crate) fn labels_from_rate_batch(batch: &RecordBatch, row: usize) -> Labels {
    labels_from_batch_columns(batch, row, |name| name == rate_range::RATE_VALUE_COLUMN)
}

/// Assembles `*_over_time` projection output batches into a result.
///
/// `preserve_metric_name` keeps `__name__` only for `last_over_time`. Empty
/// windows are omitted while genuine NaN results are retained.
pub(crate) fn assemble_over_time_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
    preserve_metric_name: bool,
) -> Result<QueryResult> {
    assemble_float_vector_batches(
        batches,
        labels_by_fp,
        time_ms,
        over_time_range::OVER_TIME_VALUE_COLUMN,
        "over_time projection missing Float64 value column",
        !preserve_metric_name,
    )
}

/// Assembles rate-family projection output batches into a result.
///
/// Empty windows are omitted while genuine NaN results are retained. Rate
/// families always drop the metric name.
pub(crate) fn assemble_rate_batches(
    batches: &[RecordBatch],
    labels_by_fp: &BTreeMap<SeriesFingerprint, Labels>,
    time_ms: i64,
) -> Result<QueryResult> {
    assemble_float_vector_batches(
        batches,
        labels_by_fp,
        time_ms,
        rate_range::RATE_VALUE_COLUMN,
        "rate projection missing Float64 value column",
        true,
    )
}
