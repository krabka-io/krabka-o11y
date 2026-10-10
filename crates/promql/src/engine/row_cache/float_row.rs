use arrow::array::{PrimitiveArray, RecordBatch};

use super::{Array, AsArray, Float64Type, Int64Type, SeriesFingerprint, UInt64Type};

#[derive(Clone, Copy)]
pub(crate) struct FloatRow {
    pub(crate) fp: SeriesFingerprint,
    pub(crate) ts_ms: i64,
    pub(crate) value: f64,
    pub(crate) start_timestamp_ms: Option<i64>,
}

/// The typed columns of one float-table batch, selected as
/// `series_fingerprint, timestamp, value, start_timestamp_ms`.
pub(crate) struct FloatRowColumns<'batch> {
    fingerprints: &'batch PrimitiveArray<UInt64Type>,
    timestamps: &'batch PrimitiveArray<Int64Type>,
    values: &'batch PrimitiveArray<Float64Type>,
    start_timestamps: &'batch PrimitiveArray<Int64Type>,
}

impl<'batch> FloatRowColumns<'batch> {
    /// Borrows the float columns of `batch`, in [`FLOAT_ROW_COLUMNS`] order.
    pub(crate) fn from_batch(batch: &'batch RecordBatch) -> Self {
        Self {
            fingerprints: batch.column(0).as_primitive::<UInt64Type>(),
            timestamps: batch.column(1).as_primitive::<Int64Type>(),
            values: batch.column(2).as_primitive::<Float64Type>(),
            start_timestamps: batch.column(3).as_primitive::<Int64Type>(),
        }
    }

    /// Reads row `row_index` of the batch.
    pub(crate) fn row(&self, row_index: usize) -> FloatRow {
        FloatRow {
            fp: self.fingerprints.value(row_index),
            ts_ms: self.timestamps.value(row_index),
            value: self.values.value(row_index),
            start_timestamp_ms: (!self.start_timestamps.is_null(row_index))
                .then(|| self.start_timestamps.value(row_index)),
        }
    }
}

/// The float-table columns that [`FloatRowColumns::from_batch`] reads.
pub(crate) const FLOAT_ROW_COLUMNS: &str =
    "series_fingerprint, timestamp, value, start_timestamp_ms";
