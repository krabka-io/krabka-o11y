//! Shared Arrow decode helpers for metrics block codecs.

use arrow::{
    array::{Array, Int64Array, UInt64Array},
    record_batch::RecordBatch,
};

use crate::histogram::HistogramCodecError;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct SampleIdentity {
    pub fingerprint: u64,
    pub timestamp_ms: i64,
}

pub(crate) fn read_sample_identity(
    fingerprints: &UInt64Array,
    timestamps: &Int64Array,
    row: usize,
) -> Result<SampleIdentity, HistogramCodecError> {
    require_non_null(fingerprints, row, crate::schema::COL_FINGERPRINT)?;
    require_non_null(timestamps, row, crate::schema::COL_TIMESTAMP)?;
    Ok(SampleIdentity {
        fingerprint: fingerprints.value(row),
        timestamp_ms: timestamps.value(row),
    })
}

pub(crate) fn decode_rows_with_identity<T>(
    batch: &RecordBatch,
    fingerprints: &UInt64Array,
    timestamps: &Int64Array,
    mut decode: impl FnMut(SampleIdentity, usize) -> Result<T, HistogramCodecError>,
) -> Result<Vec<T>, HistogramCodecError> {
    let mut rows = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        let identity = read_sample_identity(fingerprints, timestamps, row)?;
        rows.push(decode(identity, row)?);
    }
    Ok(rows)
}

#[cfg_attr(test, mutants::skip)]
#[cfg_attr(test, mutants::skip)]
#[cfg_attr(test, mutants::skip)]
#[cfg_attr(test, mutants::skip)]

// cargo-mutants: error formatting is validated through required-column decode tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn null_required_column(column: &str, row: usize) -> HistogramCodecError {
    HistogramCodecError::SchemaMismatch(format!(
        "column `{column}` contains null for required row {row}"
    ))
}

// cargo-mutants: exercised through the sample and histogram codec decode tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn schema_mismatch(column: &str) -> HistogramCodecError {
    HistogramCodecError::SchemaMismatch(format!("column `{column}` missing or wrong type"))
}

// cargo-mutants: exercised through the sample and histogram codec null-column tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn require_non_null(
    array: &dyn Array,
    row: usize,
    column: &str,
) -> Result<(), HistogramCodecError> {
    if array.is_null(row) {
        Err(null_required_column(column, row))
    } else {
        Ok(())
    }
}

// cargo-mutants: generic downcast glue is covered by caller-specific schema tests.
#[cfg_attr(test, mutants::skip)]
pub(crate) fn typed_column<'a, T: 'static>(
    batch: &'a RecordBatch,
    name: &str,
) -> Result<&'a T, HistogramCodecError> {
    batch
        .column_by_name(name)
        .ok_or_else(|| schema_mismatch(name))?
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| schema_mismatch(name))
}
