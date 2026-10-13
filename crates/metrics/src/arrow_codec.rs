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

mod null_required_column;
mod require_non_null;
mod schema_mismatch;
mod typed_column;

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
use null_required_column::null_required_column;
#[cfg_attr(test, mutants::skip)]
pub(crate) use require_non_null::require_non_null;
#[cfg_attr(test, mutants::skip)]
pub(crate) use schema_mismatch::schema_mismatch;
#[cfg_attr(test, mutants::skip)]
pub(crate) use typed_column::typed_column;
