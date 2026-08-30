//! Shared Arrow decode helpers for metrics block codecs.

use arrow::{array::Array, record_batch::RecordBatch};

use crate::histogram::HistogramCodecError;

mod null_required_column;
mod require_non_null;
mod schema_mismatch;
mod typed_column;

#[cfg_attr(test, mutants::skip)]
use null_required_column::null_required_column;
#[cfg_attr(test, mutants::skip)]
pub(crate) use require_non_null::require_non_null;
#[cfg_attr(test, mutants::skip)]
pub(crate) use schema_mismatch::schema_mismatch;
#[cfg_attr(test, mutants::skip)]
pub(crate) use typed_column::typed_column;
