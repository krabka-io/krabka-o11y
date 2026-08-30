use std::collections::BTreeMap;

use arrow::{
    array::{Array, Float64Array, Int64Array, StringArray},
    record_batch::RecordBatch,
};
use krabka_blockstore::{Labels, SeriesFingerprint};

use super::labels::labels_without_metric_name;
use crate::{
    PromqlError,
    error::Result,
    planner::{aggregate::AGGREGATE_VALUE_COLUMN, leaf, over_time_range, rate_range, scalar_math},
    result::{InstantSample, QueryResult, SampleValue},
};

mod assemble_aggregate_batches;
mod assemble_over_time_batches;
mod assemble_rate_batches;
mod assemble_scalar_math_batches;
mod assemble_selector_batches;
mod labels_from_batch;
mod labels_from_rate_batch;

pub(super) use assemble_aggregate_batches::assemble_aggregate_batches;
pub(super) use assemble_over_time_batches::assemble_over_time_batches;
pub(super) use assemble_rate_batches::assemble_rate_batches;
pub(super) use assemble_scalar_math_batches::assemble_scalar_math_batches;
pub(super) use assemble_selector_batches::assemble_selector_batches;
use labels_from_batch::labels_from_batch;
use labels_from_rate_batch::labels_from_rate_batch;
