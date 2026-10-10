use std::collections::BTreeSet;

use krabka_blockstore::{
    BlockDescriptor, LabelIndex, LabelPredicate, LogBlockIndex as BlockIndex,
    LogBlockStoreError as BlockStoreError, LogSeriesFingerprint as SeriesFingerprint, TimeRange,
};
use thiserror::Error;
use tracing::field::Empty;

use crate::{LabelMatcher, StreamQuery};

mod label_predicate;
mod plan_error;
mod plan_stream_query;
mod stream_plan;

use label_predicate::label_predicate;
pub use plan_error::PlanError;
pub use plan_stream_query::plan_stream_query;
pub use stream_plan::StreamPlan;
