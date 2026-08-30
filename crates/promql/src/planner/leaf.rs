//! Leaf source and `LogicalPlan` assembly for the instant-vector-selector
//! operator path.
//!
//! The custom operators [`SeriesDivide`], [`SeriesNormalize`], and
//! [`InstantManipulate`] read per-series batches. Each batch carries the label
//! columns of the series plus an `Int64` `timestamp` column and a `Float64`
//! `value` column. The `MetricStore::scan` seam returns fingerprint, timestamp,
//! and value rows with no label columns. This module fills that gap: it
//! materializes the labels of the matched series, keyed by fingerprint, into
//! label columns beside the samples. It then registers the result as an
//! in-memory leaf table and assembles the
//! `SeriesDivide -> SeriesNormalize -> InstantManipulate` chain that selects one
//! sample per series inside the lookback window.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::MemTable,
    logical_expr::{Extension, LogicalPlan},
    prelude::SessionContext,
};
use krabka_blockstore::{Labels, SeriesFingerprint};
use krabka_units::prelude::*;

use crate::{
    PromqlError,
    error::Result,
    extension::{
        instant_manipulate::InstantManipulate, normalize::SeriesNormalize,
        planner::prom_session_context, series_divide::SeriesDivide,
    },
};

// === split-modules: generated submodules ===
mod build_leaf_batch;
mod instant_selector_plan;
mod labeled_sample;
mod leaf_schema;
mod plan_instant_vector_selector;
mod sample_time_column;
mod time_column;
mod value_column;

use build_leaf_batch::build_leaf_batch;
pub use instant_selector_plan::InstantSelectorPlan;
pub use labeled_sample::LabeledSample;
use leaf_schema::leaf_schema;
pub use plan_instant_vector_selector::plan_instant_vector_selector;
pub use sample_time_column::SAMPLE_TIME_COLUMN;
pub use time_column::TIME_COLUMN;
pub use value_column::VALUE_COLUMN;
