//! Leaf source and `LogicalPlan` assembly for the instant-vector-selector
//! operator path.
//!
//! The custom operators [`SeriesDivide`], [`SeriesNormalize`], and
//! [`InstantManipulate`] read per-series batches. Each batch carries the label
//! columns of the series plus an `Int64` `timestamp` column and a `Float64`
//! `value` column. The `MetricStore::scan` seam returns fingerprint, timestamp,
//! and value rows with no label columns. This module fills that gap: it
//! materializes the labels of the matched series, keyed by fingerprint, into
//! label columns beside the samples. It then wraps the result in a catalog-free
//! in-memory `TableScan` (see [`leaf_scan`]) and assembles the
//! `SeriesDivide -> SeriesNormalize -> InstantManipulate` chain that selects one
//! sample per series inside the lookback window.

use std::{collections::BTreeSet, sync::Arc};

use arrow::{
    array::{ArrayRef, BinaryBuilder, Float64Array, Int64Array},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    catalog::MemTable,
    datasource::provider_as_source,
    logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder},
    prelude::SessionContext,
};
use krabka_blockstore::SeriesFingerprint;
use krabka_units::prelude::*;

use super::{LabeledSeries, StepGrid};
use crate::{
    PromqlError, PromqlLabels as Labels,
    error::Result,
    extension::{
        instant_manipulate::InstantManipulate, normalize::SeriesNormalize,
        planner::prom_session_context, series_divide::SeriesDivide,
    },
};

mod build_leaf_batch;
mod leaf_scan;
mod plan_instant_vector_selector;
mod range_udf_plan;
mod series_leaf;

use build_leaf_batch::build_leaf_batch;
pub(crate) use leaf_scan::leaf_scan;
pub use plan_instant_vector_selector::plan_instant_vector_selector;
pub(crate) use range_udf_plan::{RangeWindows, WindowUdf, range_udf_plan};
pub(crate) use series_leaf::{SeriesLeaf, divide_and_normalize, series_leaf};

/// The assembled operator plan plus labels needed to reattach series labels.
pub struct InstantSelectorPlan {
    /// Session context whose planner understands the custom operators.
    pub ctx: SessionContext,
    /// The logical plan for the instant-selector chain.
    pub plan: LogicalPlan,
    /// Series labels keyed by fingerprint, for assembling the result.
    pub labels_by_fp: std::collections::BTreeMap<SeriesFingerprint, Labels>,
}

/// Leaf-batch column that keeps the original sample timestamp.
///
/// The operator chain carries this through unchanged, so the engine can
/// recover the selected sample's timestamp after evaluation.
pub const SAMPLE_TIME_COLUMN: &str = "sample_timestamp";

/// Whether a leaf batch carries a sample-time duplicate of its timestamp.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum SampleTimePresence {
    Included,
    Omitted,
}

/// Leaf-batch column with the per-sample timestamp in epoch milliseconds.
///
/// This column is the time index of the operator chain, so
/// [`InstantManipulate`] rewrites it to the grid eval timestamp on output.
pub const TIME_COLUMN: &str = "timestamp";

/// Leaf-batch column carrying the per-sample float value.
pub const VALUE_COLUMN: &str = "value";

/// The leaf-batch schema: one nullable `Binary` column per label name, then the
/// timestamp and value columns, then the sample-time duplicate when
/// `sample_time` includes it.
pub(crate) fn leaf_schema(label_names: &[String], sample_time: SampleTimePresence) -> Arc<Schema> {
    let mut fields = Vec::with_capacity(label_names.len() + 3);
    for name in label_names {
        // Label columns are nullable so an ABSENT label (NULL) is distinguishable
        // from a PRESENT-but-empty-valued label (`""`). The reconstruction
        // (`engine::labels_from_batch`) maps NULL -> absent and `""` ->
        // present-empty, preserving the byte-exact label set through the chain.
        fields.push(Field::new(name, DataType::Binary, true));
    }
    fields.push(Field::new(TIME_COLUMN, DataType::Int64, false));
    fields.push(Field::new(VALUE_COLUMN, DataType::Float64, false));
    if sample_time == SampleTimePresence::Included {
        fields.push(Field::new(SAMPLE_TIME_COLUMN, DataType::Int64, false));
    }
    Arc::new(Schema::new(fields))
}
