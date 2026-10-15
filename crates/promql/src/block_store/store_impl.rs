use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{Array, Float64Array, Int64Array, MapArray, StringArray, UInt64Array};
use datafusion::prelude::SessionContext;
use krabka_blockstore::{BlockSkipReason, ScanReport, ScanTableRequest, SeriesFingerprint};
use krabka_metrics::{
    exemplar_schema, float_sample_schema, metadata_schema, native_histogram_schema,
};

use super::MetricBlockStore;
use crate::{
    PromqlError, PromqlLabels as Labels, PromqlMatcher as LabelMatcher,
    error::Result,
    store::{
        ExemplarRecord, ExemplarScan, LabelNameCardinality, LabelValueCardinality, MetadataRecord,
        MetadataScan, MetricStore, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
    },
};
mod exemplars_from_batch;
mod metadata_from_batch;
mod metric_block_store;
mod missing_block_warnings;
use exemplars_from_batch::exemplars_from_batch;
use metadata_from_batch::metadata_from_batch;
use missing_block_warnings::missing_block_warnings;

use crate::series_stats::{SeriesRef, label_name_cardinality, label_value_cardinality, tsdb_stats};

pub(crate) const EXEMPLAR_TABLE: &str = "metric_exemplars";

pub(crate) const FLOAT_TABLE: &str = "metric_float_samples";

pub(crate) const HISTOGRAM_TABLE: &str = "metric_native_histograms";

pub(crate) const METADATA_TABLE: &str = "metric_metadata";

pub(crate) fn blockstore_error(error: krabka_blockstore::BlockStoreError) -> PromqlError {
    let message = error.to_string();
    drop(error);
    PromqlError::Store(message)
}

pub(crate) fn datafusion_error(error: datafusion::error::DataFusionError) -> PromqlError {
    let message = error.to_string();
    drop(error);
    PromqlError::Store(message)
}

pub(crate) fn append_exemplar_label_map(
    labels: &mut Labels,
    label_maps: &MapArray,
    row: usize,
) -> Result<()> {
    let entries = label_maps.value(row);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| PromqlError::Store("exemplar label map key column has wrong type".into()))?;
    let values = entries
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| {
            PromqlError::Store("exemplar label map value column has wrong type".into())
        })?;
    for index in 0..keys.len() {
        if !keys.is_null(index) && !values.is_null(index) {
            labels.insert(keys.value(index), values.value(index));
        }
    }
    Ok(())
}
