use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{Array, Float64Array, Int64Array, MapArray, StringArray, UInt64Array};
use datafusion::prelude::SessionContext;
use krabka_blockstore::{
    BlockSkipReason, LabelMatcher, Labels, ScanReport, ScanTableRequest, SeriesFingerprint,
};
use krabka_metrics::{
    exemplar_schema, float_sample_schema, metadata_schema, native_histogram_schema,
};

use super::MetricBlockStore;
use crate::{
    PromqlError,
    error::Result,
    store::{
        ExemplarRecord, ExemplarScan, LabelNameCardinality, LabelValueCardinality, MetadataRecord,
        MetadataScan, MetricStore, NamedTsdbStat, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
    },
};

mod append_exemplar_label_map;
mod blockstore_error;
mod datafusion_error;
mod exemplar_table;
mod exemplars_from_batch;
mod float_table;
mod histogram_table;
mod metadata_from_batch;
mod metadata_table;
mod metric_block_store;
mod missing_block_warnings;
mod named_stats;

use append_exemplar_label_map::append_exemplar_label_map;
use blockstore_error::blockstore_error;
use datafusion_error::datafusion_error;
use exemplar_table::EXEMPLAR_TABLE;
use exemplars_from_batch::exemplars_from_batch;
use float_table::FLOAT_TABLE;
use histogram_table::HISTOGRAM_TABLE;
use metadata_from_batch::metadata_from_batch;
use metadata_table::METADATA_TABLE;
use missing_block_warnings::missing_block_warnings;
use named_stats::named_stats;
