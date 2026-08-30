use std::collections::{BTreeMap, BTreeSet};

use arrow::array::{Array, Float64Array, Int64Array, MapArray, StringArray, UInt64Array};
use datafusion::prelude::SessionContext;
use krabka_blockstore::{LabelMatcher, Labels, ScanTableRequest, SeriesFingerprint};
use krabka_metrics::{
    exemplar_schema, float_sample_schema, metadata_schema, native_histogram_schema,
};

use super::MetricBlockStore;
use crate::{
    PromqlError,
    error::Result,
    store::{
        ExemplarRecord, LabelNameCardinality, LabelValueCardinality, MetadataRecord, MetricStore,
        NamedTsdbStat, ScanResult, TsdbBlock, TsdbHeadStats, TsdbStats,
    },
};

// === split-modules: generated submodules ===
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
use named_stats::named_stats;
