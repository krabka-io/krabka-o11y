use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Float64Builder, Int64Builder, MapBuilder, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field},
    record_batch::RecordBatch,
};
use assert2::check;
use krabka_blockstore::{BlockStore, Labels};
use krabka_metrics::{
    CompactionIndexManifest, CompactionObjectPlan, CompactionSeriesLabels, MetricBlockKind,
    encode_float_samples, exemplar_schema, float_sample_schema, metadata_schema,
};
use object_store::{ObjectStore, memory::InMemory};

use super::MetricBlockStore;
use crate::{
    EngineOpts, InstantSample, MetadataRecord, MetricStore, NamedTsdbStat, PromqlEngine,
    QueryResult, SampleValue, TsdbBlock, TsdbHeadStats, TsdbStats,
};

mod exemplar_batch;
mod exemplar_batch_from_rows;
mod exemplars_include_closed_range_boundaries_and_filter_outside_rows;
mod exemplars_reads_compacted_exemplar_sidecar_blocks;
mod expected_stats;
mod index_metadata_methods_report_float_and_histogram_series;
mod labels;
mod metadata_batch;
mod metadata_reads_compacted_metadata_sidecar_blocks;
mod prometheus_query_reads_float_samples_from_blockstore;
mod prometheus_query_rebuilds_float_index_from_compaction_manifest;
mod tsdb_blocks_reports_compaction_manifest_blocks;

use exemplar_batch::exemplar_batch;
use exemplar_batch_from_rows::exemplar_batch_from_rows;
use expected_stats::expected_stats;
use labels::labels;
use metadata_batch::metadata_batch;
