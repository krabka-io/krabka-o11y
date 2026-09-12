use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Float64Builder, Int64Builder, MapBuilder, StringBuilder, UInt64Builder},
    datatypes::{DataType, Field, SchemaRef},
    record_batch::RecordBatch,
};
use assert2::check;
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use krabka_blockstore::{BlockStore, Labels};
use krabka_metrics::{
    CompactionIndexManifest, CompactionObjectPlan, CompactionSeriesLabels, MetricBlockKind,
    encode_float_samples, exemplar_schema, float_sample_schema, metadata_schema,
};
use krabka_observability::server_security::{ServerSecurity, authenticate_requests};
use object_store::{
    ObjectStore, ObjectStoreExt as _, PutPayload, memory::InMemory, path::Path as ObjectPath,
};
use tower::ServiceExt as _;

use super::MetricBlockStore;
use crate::{
    Annotations, EngineOpts, InstantSample, MetadataRecord, MetricStore, NamedTsdbStat,
    PrometheusApiState, PromqlEngine, PromqlError, QueryResult, SampleValue, TsdbBlock,
    TsdbHeadStats, TsdbStats, prometheus_router, test_support::tenant_id,
};

mod a_corrupt_block_still_fails_the_query;
mod a_query_answers_around_a_deleted_block_and_warns;
mod a_query_over_present_blocks_raises_no_warning;
mod an_http_query_over_a_deleted_block_warns_in_the_response;
mod exemplar_batch;
mod exemplar_batch_from_rows;
mod exemplars_answer_around_a_deleted_block_and_warn;
mod exemplars_include_closed_range_boundaries_and_filter_outside_rows;
mod exemplars_reads_compacted_exemplar_sidecar_blocks;
mod expected_stats;
mod index_metadata_methods_report_float_and_histogram_series;
mod labels;
mod metadata_answers_around_a_deleted_block_and_warns;
mod metadata_batch;
mod metadata_reads_compacted_metadata_sidecar_blocks;
mod prometheus_query_reads_float_samples_from_blockstore;
mod prometheus_query_rebuilds_float_index_from_compaction_manifest;
mod sidecar_manifest;
mod tsdb_blocks_reports_compaction_manifest_blocks;
mod write_float_block;

use exemplar_batch::exemplar_batch;
use exemplar_batch_from_rows::exemplar_batch_from_rows;
use expected_stats::expected_stats;
use labels::labels;
use metadata_batch::metadata_batch;
use sidecar_manifest::sidecar_manifest;
use write_float_block::write_float_block;
