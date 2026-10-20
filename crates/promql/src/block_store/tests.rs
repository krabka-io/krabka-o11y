use std::sync::Arc;

use arrow::{datatypes::SchemaRef, record_batch::RecordBatch};
use assert2::check;
use axum::{body::to_bytes, http::StatusCode};
use krabka_blockstore::{BlockStore, Labels};
use krabka_metrics::{
    CompactionIndexManifest, CompactionObjectPlan, CompactionSeriesLabels, ExemplarRow,
    FloatSampleRow, MetadataRow, MetricBlockKind, encode_exemplar_rows, encode_float_samples,
    encode_metadata_rows, exemplar_schema, float_sample_schema, metadata_schema,
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
    TsdbHeadStats, TsdbStats, prometheus_router,
    test_support::{labels, tenant_id},
};

mod cold_series_labels;
mod counting_object_store;
mod deleted_api_block_store;
mod manifest_store;
mod memory_block_store_fixture;
mod request_counts;

mod exemplar_cases;
mod metadata_sidecar_cases;
mod missing_block_cases;
mod query_scan_cases;
use counting_object_store::CountingObjectStore;
use deleted_api_block_store::deleted_api_block_store;
use exemplar_batch::exemplar_batch;
use expected_stats::{ExpectedStat, expected_stats};
use instant_up_query::{InstantUpQuery, instant_up_query};
use manifest_store::{
    ManifestBlock, api_exemplar_scan, api_exemplars, assert_up_is_one, manifest_store,
};
use memory_block_store_fixture::MemoryBlockStoreFixture;
use request_counts::RequestCounts;
use sidecar_manifest::sidecar_manifest;
use write_float_block::write_float_block;

pub(crate) fn float_sample_row(
    fingerprint: u64,
    timestamp_ms: i64,
    value: f64,
    start_timestamp_ms: Option<i64>,
) -> FloatSampleRow {
    FloatSampleRow {
        fingerprint,
        timestamp_ms,
        value,
        start_timestamp_ms,
    }
}

pub(crate) fn up_api_labels() -> krabka_blockstore::Labels {
    labels(&[
        crate::test_support::TestLabelPair::new("__name__", "up"),
        crate::test_support::TestLabelPair::new("job", "api"),
    ])
}

pub(crate) fn up_api_sample_batch(samples: &[(i64, f64)]) -> RecordBatch {
    let fingerprint = up_api_labels().fingerprint();
    let rows = samples
        .iter()
        .map(|&(timestamp_ms, value)| float_sample_row(fingerprint, timestamp_ms, value, None))
        .collect::<Vec<_>>();
    encode_float_samples(&rows).expect("valid up sample rows")
}

pub(crate) fn compaction_rows_for_series(
    series: &Labels,
    float_rows: Vec<krabka_metrics::FloatRow>,
    histogram_rows: Vec<krabka_metrics::NativeHistogramRow>,
) -> krabka_metrics::TenantCompactionRows {
    let mut series_labels = std::collections::BTreeMap::new();
    series_labels.insert(series.fingerprint(), series.clone().into());
    krabka_metrics::TenantCompactionRows {
        tenant: "tenant-a".to_string(),
        series_labels,
        float_rows,
        histogram_rows,
        exemplar_rows: Vec::new(),
        metadata_rows: Vec::new(),
        clock_rows: Vec::new(),
    }
}

mod support;
pub(crate) use support::{
    exemplar_batch, expected_stats, instant_up_query, sidecar_manifest, write_float_block,
};
