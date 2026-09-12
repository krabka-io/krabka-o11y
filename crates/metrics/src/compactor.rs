//! Deterministic compactor core for metrics WAL records.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use arrow::{
    array::{
        ArrayRef, BooleanArray, BooleanBuilder, Float64Builder, Int64Array, Int64Builder,
        MapBuilder, StringBuilder, StringDictionaryBuilder, UInt32Builder, UInt64Array,
        UInt64Builder,
    },
    compute::filter_record_batch,
    datatypes::{DataType, Field, Int32Type},
    error::ArrowError,
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use krabka_blockstore::{
    BlockDeletion, BlockDeletionFailure, BlockDeletionReport, BlockLevel, BlockMeta,
    BlockStoreError, BlockTimestampUnit, BlockWriter, CompactionCandidate, CompactionJob,
    CompactionPolicy, DEFAULT_BLOCK_SWEEP_GRACE, ERASURE_REQUEST_PREFIX, ErasureRequest, Index,
    Labels, LifecycleError, MERGE_BATCH_ROWS, MERGE_READ_BATCH_ROWS, ObjectStoreMetrics,
    ObjectStoreRetryPolicy, OrphanSweepStats, RetentionWindows, RetryingObjectStore, SortedMerge,
    SummaryColumns, delete_blocks, escape_object_path_segment, input_key_fingerprint,
    list_erasure_requests, open_block_stream, plan_compactions, plan_expired_blocks,
    reconcile_orphans, series_block_schema, versioned_compaction_key,
};
use krabka_client_consumer::{AutoOffsetReset, Consumer, ConsumerError, ConsumerRecord};
use krabka_ids::{Offset, PartitionIndex};
use krabka_telemetry::propagation::{TRACEPARENT, set_remote_parent};
use krabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use serde::{Deserialize, Serialize};
use tracing::Instrument as _;

use crate::{
    NativeHistogram,
    arrow_codec::typed_column,
    encode_float_samples, encode_native_histograms,
    histogram::HistogramCodecError,
    metrics::ServiceMetrics,
    schema::{
        CCOL_CLOCK, CCOL_EST_ERROR_NANOS, CCOL_FREQUENCY_PPB, CCOL_GM_CLOCK_ACCURACY,
        CCOL_GM_CLOCK_CLASS, CCOL_GNSS_FIX, CCOL_INGEST_UNIX_NANOS, CCOL_LAST_STEP_NANOS,
        CCOL_LAST_SYNC_UNIX_NANOS, CCOL_MAX_ERROR_NANOS, CCOL_MEAN_PATH_DELAY_NANOS, CCOL_NODE,
        CCOL_OFFSET_NANOS, CCOL_READING_UNIX_NANOS, CCOL_REFERENCE_ID, CCOL_ROOT_DELAY_NANOS,
        CCOL_ROOT_DISPERSION_NANOS, CCOL_SATELLITES_USED, CCOL_SOURCE_KIND, CCOL_STEPS_REMOVED,
        CCOL_STRATUM, CCOL_SYNC_STATE, CCOL_UNCERTAINTY_NANOS, CCOL_UNSYNCHRONIZED,
        COL_FINGERPRINT, COL_TIMESTAMP, clock_reading_schema, exemplar_schema, metadata_schema,
    },
    wal::{ClockReadingPayload, SamplePayload, WalError, WalExemplar, WalRecord},
    wire::{GnssFix, UnixNanos},
};

#[cfg(test)]
mod tests {

    /// A buffer flushes on either threshold, and on neither when empty. The
    /// row and age thresholds are checked at their own boundary with the other
    /// far from its own, so each is shown to be sufficient by itself -- a
    /// buffer that flushed only when both were met would pass a test that
    /// crossed them together.
    #[test]
    fn a_compaction_buffer_flushes_on_rows_or_age_but_never_when_empty() {
        use std::time::{Duration, Instant};

        let config = super::CompactionLoopConfig {
            wal_topic: "wal".into(),
            poll_timeout: secs(1),
            flush_max_rows: 3,
            flush_max_age: secs(10),
        };
        let record = |offset: i64| super::CompactionWalRecord {
            partition: krabka_ids::PartitionIndex(0),
            offset: krabka_ids::Offset(offset),
            value: Vec::new(),
        };
        let now = Instant::now();

        // Empty flushes on neither threshold, however old the clock claims to be.
        let empty = super::CompactionBuffer::new();
        check!(!empty.should_flush(&config, now));
        check!(
            !empty.should_flush(&config, now + Duration::from_hours(1)),
            "an empty buffer has nothing to age"
        );

        // Rows alone, with the age nowhere near its threshold.
        let mut by_rows = super::CompactionBuffer::new();
        by_rows.extend(vec![record(1), record(2)], now);
        check!(!by_rows.should_flush(&config, now), "two of three rows");
        by_rows.extend(vec![record(3)], now);
        check!(
            by_rows.should_flush(&config, now),
            "the third row is enough"
        );

        // Age alone, with the row count nowhere near its threshold.
        let mut by_age = super::CompactionBuffer::new();
        by_age.extend(vec![record(1)], now);
        check!(
            !by_age.should_flush(&config, now + Duration::from_secs(9)),
            "one second short"
        );
        check!(
            by_age.should_flush(&config, now + Duration::from_secs(10)),
            "exactly the age threshold is enough"
        );

        // The age is measured from the first record in, not the most recent,
        // so a later arrival does not reset the deadline.
        let mut anchored = super::CompactionBuffer::new();
        anchored.extend(vec![record(1)], now);
        anchored.extend(vec![record(2)], now + Duration::from_secs(9));
        check!(
            anchored.should_flush(&config, now + Duration::from_secs(10)),
            "the deadline follows the oldest record"
        );
    }
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use assert2::{assert, check};
    use async_trait::async_trait;
    use futures::{StreamExt as _, stream::BoxStream};
    use krabka_blockstore::{BlockDeletionFailure, Labels, OrphanSweepStats, RetentionWindows};
    use krabka_units::prelude::*;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        ObjectStoreExt, PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory,
        path::Path,
    };

    use super::{
        CompactionLoopContext, CompactionRetentionPhase, ObjectStoreMetrics,
        ObjectStoreRetryPolicy, RetryingObjectStore, ServiceMetrics, compact_wal_records,
        encode_tenant_batches,
    };
    use crate::{
        BucketSpan, FloatRow, NativeHistogram, ResetHint,
        distributor::wal_records_from_series,
        wal::{SamplePayload, WalExemplar, WalRecord},
        wire::{DecodedExemplar, DecodedSample, DecodedSeries},
    };

    fn labels(pairs: &[(&str, &str)]) -> Labels {
        pairs
            .iter()
            .map(|(name, value)| ((*name).to_string(), (*value).to_string()))
            .collect()
    }

    fn float_record(tenant: &str, metric: &str, job: &str, timestamp_ms: i64) -> WalRecord {
        WalRecord {
            tenant: tenant.to_string(),
            labels: vec![
                ("__name__".into(), metric.into()),
                ("job".into(), job.into()),
            ],
            payload: SamplePayload::Float {
                timestamp_ms,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        }
    }

    fn hist() -> NativeHistogram {
        NativeHistogram {
            schema: 1,
            is_float: false,
            reset_hint: ResetHint::No,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count: 3.0,
            sum: 6.0,
            positive_spans: vec![BucketSpan {
                offset: 0,
                length: 1,
            }],
            positive_counts: vec![3.0],
            negative_spans: Vec::new(),
            negative_counts: Vec::new(),
            custom_values: None,
            start_timestamp_ms: Some(10),
        }
    }

    /// Each block kind draws its fingerprints from its own row collection.
    /// Every row kind here carries a different fingerprint, so a kind reading
    /// its neighbour's rows returns the wrong series -- with one shared
    /// fingerprint, or with only float rows populated, all four arms agree.
    #[test]
    fn each_block_kind_draws_its_fingerprints_from_its_own_rows() {
        let named = |name: &str| {
            let mut labels = Labels::new();
            labels.insert("__name__", name);
            labels
        };
        let rows = super::TenantCompactionRows {
            tenant: "t".to_string(),
            series_labels: std::collections::BTreeMap::from([
                (1, named("float")),
                (2, named("histogram")),
                (3, named("exemplar")),
                (4, named("metadata")),
            ]),
            float_rows: vec![FloatRow {
                fingerprint: 1,
                timestamp_ms: 0,
                value: 0.0,
                start_timestamp_ms: None,
            }],
            histogram_rows: vec![super::NativeHistogramRow {
                fingerprint: 2,
                timestamp_ms: 0,
                hist: hist(),
            }],
            exemplar_rows: vec![super::ExemplarRow {
                fingerprint: 3,
                timestamp_ms: 0,
                value: 0.0,
                trace_id: None,
                span_id: None,
                labels: Vec::new(),
            }],
            metadata_rows: vec![super::MetadataRow {
                fingerprint: 4,
                metric_family_name: String::new(),
                metric_type: String::new(),
                help: String::new(),
                unit: String::new(),
            }],
            // `ClockReadings` arrived after this test did, and building a
            // reading needs a whole decoded payload. The four kinds below are
            // what it pins.
            clock_rows: Vec::new(),
        };

        for (kind, want) in [
            (super::MetricBlockKind::Float, 1_u64),
            (super::MetricBlockKind::NativeHistograms, 2),
            (super::MetricBlockKind::Exemplars, 3),
            (super::MetricBlockKind::Metadata, 4),
        ] {
            let series = super::series_labels_for_kind(&rows, kind);
            check!(
                series.iter().map(|s| s.fingerprint).collect::<Vec<_>>() == vec![want],
                "{kind:?}"
            );
        }
    }

    #[test]
    fn compact_wal_records_groups_by_tenant_and_sorts_rows() {
        let a_late = float_record("tenant-a", "up", "api", 30);
        let mut a_early = float_record("tenant-a", "up", "api", 10);
        let SamplePayload::Float {
            start_timestamp_ms, ..
        } = &mut a_early.payload
        else {
            panic!("expected float payload");
        };
        *start_timestamp_ms = Some(5);
        let b_row = float_record("tenant-b", "up", "api", 20);

        let compacted = compact_wal_records(&[a_late.clone(), b_row.clone(), a_early.clone()]);

        check!(compacted.len() == 2);
        check!(compacted[0].tenant == "tenant-a");
        check!(compacted[1].tenant == "tenant-b");
        check!(compacted[0].float_rows.len() == 2);
        check!(compacted[0].float_rows[0].timestamp_ms == 10);
        check!(compacted[0].float_rows[1].timestamp_ms == 30);
        check!(compacted[0].float_rows[0].fingerprint == a_early.series_fingerprint());
        check!(compacted[0].float_rows[0].start_timestamp_ms == Some(5));
        check!(compacted[1].float_rows[0].fingerprint == b_row.series_fingerprint());
    }

    #[test]
    fn compaction_object_keys_are_deterministic_by_tenant_kind_and_offsets() {
        let cases = [
            (
                super::MetricBlockKind::Float,
                "metrics/tenant!2Fa/float/00000000000000000042-00000000000000000099.parquet",
            ),
            (
                super::MetricBlockKind::NativeHistograms,
                "metrics/tenant!2Fa/native-histograms/00000000000000000042-00000000000000000099.parquet",
            ),
            (
                super::MetricBlockKind::Exemplars,
                "metrics/tenant!2Fa/exemplars/00000000000000000042-00000000000000000099.parquet",
            ),
        ];
        for (kind, expected) in cases {
            check!(
                super::compaction_object_key("tenant/a", kind, 42, 99) == expected,
                "kind {kind:?}"
            );
        }
    }

    /// A tenant of exactly `.` or `..` must not survive as a relative path
    /// segment in the object key. An interior dot is part of a valid tenant
    /// name and stays as it is.
    #[test]
    fn tenant_dot_segments_cannot_form_path_traversal() {
        let key =
            |tenant| super::compaction_object_key(tenant, super::MetricBlockKind::Float, 42, 99);
        check!(
            key("..") == "metrics/!2E!2E/float/00000000000000000042-00000000000000000099.parquet"
        );
        check!(key(".") == "metrics/!2E/float/00000000000000000042-00000000000000000099.parquet");
        check!(key("a.b") == "metrics/a.b/float/00000000000000000042-00000000000000000099.parquet");
    }

    /// Both key builders put the tenant in one escaped segment, and the
    /// object store keeps that key byte for byte. The retention sweep compares
    /// a listed location with the key in a manifest, so an escape that the
    /// store rewrote would make the two differ. The segment also reads back as
    /// the tenant it came from.
    #[test]
    fn an_escaped_tenant_is_one_key_segment_that_reads_back_as_the_tenant() {
        let cases = [
            ("plain", "tenant-a", "tenant-a"),
            ("star and parentheses", "team*(1)", "team!2A!281!29"),
            ("bang", "a!b", "a!21b"),
            ("apostrophe", "o'brien", "o!27brien"),
            ("separator", "tenant/a", "tenant!2Fa"),
            ("dot dot", "..", "!2E!2E"),
        ];
        for (name, tenant, segment) in cases {
            let keys = [
                (
                    4,
                    super::compaction_object_key(tenant, super::MetricBlockKind::Float, 42, 99),
                ),
                (
                    5,
                    super::compaction_partition_object_key(
                        tenant,
                        super::MetricBlockKind::Float,
                        super::PartitionIndex(3),
                        42,
                        99,
                    ),
                ),
                // A merged block. The `compacted` segment is what keeps the
                // level-keyed names apart from the offset-keyed ones in a
                // listing, and the tenant is escaped by the same rule.
                (
                    5,
                    super::compacted_metric_object_key(
                        &krabka_blockstore::CompactionJob {
                            tenant: tenant.to_string(),
                            input_keys: vec!["a".to_string(), "b".to_string()],
                            output_level: krabka_blockstore::BlockLevel(1),
                            min_ts: 42,
                            max_ts: 99,
                            row_count: 2,
                        },
                        super::MetricBlockKind::Float,
                    ),
                ),
            ];
            for (segment_count, key) in keys {
                let segments = key.split('/').collect::<Vec<_>>();
                check!(segments.len() == segment_count, "{name}: {key}");
                check!(segments[0] == "metrics", "{name}");
                check!(segments[1] == segment, "{name}");
                check!(
                    krabka_blockstore::unescape_object_path_segment(segments[1])
                        == Some(tenant.to_string()),
                    "{name}"
                );
                check!(
                    object_store::path::Path::from(key.as_str()).as_ref() == key,
                    "{name}"
                );
            }
        }
    }

    #[test]
    fn compaction_object_plan_pairs_block_and_index_keys() {
        let plan = super::compaction_object_plan("tenant/a", super::MetricBlockKind::Float, 42, 99);

        assert!(
            plan.block_key
                == "metrics/tenant!2Fa/float/00000000000000000042-00000000000000000099.parquet"
        );
        assert!(
            plan.index_key
                == "metrics/tenant!2Fa/float/00000000000000000042-00000000000000000099.index"
        );
    }

    #[test]
    fn compaction_object_plan_records_offset_window_and_row_count() {
        let compacted = compact_wal_records(&[
            float_record("tenant-a", "up", "api", 10),
            float_record("tenant-a", "up", "api", 20),
        ]);

        let plan = super::compaction_object_plan_for_rows(
            &compacted[0],
            super::MetricBlockKind::Float,
            42,
            99,
        );

        assert!(
            plan == super::CompactionObjectPlan {
                block_key:
                    "metrics/tenant-a/float/00000000000000000042-00000000000000000099.parquet"
                        .to_string(),
                index_key: "metrics/tenant-a/float/00000000000000000042-00000000000000000099.index"
                    .to_string(),
                first_offset: 42,
                last_offset: 99,
                row_count: 2,
            }
        );
    }

    #[test]
    fn compaction_index_manifest_round_trips() {
        let plan = super::CompactionObjectPlan {
            block_key: "metrics/tenant-a/float/00000000000000000042-00000000000000000099.parquet"
                .to_string(),
            index_key: "metrics/tenant-a/float/00000000000000000042-00000000000000000099.index"
                .to_string(),
            first_offset: 42,
            last_offset: 99,
            row_count: 2,
        };

        let block_meta = krabka_blockstore::BlockMeta {
            tenant: "tenant-a".to_string(),
            object_key: plan.block_key.clone(),
            min_ts: 1_000,
            max_ts: 2_000,
            row_count: 2,
            fingerprints: vec![7, 9],
            level: krabka_blockstore::BlockLevel::INGESTED,
        };
        let manifest = super::CompactionIndexManifest::from_block_meta(
            super::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![super::CompactionSeriesLabels {
                fingerprint: 7,
                labels: labels(&[("__name__", "up")]),
            }],
        );
        let encoded = manifest.encode().expect("encode manifest");
        let decoded = super::CompactionIndexManifest::decode(&encoded).expect("decode manifest");

        assert!(decoded == manifest);
        assert!(
            decoded
                == super::CompactionIndexManifest {
                    tenant: "tenant-a".to_string(),
                    kind: super::MetricBlockKind::Float,
                    block_key:
                        "metrics/tenant-a/float/00000000000000000042-00000000000000000099.parquet"
                            .to_string(),
                    index_key:
                        "metrics/tenant-a/float/00000000000000000042-00000000000000000099.index"
                            .to_string(),
                    level: krabka_blockstore::BlockLevel::INGESTED,
                    first_offset: 42,
                    last_offset: 99,
                    row_count: 2,
                    min_ts: 1_000,
                    max_ts: 2_000,
                    fingerprints: vec![7, 9],
                    series: vec![super::CompactionSeriesLabels {
                        fingerprint: 7,
                        labels: labels(&[("__name__", "up")]),
                    }],
                }
        );
    }

    #[tokio::test]
    async fn object_store_compaction_index_sink_writes_encoded_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let sink = super::ObjectStoreCompactionIndexSink::new(object_store.clone());
        let plan = super::CompactionObjectPlan {
            block_key: "metrics/tenant-a/float/partition=0000000003/00000000000000000042-00000000000000000099.parquet"
                .to_string(),
            index_key: "metrics/tenant-a/float/partition=0000000003/00000000000000000042-00000000000000000099.index"
                .to_string(),
            first_offset: 42,
            last_offset: 99,
            row_count: 2,
        };
        let manifest = super::CompactionIndexManifest::from_plan(
            "tenant-a",
            super::MetricBlockKind::Float,
            &plan,
        );

        super::CompactionIndexSink::write_manifest(&sink, &manifest)
            .await
            .expect("write manifest");
        let bytes = object_store
            .get(&object_store::path::Path::from(manifest.index_key.clone()))
            .await
            .expect("get manifest")
            .bytes()
            .await
            .expect("manifest bytes");
        let decoded =
            super::CompactionIndexManifest::decode(&bytes).expect("decode persisted manifest");

        assert!(decoded == manifest);
    }

    /// A wall-clock instant well inside the range every unit can express.
    const NOW_MS: i64 = 1_700_000_000_000;
    const REFUSING_STORE: &str = "refuses-one-delete";
    const REFUSAL: &str = "this object will not delete";

    fn at_ms(millis: i64) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(u64::try_from(millis).expect("an epoch time"))
    }

    /// A per-tenant window table. A tenant it does not name keeps its blocks
    /// forever, which is what an unconfigured tenant does in production.
    struct Windows(BTreeMap<String, Time>);

    impl Windows {
        fn new(entries: &[(&str, Time)]) -> Self {
            Self(
                entries
                    .iter()
                    .map(|(tenant, window)| ((*tenant).to_string(), *window))
                    .collect(),
            )
        }
    }

    impl RetentionWindows for Windows {
        fn block_retention(&self, tenant: &str) -> Time {
            self.0.get(tenant).copied().unwrap_or(Time::ZERO)
        }
    }

    /// An object store that refuses to delete one key, delegates the rest, and
    /// records the order it was asked in.
    ///
    /// `InMemory` cannot fail a delete, and a pass that stopped at the first
    /// refusal would leave every block behind it in the bucket forever. The
    /// recorded order is how a test sees which of a block's two objects the
    /// pass deleted first. An empty `refused` refuses nothing.
    struct RefusesOneDelete {
        inner: Arc<InMemory>,
        refused: String,
        asked: Arc<Mutex<Vec<String>>>,
    }

    impl std::fmt::Debug for RefusesOneDelete {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(REFUSING_STORE)
        }
    }

    impl std::fmt::Display for RefusesOneDelete {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(REFUSING_STORE)
        }
    }

    #[async_trait]
    impl ObjectStore for RefusesOneDelete {
        async fn put_opts(
            &self,
            location: &Path,
            payload: PutPayload,
            opts: PutOptions,
        ) -> object_store::Result<PutResult> {
            self.inner.put_opts(location, payload, opts).await
        }

        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> object_store::Result<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }

        async fn get_opts(
            &self,
            location: &Path,
            options: GetOptions,
        ) -> object_store::Result<GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            let inner = self.inner.clone();
            let refused = self.refused.clone();
            let asked = self.asked.clone();
            locations
                .then(move |location| {
                    let inner = inner.clone();
                    let refused = refused.clone();
                    let asked = asked.clone();
                    async move {
                        let location = location?;
                        asked
                            .lock()
                            .expect("the delete order")
                            .push(location.as_ref().to_string());
                        if location.as_ref() == refused {
                            return Err(object_store::Error::Generic {
                                store: REFUSING_STORE,
                                source: REFUSAL.into(),
                            });
                        }
                        ObjectStoreExt::delete(inner.as_ref(), &location).await?;
                        Ok(location)
                    }
                })
                .boxed()
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&Path>,
        ) -> object_store::Result<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &Path,
            to: &Path,
            options: CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }
    }

    /// Writes one float block and its index manifest the way the compactor
    /// writes them, and answers with the manifest the sweep will read.
    async fn write_float_block(
        store: &Arc<dyn ObjectStore>,
        tenant: &str,
        first_offset: i64,
        timestamp_ms: i64,
    ) -> super::CompactionIndexManifest {
        let block_writer = krabka_blockstore::BlockWriter::new(store.clone());
        let sink = super::ObjectStoreCompactionIndexSink::new(store.clone());
        let rows = super::TenantCompactionRows {
            tenant: tenant.to_string(),
            series_labels: BTreeMap::from([(7, labels(&[("__name__", "up")]))]),
            float_rows: vec![FloatRow {
                fingerprint: 7,
                timestamp_ms,
                value: 1.0,
                start_timestamp_ms: None,
            }],
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
            clock_rows: Vec::new(),
        };
        let mut writes = super::write_compacted_tenant_blocks(
            &block_writer,
            &sink,
            &rows,
            first_offset,
            first_offset + 1,
        )
        .await
        .expect("write compacted blocks");
        assert!(writes.len() == 1);
        writes.remove(0).manifest
    }

    async fn exists(store: &Arc<dyn ObjectStore>, key: &str) -> bool {
        store.head(&Path::from(key)).await.is_ok()
    }

    fn one_object_deleted() -> CompactionRetentionPhase {
        CompactionRetentionPhase {
            deleted: 1,
            absent: 0,
            failures: Vec::new(),
        }
    }

    #[tokio::test]
    async fn retention_deletes_blocks_and_indexes_older_than_cutoff() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let old = write_float_block(&object_store, "tenant-a", 1, NOW_MS - 10_000).await;
        let fresh = write_float_block(&object_store, "tenant-a", 3, NOW_MS - 1_000).await;
        let windows = Windows::new(&[("tenant-a", secs(5))]);

        let stats = super::enforce_compaction_retention(&object_store, at_ms(NOW_MS), &windows)
            .await
            .expect("enforce retention");

        assert!(
            stats
                == super::CompactionRetentionStats {
                    manifests_scanned: 2,
                    // The manifest first, then the block it named.
                    manifests_retired: one_object_deleted(),
                    blocks_deleted: one_object_deleted(),
                    // The two objects of the surviving block, and nothing the
                    // index does not name.
                    orphans: OrphanSweepStats {
                        listed: 2,
                        live: 2,
                        ..OrphanSweepStats::default()
                    },
                }
        );
        check!(!exists(&object_store, &old.index_key).await);
        check!(!exists(&object_store, &old.block_key).await);
        check!(exists(&object_store, &fresh.index_key).await);
        check!(exists(&object_store, &fresh.block_key).await);
    }

    /// Three tenants, three windows, one clock. Every block is the same age,
    /// so only the window can decide which of them goes. The tenant the file
    /// never names reads the default window, and the sweep reaches it because
    /// it lists the whole prefix rather than the configured tenants.
    #[tokio::test]
    async fn each_tenant_keeps_its_blocks_for_its_own_window() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let overrides = crate::OverridesProvider::from_yaml(
            "defaults:
  compactor_blocks_retention_period: \"1h\"
overrides:
  tenant-short:
    compactor_blocks_retention_period: \"5s\"
  tenant-forever:
    compactor_blocks_retention_period: \"0s\"
",
        )
        .expect("parse runtime overrides");
        let expired = write_float_block(&object_store, "tenant-short", 1, NOW_MS - 10_000).await;
        let on_default =
            write_float_block(&object_store, "tenant-default", 3, NOW_MS - 10_000).await;
        let forever = write_float_block(&object_store, "tenant-forever", 5, NOW_MS - 10_000).await;

        let stats = super::enforce_compaction_retention(&object_store, at_ms(NOW_MS), &overrides)
            .await
            .expect("enforce retention");

        assert!(
            stats
                == super::CompactionRetentionStats {
                    manifests_scanned: 3,
                    manifests_retired: one_object_deleted(),
                    blocks_deleted: one_object_deleted(),
                    orphans: OrphanSweepStats {
                        listed: 4,
                        live: 4,
                        ..OrphanSweepStats::default()
                    },
                }
        );
        check!(!exists(&object_store, &expired.block_key).await);
        check!(!exists(&object_store, &expired.index_key).await);
        check!(
            exists(&object_store, &on_default.block_key).await,
            "an unlisted tenant keeps the default window, not no window"
        );
        check!(exists(&object_store, &on_default.index_key).await);
        check!(
            exists(&object_store, &forever.block_key).await,
            "a zero window keeps a block forever"
        );
        check!(exists(&object_store, &forever.index_key).await);
    }

    /// A tenant name with a character that needs an escape still ages out.
    /// The sweep compares each listed location with the key its manifest
    /// holds, and it stops on a mismatch. An escape that the object store
    /// rewrites on the way in would make every such block a mismatch, and no
    /// block of that tenant would ever be deleted.
    #[tokio::test]
    async fn retention_deletes_the_blocks_of_a_tenant_whose_name_is_escaped() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifest = write_float_block(&object_store, "team*(1)", 1, NOW_MS - 10_000).await;
        check!(
            manifest
                .index_key
                .starts_with("metrics/team!2A!281!29/float/")
        );
        let windows = Windows::new(&[("team*(1)", secs(5))]);

        let stats = super::enforce_compaction_retention(&object_store, at_ms(NOW_MS), &windows)
            .await
            .expect("enforce retention");

        check!(
            stats
                == super::CompactionRetentionStats {
                    manifests_scanned: 1,
                    manifests_retired: one_object_deleted(),
                    blocks_deleted: one_object_deleted(),
                    orphans: OrphanSweepStats::default(),
                }
        );
    }

    #[tokio::test]
    async fn zero_and_negative_retention_windows_sweep_nothing() {
        // The retention window is an extent, so "no window configured" is any
        // non-positive extent. A sweep that read one as "delete everything"
        // would empty the bucket of every tenant an operator never configured.
        for (name, window) in [
            ("a zero window", Time::ZERO),
            ("a negative window", Time::from_millis(-1)),
        ] {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let ancient = write_float_block(&object_store, "tenant-a", 1, 0).await;
            let windows = Windows::new(&[("tenant-a", window)]);

            let stats = super::enforce_compaction_retention(&object_store, at_ms(NOW_MS), &windows)
                .await
                .expect("enforce retention");

            check!(
                stats
                    == super::CompactionRetentionStats {
                        manifests_scanned: 1,
                        manifests_retired: CompactionRetentionPhase::default(),
                        blocks_deleted: CompactionRetentionPhase::default(),
                        orphans: OrphanSweepStats {
                            listed: 2,
                            live: 2,
                            ..OrphanSweepStats::default()
                        },
                    },
                "{name}"
            );
            check!(exists(&object_store, &ancient.block_key).await, "{name}");
            check!(exists(&object_store, &ancient.index_key).await, "{name}");
        }
    }

    /// The orphan half of the pass deletes every object the index does not
    /// name, and the `.index` manifests live under the same prefix as the
    /// blocks. A live set built from the block keys alone would name no
    /// manifest at all, so this sweep would delete the whole index: every
    /// block still in the bucket, and no query able to find one. Nothing else
    /// in metrics records which blocks exist.
    #[tokio::test]
    async fn the_orphan_sweep_keeps_every_index_manifest() {
        const GRACE_AND_MORE: Duration = Duration::from_hours(2);
        // A zero window everywhere, so the orphan half is the only half that
        // can delete anything here.
        let windows = Windows::new(&[]);

        for (name, now, deleted, within_grace) in [
            (
                "past the grace window",
                SystemTime::now() + GRACE_AND_MORE,
                1,
                0,
            ),
            ("inside the grace window", SystemTime::now(), 0, 1),
        ] {
            let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
            let first = write_float_block(&object_store, "tenant-a", 1, NOW_MS).await;
            let second = write_float_block(&object_store, "tenant-b", 3, NOW_MS).await;
            // A block that no manifest names: a write that put its object and
            // never published an index entry leaves exactly this.
            let orphan = "metrics/tenant-a/float/00000000000000000009-00000000000000000010.parquet";
            object_store
                .put(
                    &Path::from(orphan),
                    PutPayload::from(b"an unreferenced block".to_vec()),
                )
                .await
                .expect("put the orphan");

            let stats = super::enforce_compaction_retention(&object_store, now, &windows)
                .await
                .expect("enforce retention");

            check!(
                stats
                    == super::CompactionRetentionStats {
                        manifests_scanned: 2,
                        manifests_retired: CompactionRetentionPhase::default(),
                        blocks_deleted: CompactionRetentionPhase::default(),
                        orphans: OrphanSweepStats {
                            listed: 5,
                            live: 4,
                            kept_within_grace: within_grace,
                            deleted,
                            absent: 0,
                            failed: 0,
                        },
                    },
                "{name}"
            );
            for manifest in [&first, &second] {
                check!(
                    exists(&object_store, &manifest.index_key).await,
                    "{name}: {} is the index itself",
                    manifest.index_key
                );
                check!(exists(&object_store, &manifest.block_key).await, "{name}");
            }
            check!(
                exists(&object_store, orphan).await == (deleted == 0),
                "{name}"
            );
        }
    }

    /// One object a backend refuses does not end the pass. A sweep covers
    /// every tenant, so a stop at the first refusal would keep every block
    /// behind it in the bucket for as long as that object refuses.
    #[tokio::test]
    async fn one_object_that_will_not_delete_does_not_stop_the_others() {
        let inner = Arc::new(InMemory::new());
        let seeded: Arc<dyn ObjectStore> = inner.clone();
        let stubborn = write_float_block(&seeded, "tenant-a", 1, NOW_MS - 10_000).await;
        let yielding = write_float_block(&seeded, "tenant-b", 3, NOW_MS - 10_000).await;
        let object_store: Arc<dyn ObjectStore> = Arc::new(RefusesOneDelete {
            inner: inner.clone(),
            refused: stubborn.block_key.clone(),
            asked: Arc::new(Mutex::new(Vec::new())),
        });
        let windows = Windows::new(&[("tenant-a", secs(5)), ("tenant-b", secs(5))]);

        let stats = super::enforce_compaction_retention(&object_store, at_ms(NOW_MS), &windows)
            .await
            .expect("a refused delete is not a failed pass");

        check!(
            stats
                == super::CompactionRetentionStats {
                    manifests_scanned: 2,
                    // Both manifests go: the index entry of a block leaves
                    // before the block does.
                    manifests_retired: CompactionRetentionPhase {
                        deleted: 2,
                        absent: 0,
                        failures: Vec::new(),
                    },
                    // The block whose object refused is then an object no
                    // index names, and a later pass sweeps it as an orphan --
                    // which is the way round that leaves no manifest naming a
                    // block that is gone.
                    blocks_deleted: CompactionRetentionPhase {
                        deleted: 1,
                        absent: 0,
                        failures: vec![BlockDeletionFailure {
                            object_key: stubborn.block_key.clone(),
                            failed_key: stubborn.block_key.clone(),
                            error: object_store::Error::Generic {
                                store: REFUSING_STORE,
                                source: REFUSAL.into(),
                            }
                            .to_string(),
                        }],
                    },
                    orphans: OrphanSweepStats {
                        listed: 1,
                        live: 1,
                        ..OrphanSweepStats::default()
                    },
                }
        );
        check!(
            !exists(&seeded, &yielding.block_key).await,
            "the block behind the refusal still went"
        );
        check!(exists(&seeded, &stubborn.block_key).await);
    }

    /// The `.index` manifest *is* the metrics index entry, so a pass that
    /// deleted the block first would leave a live manifest naming an object
    /// that is gone, and every query over that window would resolve a key with
    /// nothing behind it. The other order leaves an unreferenced object, which
    /// the orphan sweep reclaims and which no query ever reaches.
    #[tokio::test]
    async fn retention_retires_the_manifest_before_it_deletes_the_block() {
        let inner = Arc::new(InMemory::new());
        let seeded: Arc<dyn ObjectStore> = inner.clone();
        let expired = write_float_block(&seeded, "tenant-a", 1, NOW_MS - 10_000).await;
        let asked = Arc::new(Mutex::new(Vec::new()));
        let object_store: Arc<dyn ObjectStore> = Arc::new(RefusesOneDelete {
            inner: inner.clone(),
            refused: String::new(),
            asked: asked.clone(),
        });
        let windows = Windows::new(&[("tenant-a", secs(5))]);

        super::enforce_compaction_retention(&object_store, at_ms(NOW_MS), &windows)
            .await
            .expect("enforce retention");

        let asked = asked.lock().expect("the delete order").clone();
        assert!(
            asked == vec![expired.index_key.clone(), expired.block_key.clone()],
            "the index entry leaves before the object it names"
        );
    }

    /// A manifest the store refuses keeps its block. The index entry is still
    /// live, so deleting the object would produce exactly the state the
    /// ordering exists to prevent.
    #[tokio::test]
    async fn an_expired_block_whose_manifest_will_not_delete_is_left_alone() {
        let inner = Arc::new(InMemory::new());
        let seeded: Arc<dyn ObjectStore> = inner.clone();
        let stubborn = write_float_block(&seeded, "tenant-a", 1, NOW_MS - 10_000).await;
        let yielding = write_float_block(&seeded, "tenant-b", 3, NOW_MS - 10_000).await;
        let object_store: Arc<dyn ObjectStore> = Arc::new(RefusesOneDelete {
            inner: inner.clone(),
            refused: stubborn.index_key.clone(),
            asked: Arc::new(Mutex::new(Vec::new())),
        });
        let windows = Windows::new(&[("tenant-a", secs(5)), ("tenant-b", secs(5))]);

        let stats = super::enforce_compaction_retention(&object_store, at_ms(NOW_MS), &windows)
            .await
            .expect("a refused delete is not a failed pass");

        check!(
            stats
                == super::CompactionRetentionStats {
                    manifests_scanned: 2,
                    manifests_retired: CompactionRetentionPhase {
                        deleted: 1,
                        absent: 0,
                        failures: vec![BlockDeletionFailure {
                            object_key: stubborn.index_key.clone(),
                            failed_key: stubborn.index_key.clone(),
                            error: object_store::Error::Generic {
                                store: REFUSING_STORE,
                                source: REFUSAL.into(),
                            }
                            .to_string(),
                        }],
                    },
                    // One block, not two: the tenant whose manifest refused is
                    // not offered to the second phase at all.
                    blocks_deleted: one_object_deleted(),
                    orphans: OrphanSweepStats {
                        listed: 2,
                        live: 2,
                        ..OrphanSweepStats::default()
                    },
                }
        );
        check!(
            exists(&seeded, &stubborn.block_key).await,
            "a live manifest must never name a block that is gone"
        );
        check!(exists(&seeded, &stubborn.index_key).await);
        check!(!exists(&seeded, &yielding.block_key).await);
        check!(!exists(&seeded, &yielding.index_key).await);
    }

    /// Retention and orphan reconciliation are two halves of one pass, and a
    /// deployment where no tenant has a window still has orphans: a writer that
    /// put its block and died before it published the manifest leaves one, and
    /// nothing else in metrics ever looks for it. The pass must therefore do the
    /// orphan half and expire nothing.
    #[tokio::test]
    async fn a_deployment_with_no_retention_window_still_reclaims_orphans() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // The built-in limits, which is what a deployment with no overrides file
        // reads. Its window is zero, so nothing can expire.
        let no_windows = crate::OverridesProvider::new(crate::Limits::default());
        check!(!no_windows.expires_any_blocks());
        let live = write_float_block(&object_store, "tenant-a", 1, NOW_MS).await;
        let orphan = "metrics/tenant-a/float/00000000000000000009-00000000000000000010.parquet";
        object_store
            .put(
                &Path::from(orphan),
                PutPayload::from(b"an unreferenced block".to_vec()),
            )
            .await
            .expect("put the orphan");

        let stats = super::enforce_compaction_retention(
            &object_store,
            SystemTime::now() + Duration::from_hours(2),
            &no_windows,
        )
        .await
        .expect("enforce retention");

        assert!(
            stats
                == super::CompactionRetentionStats {
                    manifests_scanned: 1,
                    manifests_retired: CompactionRetentionPhase::default(),
                    blocks_deleted: CompactionRetentionPhase::default(),
                    orphans: OrphanSweepStats {
                        listed: 3,
                        live: 2,
                        deleted: 1,
                        ..OrphanSweepStats::default()
                    },
                }
        );
        check!(!exists(&object_store, orphan).await);
        check!(exists(&object_store, &live.block_key).await);
        check!(exists(&object_store, &live.index_key).await);
    }

    #[tokio::test]
    async fn retention_rejects_manifest_with_mismatched_index_key() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let listed_index_key = "metrics/tenant-a/float/mismatch.index";
        let manifest_index_key = "metrics/tenant-a/float/actual.index";
        let manifest = super::CompactionIndexManifest {
            tenant: "tenant-a".to_string(),
            kind: super::MetricBlockKind::Float,
            block_key: "metrics/tenant-a/float/block.parquet".to_string(),
            index_key: manifest_index_key.to_string(),
            level: krabka_blockstore::BlockLevel::INGESTED,
            first_offset: 0,
            last_offset: 1,
            row_count: 1,
            min_ts: 1_000,
            max_ts: 1_000,
            fingerprints: vec![1],
            series: Vec::new(),
        };
        object_store
            .put(
                &Path::from(listed_index_key),
                PutPayload::from(manifest.encode().expect("encode manifest")),
            )
            .await
            .expect("write mismatched manifest");
        let windows = Windows::new(&[("tenant-a", secs(5))]);

        let error = super::enforce_compaction_retention(&object_store, at_ms(NOW_MS), &windows)
            .await
            .expect_err("mismatched manifest should fail");

        assert!(matches!(
            error,
            super::CompactionRetentionError::Manifest(
                super::CompactionManifestError::KeyMismatch { listed, manifest }
            ) if listed == listed_index_key && manifest == manifest_index_key
        ));
        // Nothing is deleted, and the orphan half never runs: the pass cannot
        // tell which blocks the index names, and every object under the
        // prefix would look unreferenced.
        check!(exists(&object_store, listed_index_key).await);
    }

    #[test]
    fn metrics_compactor_config_validates_required_consumer_fields() {
        let cfg = super::MetricsCompactorConfig {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            bootstrap: String::new(),
            group_id: "metrics-compactor".to_string(),
            client_id: "krabka-metrics-compactor".to_string(),
            wal_topic: crate::WAL_TOPIC.to_string(),
            poll_timeout: millis(500),
            auto_offset_reset: krabka_client_consumer::AutoOffsetReset::Earliest,
            flush_max_rows: super::DEFAULT_FLUSH_MAX_ROWS,
            flush_max_age: super::DEFAULT_FLUSH_MAX_AGE,
            object_store_retry: ObjectStoreRetryPolicy::DEFAULT,
        };

        let err = cfg.validate().expect_err("empty bootstrap should fail");
        assert!(format!("{err}").contains("bootstrap"));
    }

    #[tokio::test]
    async fn metrics_compactor_config_builds_runtime_with_shared_object_store() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cfg = super::MetricsCompactorConfig {
            client_dispatch_queue_capacity:
                krabka_client_core::ConnectionDispatchQueueCapacity::default(),
            client_frame_max: krabka_client_core::ClientFrameMax::default(),
            bootstrap: "127.0.0.1:9092".to_string(),
            group_id: "metrics-compactor".to_string(),
            client_id: "krabka-metrics-compactor".to_string(),
            wal_topic: crate::WAL_TOPIC.to_string(),
            poll_timeout: millis(250),
            auto_offset_reset: krabka_client_consumer::AutoOffsetReset::Earliest,
            flush_max_rows: 12_345,
            flush_max_age: secs(7),
            object_store_retry: ObjectStoreRetryPolicy::DEFAULT,
        };

        let runtime = cfg
            .build_runtime(object_store.clone(), ObjectStoreMetrics::unregistered())
            .expect("build runtime");
        assert!(
            runtime.loop_config
                == super::CompactionLoopConfig {
                    wal_topic: crate::WAL_TOPIC.to_string(),
                    poll_timeout: millis(250),
                    flush_max_rows: 12_345,
                    flush_max_age: secs(7),
                }
        );

        let manifest = super::CompactionIndexManifest::from_plan(
            "tenant-a",
            super::MetricBlockKind::Float,
            &super::compaction_partition_object_plan(
                "tenant-a",
                super::MetricBlockKind::Float,
                super::PartitionIndex(0),
                10,
                10,
            ),
        );
        super::CompactionIndexSink::write_manifest(&runtime.index_sink, &manifest)
            .await
            .expect("write manifest through runtime sink");
        let bytes = object_store
            .get(&object_store::path::Path::from(manifest.index_key.clone()))
            .await
            .expect("get manifest")
            .bytes()
            .await
            .expect("manifest bytes");
        assert!(super::CompactionIndexManifest::decode(&bytes).expect("decode") == manifest);
    }

    #[derive(Default)]
    struct RecordingIndexSink {
        manifests: Mutex<Vec<super::CompactionIndexManifest>>,
    }

    #[async_trait]
    impl super::CompactionIndexSink for RecordingIndexSink {
        async fn write_manifest(
            &self,
            manifest: &super::CompactionIndexManifest,
        ) -> Result<(), super::CompactionIndexError> {
            self.manifests
                .lock()
                .expect("manifest lock")
                .push(manifest.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn write_compacted_tenant_blocks_writes_block_before_index_manifest() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store.clone());
        let sink = RecordingIndexSink::default();
        let rows = super::TenantCompactionRows {
            tenant: "tenant-a".to_string(),
            series_labels: BTreeMap::from([(7, labels(&[("__name__", "up")]))]),
            float_rows: vec![
                FloatRow {
                    fingerprint: 7,
                    timestamp_ms: 100,
                    value: 1.0,
                    start_timestamp_ms: Some(50),
                },
                FloatRow {
                    fingerprint: 7,
                    timestamp_ms: 200,
                    value: 2.0,
                    start_timestamp_ms: None,
                },
            ],
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: Vec::new(),
            clock_rows: Vec::new(),
        };

        let writes = super::write_compacted_tenant_blocks(&block_writer, &sink, &rows, 42, 99)
            .await
            .expect("write compacted blocks");

        check!(writes.len() == 1);
        check!(writes[0].kind == super::MetricBlockKind::Float);
        check!(writes[0].block_meta.row_count == 2);
        let persisted = krabka_blockstore::read_block(object_store, &writes[0].manifest.block_key)
            .await
            .expect("read persisted block");
        assert!(persisted.len() == 1);
        assert!(persisted[0].num_rows() == 2);
        assert!(
            crate::decode_float_samples(&persisted[0]).expect("decode persisted floats")
                == vec![(7, 100, 1.0, Some(50)), (7, 200, 2.0, None)]
        );

        let manifests = sink.manifests.lock().expect("manifest lock");
        check!(manifests.as_slice() == [writes[0].manifest.clone()]);
        check!(manifests[0].tenant == "tenant-a");
        check!(manifests[0].kind == super::MetricBlockKind::Float);
        check!(manifests[0].first_offset == 42);
        check!(manifests[0].last_offset == 99);
        check!(manifests[0].row_count == 2);
    }

    #[tokio::test]
    async fn write_compacted_tenant_blocks_persists_metadata_only_rows() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store.clone());
        let sink = RecordingIndexSink::default();
        let rows = super::TenantCompactionRows {
            tenant: "tenant-a".to_string(),
            series_labels: BTreeMap::from([(7, labels(&[("__name__", "http_requests_total")]))]),
            float_rows: Vec::new(),
            histogram_rows: Vec::new(),
            exemplar_rows: Vec::new(),
            metadata_rows: vec![super::MetadataRow {
                fingerprint: 7,
                metric_family_name: "http_requests_total".to_string(),
                metric_type: "counter".to_string(),
                help: "Total HTTP requests.".to_string(),
                unit: "requests".to_string(),
            }],
            clock_rows: Vec::new(),
        };

        let writes = super::write_compacted_tenant_blocks(&block_writer, &sink, &rows, 42, 99)
            .await
            .expect("write compacted blocks");

        check!(writes.len() == 1);
        check!(writes[0].kind == super::MetricBlockKind::Metadata);
        check!(writes[0].block_meta.row_count == 1);
        let persisted = krabka_blockstore::read_block(object_store, &writes[0].manifest.block_key)
            .await
            .expect("read persisted metadata block");
        assert!(persisted.len() == 1);
        assert!(persisted[0].num_rows() == 1);

        let manifests = sink.manifests.lock().expect("manifest lock");
        check!(manifests.as_slice() == [writes[0].manifest.clone()]);
        check!(manifests[0].tenant == "tenant-a");
        check!(manifests[0].kind == super::MetricBlockKind::Metadata);
        check!(manifests[0].first_offset == 42);
        check!(manifests[0].last_offset == 99);
        check!(manifests[0].row_count == 1);
    }

    #[derive(Default)]
    struct RecordingOffsetCommitter {
        commits: Mutex<Vec<super::CompactionPartitionOffset>>,
    }

    #[async_trait]
    impl super::CompactionOffsetCommitter for RecordingOffsetCommitter {
        async fn commit_offsets(
            &self,
            offsets: &[super::CompactionPartitionOffset],
        ) -> Result<(), super::CompactionCommitError> {
            self.commits
                .lock()
                .expect("commit lock")
                .extend_from_slice(offsets);
            Ok(())
        }
    }

    #[tokio::test]
    async fn process_compaction_partition_window_commits_after_blocks_and_indexes() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let committer = RecordingOffsetCommitter::default();
        let first = float_record("tenant-a", "up", "api", 100);
        let second = float_record("tenant-a", "up", "api", 200);
        let records = vec![
            super::CompactionWalRecord {
                partition: super::PartitionIndex(3),
                offset: super::Offset(42),
                value: first.encode().expect("encode first"),
            },
            super::CompactionWalRecord {
                partition: super::PartitionIndex(3),
                offset: super::Offset(43),
                value: second.encode().expect("encode second"),
            },
        ];

        let result =
            super::process_compaction_partition_window(&block_writer, &sink, &committer, &records)
                .await
                .expect("process compaction window");

        check!(
            result.committed_offset
                == Some(super::CompactionPartitionOffset {
                    partition: super::PartitionIndex(3),
                    offset: super::Offset(44),
                })
        );
        check!(result.writes.len() == 1);
        check!(sink.manifests.lock().expect("manifest lock").len() == 1);
        check!(
            committer.commits.lock().expect("commit lock").as_slice()
                == [super::CompactionPartitionOffset {
                    partition: super::PartitionIndex(3),
                    offset: super::Offset(44),
                }]
        );
    }

    /// Index sink that succeeds for the first `ok_before_failure` manifest
    /// writes and then fails. It models one partition's block write that
    /// succeeds before a later partition's write fails mid-batch.
    struct FailAfterIndexSink {
        ok_before_failure: usize,
        calls: Mutex<usize>,
    }

    impl FailAfterIndexSink {
        fn new(ok_before_failure: usize) -> Self {
            Self {
                ok_before_failure,
                calls: Mutex::new(0),
            }
        }
    }

    #[async_trait]
    impl super::CompactionIndexSink for FailAfterIndexSink {
        async fn write_manifest(
            &self,
            _manifest: &super::CompactionIndexManifest,
        ) -> Result<(), super::CompactionIndexError> {
            let mut calls = self.calls.lock().expect("calls lock");
            if *calls >= self.ok_before_failure {
                return Err(super::CompactionIndexError::ObjectStore(
                    "injected index write failure".to_string(),
                ));
            }
            *calls += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn process_compaction_record_batch_does_not_commit_when_a_later_partition_write_fails() {
        // Two partitions processed in order (0, then 1). Partition 0's block +
        // index write succeeds; partition 1's index write fails. Because the
        // commit advances the WHOLE assignment's offsets, committing per-partition
        // would have advanced partition 1's offset past records whose block was
        // never written — silent data loss. The fix writes all partitions first
        // and commits once, so a mid-batch failure must leave NOTHING committed
        // and the next poll re-reads from the last committed offset (at-least-once).
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store);
        // Float-only records => exactly one block (one index manifest) per
        // partition, so `ok_before_failure = 1` lets partition 0 through and fails
        // partition 1.
        let sink = FailAfterIndexSink::new(1);
        let committer = RecordingOffsetCommitter::default();
        let records = vec![
            super::CompactionWalRecord {
                partition: super::PartitionIndex(0),
                offset: super::Offset(42),
                value: float_record("tenant-a", "up", "api", 100)
                    .encode()
                    .expect("encode p0"),
            },
            super::CompactionWalRecord {
                partition: super::PartitionIndex(1),
                offset: super::Offset(42),
                value: float_record("tenant-a", "up", "api", 200)
                    .encode()
                    .expect("encode p1"),
            },
        ];

        let result =
            super::process_compaction_record_batch(&block_writer, &sink, &committer, &records)
                .await;

        assert!(result.is_err());
        assert!(committer.commits.lock().expect("commit lock").is_empty());
    }

    #[tokio::test]
    async fn process_compaction_record_batch_groups_partitions_and_uses_distinct_block_keys() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let committer = RecordingOffsetCommitter::default();
        let records = vec![
            super::CompactionWalRecord {
                partition: super::PartitionIndex(0),
                offset: super::Offset(42),
                value: float_record("tenant-a", "up", "api", 100)
                    .encode()
                    .expect("encode p0"),
            },
            super::CompactionWalRecord {
                partition: super::PartitionIndex(1),
                offset: super::Offset(42),
                value: float_record("tenant-a", "up", "api", 200)
                    .encode()
                    .expect("encode p1"),
            },
        ];

        let result =
            super::process_compaction_record_batch(&block_writer, &sink, &committer, &records)
                .await
                .expect("process compaction batch");

        check!(result.partition_results.len() == 2);
        check!(result.writes.len() == 2);
        check!(result.writes[0].manifest.block_key != result.writes[1].manifest.block_key);
        check!(
            result.committed_offsets
                == vec![
                    super::CompactionPartitionOffset {
                        partition: super::PartitionIndex(0),
                        offset: super::Offset(43),
                    },
                    super::CompactionPartitionOffset {
                        partition: super::PartitionIndex(1),
                        offset: super::Offset(43),
                    },
                ]
        );
        check!(
            committer.commits.lock().expect("commit lock").as_slice()
                == result.committed_offsets.as_slice()
        );
    }

    #[test]
    fn compaction_wal_records_from_consumer_records_filters_topic_and_requires_values() {
        let wal_record = float_record("tenant-a", "up", "api", 100);
        let records = vec![
            krabka_client_consumer::ConsumerRecord {
                topic: crate::WAL_TOPIC.to_string(),
                partition: 2,
                offset: 10,
                leader_epoch: -1,
                timestamp: 100,
                key: None,
                value: Some(bytes::Bytes::from(wal_record.encode().expect("encode wal"))),
                headers: Vec::new(),
            },
            krabka_client_consumer::ConsumerRecord {
                topic: "unrelated".to_string(),
                partition: 2,
                offset: 11,
                leader_epoch: -1,
                timestamp: 101,
                key: None,
                value: Some(bytes::Bytes::from_static(b"ignored")),
                headers: Vec::new(),
            },
        ];

        let converted =
            super::compaction_wal_records_from_consumer_records(crate::WAL_TOPIC, &records)
                .expect("convert consumer records");

        assert!(
            converted
                == vec![super::CompactionWalRecord {
                    partition: super::PartitionIndex(2),
                    offset: super::Offset(10),
                    value: wal_record.encode().expect("encode expected"),
                }]
        );

        let missing_value = vec![krabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 3,
            offset: 12,
            leader_epoch: -1,
            timestamp: 102,
            key: None,
            value: None,
            headers: Vec::new(),
        }];
        let err =
            super::compaction_wal_records_from_consumer_records(crate::WAL_TOPIC, &missing_value)
                .expect_err("missing value should fail");
        assert!(matches!(
            err,
            super::CompactionConsumerRecordError::MissingValue {
                partition: super::PartitionIndex(3),
                offset: super::Offset(12)
            }
        ));
    }

    #[derive(Default)]
    struct RecordingCommitSync {
        calls: Mutex<Vec<(String, Vec<super::CompactionPartitionOffset>)>>,
    }

    #[async_trait]
    impl super::CompactionConsumerCommit for RecordingCommitSync {
        async fn commit_offsets_sync(
            &self,
            topic: &str,
            offsets: &[super::CompactionPartitionOffset],
        ) -> Result<(), super::CompactionConsumerCommitError> {
            self.calls
                .lock()
                .expect("commit calls lock")
                .push((topic.to_string(), offsets.to_vec()));
            Ok(())
        }
    }

    #[tokio::test]
    async fn compaction_consumer_committer_forwards_selected_offsets() {
        let sync = RecordingCommitSync::default();
        let committer = super::CompactionConsumerCommitter::new(&sync, crate::WAL_TOPIC);

        super::CompactionOffsetCommitter::commit_offsets(
            &committer,
            &[super::CompactionPartitionOffset {
                partition: super::PartitionIndex(2),
                offset: super::Offset(11),
            }],
        )
        .await
        .expect("commit offsets");

        assert!(
            *sync.calls.lock().expect("commit calls lock")
                == vec![(
                    crate::WAL_TOPIC.to_string(),
                    vec![super::CompactionPartitionOffset {
                        partition: super::PartitionIndex(2),
                        offset: super::Offset(11),
                    }],
                )]
        );
    }

    struct StaticPoller {
        records: Vec<krabka_client_consumer::ConsumerRecord>,
    }

    #[async_trait]
    impl super::CompactionConsumerPoll for StaticPoller {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<Vec<krabka_client_consumer::ConsumerRecord>, super::CompactionConsumerPollError>
        {
            Ok(std::mem::take(&mut self.records))
        }
    }

    #[tokio::test]
    async fn poll_compactor_once_converts_processes_and_commits_records() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let commit = RecordingCommitSync::default();
        let committer = super::CompactionConsumerCommitter::new(&commit, crate::WAL_TOPIC);
        let wal_record = float_record("tenant-a", "up", "api", 100);
        let mut poller = StaticPoller {
            records: vec![krabka_client_consumer::ConsumerRecord {
                topic: crate::WAL_TOPIC.to_string(),
                partition: 4,
                offset: 21,
                leader_epoch: -1,
                timestamp: 100,
                key: None,
                value: Some(bytes::Bytes::from(wal_record.encode().expect("encode wal"))),
                headers: Vec::new(),
            }],
        };

        let result = super::poll_compactor_once(
            &mut poller,
            &block_writer,
            &sink,
            &committer,
            crate::WAL_TOPIC,
            millis(1),
            &ServiceMetrics::new(),
        )
        .await
        .expect("poll compactor once");

        check!(result.polled_records == 1);
        check!(result.compacted_records == 1);
        check!(result.batch.writes.len() == 1);
        check!(
            result.batch.committed_offsets
                == vec![super::CompactionPartitionOffset {
                    partition: super::PartitionIndex(4),
                    offset: super::Offset(22),
                }]
        );
        check!(commit.calls.lock().expect("commit calls lock").len() == 1);
        check!(sink.manifests.lock().expect("manifest lock").len() == 1);
    }

    struct QueuePoller {
        batches: Vec<Vec<krabka_client_consumer::ConsumerRecord>>,
    }

    #[async_trait]
    impl super::CompactionConsumerPoll for QueuePoller {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<Vec<krabka_client_consumer::ConsumerRecord>, super::CompactionConsumerPollError>
        {
            if self.batches.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(self.batches.remove(0))
            }
        }
    }

    #[tokio::test]
    async fn run_compactor_loop_accumulates_across_polls_and_flushes_once_on_stop() {
        // Two below-threshold polls must accumulate into ONE block (not one per
        // poll) and commit offsets only at the single shutdown flush.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let commit = RecordingCommitSync::default();
        let committer = super::CompactionConsumerCommitter::new(&commit, crate::WAL_TOPIC);
        let make_record = |offset, timestamp| krabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let mut poller = QueuePoller {
            batches: vec![vec![make_record(10, 100)], vec![make_record(11, 200)]],
        };
        let mut stop_after_empty =
            |result: &super::CompactionPollResult| result.polled_records == 0;

        let result = super::run_compactor_loop(
            &mut poller,
            &block_writer,
            &sink,
            &committer,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                // High row threshold and long age so neither below-threshold
                // poll triggers a mid-loop flush; only the shutdown flush writes.
                flush_max_rows: 50_000,
                flush_max_age: hours(1),
            },
            &mut stop_after_empty,
            &ServiceMetrics::new(),
        )
        .await
        .expect("run compactor loop");

        // ONE block written for the whole buffer, not one per poll; a single
        // commit at flush through the last buffered record (offset 11 -> commit 12).
        assert!(
            result
                == super::CompactionLoopResult {
                    polls: 3,
                    polled_records: 2,
                    compacted_records: 2,
                    writes: 1,
                    committed_offsets: vec![super::CompactionPartitionOffset {
                        partition: super::PartitionIndex(0),
                        offset: super::Offset(12),
                    }],
                }
        );
        check!(commit.calls.lock().expect("commit calls lock").len() == 1);
        assert!(sink.manifests.lock().expect("manifest lock").len() == 1);
        // The single block spans the full buffered offset range [10, 11].
        let manifests = sink.manifests.lock().expect("manifest lock");
        check!(manifests[0].first_offset == 10);
        check!(manifests[0].last_offset == 11);
        check!(manifests[0].row_count == 2);
    }

    #[tokio::test]
    async fn run_compactor_loop_flushes_when_row_threshold_reached() {
        // Crossing flush_max_rows must flush mid-loop without waiting for stop.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let commit = RecordingCommitSync::default();
        let committer = super::CompactionConsumerCommitter::new(&commit, crate::WAL_TOPIC);
        let make_record = |offset, timestamp| krabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        // Two records per poll; flush_max_rows == 2 flushes on the first poll.
        let mut poller = QueuePoller {
            batches: vec![vec![make_record(10, 100), make_record(11, 200)]],
        };
        // Stop once the buffer has flushed (a committed offset surfaced) or polls drain.
        let mut stop_after_empty =
            |result: &super::CompactionPollResult| result.polled_records == 0;

        let result = super::run_compactor_loop(
            &mut poller,
            &block_writer,
            &sink,
            &committer,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 2,
                flush_max_age: hours(1),
            },
            &mut stop_after_empty,
            &ServiceMetrics::new(),
        )
        .await
        .expect("run compactor loop");

        // One block flushed by the row threshold on the first poll; the empty
        // second poll triggers stop with an already-empty buffer (no extra write).
        check!(result.writes == 1);
        check!(
            result.committed_offsets
                == vec![super::CompactionPartitionOffset {
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(12),
                }]
        );
        check!(commit.calls.lock().expect("commit calls lock").len() == 1);
        check!(sink.manifests.lock().expect("manifest lock").len() == 1);
    }

    struct FixedClock {
        now: std::sync::Mutex<std::time::Instant>,
    }

    impl FixedClock {
        fn new(start: std::time::Instant) -> Self {
            Self {
                now: std::sync::Mutex::new(start),
            }
        }

        fn advance(&self, delta: std::time::Duration) {
            let mut guard = self.now.lock().expect("clock lock");
            *guard += delta;
        }
    }

    impl super::CompactionClock for FixedClock {
        fn now(&self) -> std::time::Instant {
            *self.now.lock().expect("clock lock")
        }
    }

    #[tokio::test]
    async fn run_compactor_loop_age_flush_uses_injected_clock() {
        // With a finite age, the buffer flushes only after the clock advances past it.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let commit = RecordingCommitSync::default();
        let committer = super::CompactionConsumerCommitter::new(&commit, crate::WAL_TOPIC);
        let make_record = |offset, timestamp| krabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let clock = std::sync::Arc::new(FixedClock::new(std::time::Instant::now()));
        let advance_clock = std::sync::Arc::clone(&clock);
        // Poll 1 buffers offset 10; poll 2 buffers offset 11; poll 3 is empty.
        let mut poller = QueuePoller {
            batches: vec![vec![make_record(10, 100)], vec![make_record(11, 200)]],
        };
        // Advance the clock past flush_max_age once both records are buffered (after 2 polls).
        let mut polls = 0_usize;
        let mut stop_after_three = move |result: &super::CompactionPollResult| {
            polls += 1;
            if polls == 2 {
                advance_clock.advance(std::time::Duration::from_mins(2));
            }
            result.polled_records == 0
        };

        let result = super::run_compactor_loop_with_clock(
            &mut poller,
            &block_writer,
            &sink,
            &committer,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 50_000,
                flush_max_age: minutes(1),
            },
            &mut stop_after_three,
            CompactionLoopContext::new(clock.as_ref(), &ServiceMetrics::new()),
        )
        .await
        .expect("run compactor loop with clock");

        // Both records land in one age-triggered block; commit through offset 11 -> 12.
        check!(result.writes == 1);
        check!(
            result.committed_offsets
                == vec![super::CompactionPartitionOffset {
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(12),
                }]
        );
        check!(commit.calls.lock().expect("commit calls lock").len() == 1);
        let manifests = sink.manifests.lock().expect("manifest lock");
        assert!(manifests.len() == 1);
        check!(manifests[0].first_offset == 10);
        check!(manifests[0].last_offset == 11);
    }

    #[tokio::test]
    async fn run_compactor_consumer_loop_uses_one_consumer_for_poll_and_commit() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let make_record = |offset, timestamp| krabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let mut consumer = PollAndCommit {
            batches: vec![vec![make_record(10, 100)], Vec::new()],
            commit_calls: 0,
            committed_offsets: Vec::new(),
        };

        let result = super::run_compactor_consumer_loop(
            &mut consumer,
            &block_writer,
            &sink,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 50_000,
                flush_max_age: hours(1),
            },
            |result| result.polled_records == 0,
            &ServiceMetrics::new(),
        )
        .await
        .expect("run compactor consumer loop");

        check!(result.polls == 2);
        check!(result.polled_records == 1);
        // Buffered for one poll, then flushed once on the empty-poll shutdown.
        check!(result.writes == 1);
        check!(consumer.commit_calls == 1);
        check!(consumer.committed_offsets[0][0].offset == krabka_ids::Offset(11));
    }

    /// An object store whose first `failures` puts fail with `error`.
    /// Everything else delegates to an in-memory store, and every attempt is
    /// counted so a test can tell one try from four.
    #[derive(Debug)]
    struct FlakyPutStore {
        inner: InMemory,
        remaining_failures: std::sync::atomic::AtomicUsize,
        attempts: std::sync::atomic::AtomicUsize,
        error: fn() -> object_store::Error,
    }

    impl FlakyPutStore {
        fn new(failures: usize, error: fn() -> object_store::Error) -> Self {
            Self {
                inner: InMemory::new(),
                remaining_failures: std::sync::atomic::AtomicUsize::new(failures),
                attempts: std::sync::atomic::AtomicUsize::new(0),
                error,
            }
        }

        fn attempts(&self) -> usize {
            self.attempts.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl std::fmt::Display for FlakyPutStore {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("FlakyPutStore")
        }
    }

    #[async_trait]
    impl ObjectStore for FlakyPutStore {
        async fn put_opts(
            &self,
            location: &object_store::path::Path,
            payload: object_store::PutPayload,
            options: object_store::PutOptions,
        ) -> object_store::Result<object_store::PutResult> {
            use std::sync::atomic::Ordering;
            self.attempts.fetch_add(1, Ordering::SeqCst);
            if self
                .remaining_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                    (left > 0).then(|| left - 1)
                })
                .is_ok()
            {
                return Err((self.error)());
            }
            self.inner.put_opts(location, payload, options).await
        }

        async fn put_multipart_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::PutMultipartOptions,
        ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
            self.inner.put_multipart_opts(location, options).await
        }

        async fn get_opts(
            &self,
            location: &object_store::path::Path,
            options: object_store::GetOptions,
        ) -> object_store::Result<object_store::GetResult> {
            self.inner.get_opts(location, options).await
        }

        fn list(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>>
        {
            self.inner.list(prefix)
        }

        async fn list_with_delimiter(
            &self,
            prefix: Option<&object_store::path::Path>,
        ) -> object_store::Result<object_store::ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }

        async fn copy_opts(
            &self,
            from: &object_store::path::Path,
            to: &object_store::path::Path,
            options: object_store::CopyOptions,
        ) -> object_store::Result<()> {
            self.inner.copy_opts(from, to, options).await
        }

        fn delete_stream(
            &self,
            locations: futures::stream::BoxStream<
                'static,
                object_store::Result<object_store::path::Path>,
            >,
        ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>>
        {
            self.inner.delete_stream(locations)
        }
    }

    fn timed_out() -> object_store::Error {
        object_store::Error::Generic {
            store: "S3",
            source: "operation timed out".into(),
        }
    }

    fn forbidden() -> object_store::Error {
        object_store::Error::PermissionDenied {
            path: "blocks".to_string(),
            source: "injected 403".into(),
        }
    }

    /// The compactor's flush must survive a transient object store, and must
    /// still commit the window's offsets exactly once.
    ///
    /// Both halves of the flush are covered: the block goes through the
    /// [`BlockWriter`], which retries a whole write, and the compaction index
    /// sidecar goes through a [`RetryingObjectStore`], which retries its put.
    /// The schedule is injected, so nothing here sleeps.
    #[tokio::test]
    async fn a_flush_rides_out_a_transient_object_store_and_commits_once() {
        let store = Arc::new(FlakyPutStore::new(2, timed_out));
        let retry = ObjectStoreRetryPolicy::immediate(4);
        let block_writer = krabka_blockstore::BlockWriter::with_retry_policy(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            retry,
        );
        let sink = super::ObjectStoreCompactionIndexSink::new(RetryingObjectStore::wrap(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            retry,
            ObjectStoreMetrics::unregistered(),
        ));
        let record = krabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset: 10,
            leader_epoch: -1,
            timestamp: 100,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", 100)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let mut consumer = PollAndCommit {
            batches: vec![vec![record]],
            commit_calls: 0,
            committed_offsets: Vec::new(),
        };

        let result = super::run_compactor_consumer_loop(
            &mut consumer,
            &block_writer,
            &sink,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 50_000,
                flush_max_age: hours(1),
            },
            |result| result.polled_records == 0,
            &ServiceMetrics::new(),
        )
        .await
        .expect("the flush rides out the transient failures");

        check!(result.writes == 1);
        // Retried, not merely attempted once.
        check!(store.attempts() > 2);
        // ... and committed exactly once, past the window's last offset.
        check!(consumer.commit_calls == 1);
        check!(consumer.committed_offsets[0][0].offset == krabka_ids::Offset(11));
    }

    /// A store that refuses the credential ends the flush on the first
    /// attempt. The budget is for faults that clear; spending it here would
    /// only delay the report.
    #[tokio::test]
    async fn a_flush_refused_by_the_store_fails_at_once_and_commits_nothing() {
        let store = Arc::new(FlakyPutStore::new(usize::MAX, forbidden));
        let retry = ObjectStoreRetryPolicy::immediate(4);
        let block_writer = krabka_blockstore::BlockWriter::with_retry_policy(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            retry,
        );
        let sink = super::ObjectStoreCompactionIndexSink::new(RetryingObjectStore::wrap(
            Arc::clone(&store) as Arc<dyn ObjectStore>,
            retry,
            ObjectStoreMetrics::unregistered(),
        ));
        let record = krabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset: 10,
            leader_epoch: -1,
            timestamp: 100,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", 100)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let mut consumer = PollAndCommit {
            batches: vec![vec![record]],
            commit_calls: 0,
            committed_offsets: Vec::new(),
        };

        let failure = super::run_compactor_consumer_loop(
            &mut consumer,
            &block_writer,
            &sink,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 50_000,
                flush_max_age: hours(1),
            },
            |result| result.polled_records == 0,
            &ServiceMetrics::new(),
        )
        .await;

        assert!(failure.is_err());
        check!(store.attempts() == 1);
        check!(consumer.commit_calls == 0);
    }

    #[tokio::test]
    async fn run_compactor_consumer_loop_accumulates_multiple_polls_into_one_block() {
        // Two below-threshold polls accumulate into ONE block and commit once.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store);
        let sink = RecordingIndexSink::default();
        let make_record = |offset, timestamp| krabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let mut consumer = PollAndCommit {
            batches: vec![vec![make_record(10, 100)], vec![make_record(11, 200)]],
            commit_calls: 0,
            committed_offsets: Vec::new(),
        };

        let result = super::run_compactor_consumer_loop(
            &mut consumer,
            &block_writer,
            &sink,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 50_000,
                flush_max_age: hours(1),
            },
            |result| result.polled_records == 0,
            &ServiceMetrics::new(),
        )
        .await
        .expect("run compactor consumer loop");

        check!(result.polls == 3);
        check!(result.polled_records == 2);
        // Single block + single commit for the whole two-poll buffer.
        check!(result.writes == 1);
        check!(consumer.commit_calls == 1);
        check!(consumer.committed_offsets[0][0].offset == krabka_ids::Offset(12));
        let manifests = sink.manifests.lock().expect("manifest lock");
        assert!(manifests.len() == 1);
        check!(manifests[0].first_offset == 10);
        check!(manifests[0].last_offset == 11);
        check!(manifests[0].row_count == 2);
    }

    /// Index sink that appends an ordered event marker shared with a
    /// committer, so a test can assert that the block and index writes come
    /// before the offset commit.
    struct OrderingIndexSink {
        store: Arc<dyn ObjectStore>,
        events: Arc<Mutex<Vec<String>>>,
    }

    #[async_trait]
    impl super::CompactionIndexSink for OrderingIndexSink {
        async fn write_manifest(
            &self,
            manifest: &super::CompactionIndexManifest,
        ) -> Result<(), super::CompactionIndexError> {
            // The block object is written before this sink runs, so assert it is
            // already durable when the index manifest lands.
            let head = self
                .store
                .head(&object_store::path::Path::from(manifest.block_key.clone()))
                .await;
            assert!(
                head.is_ok(),
                "block object must exist before index manifest"
            );
            self.events
                .lock()
                .expect("events lock")
                .push(format!("index:{}", manifest.block_key));
            Ok(())
        }
    }

    /// Committer that asserts that the buffered block object is durable, and
    /// then records the commit event after the block and index writes.
    struct OrderingCommitter {
        store: Arc<dyn ObjectStore>,
        events: Arc<Mutex<Vec<String>>>,
        block_key: String,
    }

    #[async_trait]
    impl super::CompactionOffsetCommitter for OrderingCommitter {
        async fn commit_offsets(
            &self,
            offsets: &[super::CompactionPartitionOffset],
        ) -> Result<(), super::CompactionCommitError> {
            // Commits must only happen after the block is durably written.
            let head = self
                .store
                .head(&object_store::path::Path::from(self.block_key.clone()))
                .await;
            assert!(head.is_ok(), "block must be durable before offset commit");
            for offset in offsets {
                self.events
                    .lock()
                    .expect("events lock")
                    .push(format!("commit:{}", offset.offset));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn run_compactor_loop_commits_offsets_only_after_durable_block_write() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let block_writer = krabka_blockstore::BlockWriter::new(object_store.clone());
        let events = Arc::new(Mutex::new(Vec::<String>::new()));
        let block_key = super::compaction_partition_object_key(
            "tenant-a",
            super::MetricBlockKind::Float,
            super::PartitionIndex(0),
            10,
            11,
        );
        let sink = OrderingIndexSink {
            store: object_store.clone(),
            events: Arc::clone(&events),
        };
        let committer = OrderingCommitter {
            store: object_store.clone(),
            events: Arc::clone(&events),
            block_key: block_key.clone(),
        };
        let make_record = |offset, timestamp| krabka_client_consumer::ConsumerRecord {
            topic: crate::WAL_TOPIC.to_string(),
            partition: 0,
            offset,
            leader_epoch: -1,
            timestamp,
            key: None,
            value: Some(bytes::Bytes::from(
                float_record("tenant-a", "up", "api", timestamp)
                    .encode()
                    .expect("encode wal"),
            )),
            headers: Vec::new(),
        };
        let mut poller = QueuePoller {
            batches: vec![vec![make_record(10, 100)], vec![make_record(11, 200)]],
        };

        let result = super::run_compactor_loop(
            &mut poller,
            &block_writer,
            &sink,
            &committer,
            super::CompactionLoopConfig {
                wal_topic: crate::WAL_TOPIC.to_string(),
                poll_timeout: millis(1),
                flush_max_rows: 50_000,
                flush_max_age: hours(1),
            },
            |result| result.polled_records == 0,
            &ServiceMetrics::new(),
        )
        .await
        .expect("run compactor loop");

        assert!(result.writes == 1);
        // Index manifest write (which only runs after the durable block put) must
        // precede the offset commit in the recorded event order.
        let recorded = events.lock().expect("events lock").clone();
        assert!(recorded == vec![format!("index:{block_key}"), "commit:12".to_string()]);
    }

    struct PollAndCommit {
        batches: Vec<Vec<krabka_client_consumer::ConsumerRecord>>,
        commit_calls: usize,
        committed_offsets: Vec<Vec<super::CompactionPartitionOffset>>,
    }

    #[async_trait]
    impl super::CompactionConsumerPoll for PollAndCommit {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<Vec<krabka_client_consumer::ConsumerRecord>, super::CompactionConsumerPollError>
        {
            if self.batches.is_empty() {
                Ok(Vec::new())
            } else {
                Ok(self.batches.remove(0))
            }
        }
    }

    #[async_trait]
    impl super::CompactionConsumerCommit for PollAndCommit {
        async fn commit_offsets_sync(
            &self,
            _topic: &str,
            _offsets: &[super::CompactionPartitionOffset],
        ) -> Result<(), super::CompactionConsumerCommitError> {
            Err(super::CompactionConsumerCommitError::Commit(
                "immutable commit path should not be used by this adapter test".into(),
            ))
        }
    }

    #[async_trait]
    impl super::CompactionConsumerCommitMut for PollAndCommit {
        async fn commit_offsets_sync_mut(
            &mut self,
            offsets: &[super::CompactionPartitionOffset],
        ) -> Result<(), super::CompactionConsumerCommitError> {
            self.commit_calls += 1;
            self.committed_offsets.push(offsets.to_vec());
            Ok(())
        }
    }

    struct RecordingSelectedCommit {
        calls: Arc<std::sync::Mutex<Vec<Vec<super::CompactionPartitionOffset>>>>,
    }

    #[async_trait]
    impl super::CompactionConsumerCommit for RecordingSelectedCommit {
        async fn commit_offsets_sync(
            &self,
            _topic: &str,
            offsets: &[super::CompactionPartitionOffset],
        ) -> Result<(), super::CompactionConsumerCommitError> {
            self.calls.lock().unwrap().push(offsets.to_vec());
            Ok(())
        }
    }

    #[tokio::test]
    async fn durable_consumer_forwards_each_exact_offset_set() {
        use super::CompactionConsumerCommitMut as _;

        let calls = Arc::new(std::sync::Mutex::new(Vec::new()));
        let inner = RecordingSelectedCommit {
            calls: Arc::clone(&calls),
        };
        let mut consumer = super::DurableCompactionConsumer::new(inner, crate::WAL_TOPIC);
        consumer
            .commit_offsets_sync_mut(&[super::CompactionPartitionOffset {
                partition: krabka_ids::PartitionIndex(0),
                offset: krabka_ids::Offset(11),
            }])
            .await
            .unwrap();
        consumer
            .commit_offsets_sync_mut(&[super::CompactionPartitionOffset {
                partition: krabka_ids::PartitionIndex(1),
                offset: krabka_ids::Offset(21),
            }])
            .await
            .unwrap();

        let calls = calls.lock().unwrap();
        check!(calls.len() == 2);
        check!(calls[1].len() == 1);
        check!(calls[1][0].partition == krabka_ids::PartitionIndex(1));
        check!(calls[1][0].offset == krabka_ids::Offset(21));
    }

    #[test]
    fn compact_wal_records_extracts_histograms_and_exemplars() {
        let mut record = float_record("tenant-a", "request_duration_seconds", "api", 20);
        record.exemplars = vec![WalExemplar {
            labels: vec![
                ("trace_id".into(), "abc".into()),
                ("span_id".into(), "def".into()),
                ("kind".into(), "slow".into()),
            ],
            value: 2.0,
            timestamp_ms: 19,
        }];
        let hist_record = WalRecord {
            tenant: "tenant-a".into(),
            labels: record.labels.clone(),
            payload: SamplePayload::Hist {
                timestamp_ms: 21,
                hist: hist(),
            },
            exemplars: Vec::new(),
        };

        let compacted = compact_wal_records(&[record.clone(), hist_record]);

        assert!(compacted.len() == 1);
        assert!(compacted[0].histogram_rows.len() == 1);
        check!(compacted[0].histogram_rows[0].timestamp_ms == 21);
        assert!(compacted[0].exemplar_rows.len() == 1);
        check!(compacted[0].exemplar_rows[0].fingerprint == record.series_fingerprint());
        check!(compacted[0].exemplar_rows[0].trace_id.as_deref() == Some("abc"));
        check!(compacted[0].exemplar_rows[0].span_id.as_deref() == Some("def"));
        check!(compacted[0].exemplar_rows[0].labels == vec![("kind".into(), "slow".into())]);
    }

    #[test]
    fn compact_wal_records_does_not_duplicate_series_exemplars_per_sample() {
        let labels = krabka_blockstore::Labels::from_iter([
            ("__name__".to_string(), "http_requests_total".to_string()),
            ("job".to_string(), "api".to_string()),
        ]);
        let exemplar_labels =
            krabka_blockstore::Labels::from_iter([("trace_id".to_string(), "abc".to_string())]);
        let records = wal_records_from_series(
            "tenant-a",
            &[DecodedSeries {
                labels: labels.clone(),
                samples: vec![DecodedSample::new(20, 2.0), DecodedSample::new(30, 3.0)],
                histograms: Vec::new(),
                exemplars: vec![DecodedExemplar {
                    labels: exemplar_labels,
                    timestamp_ms: 19,
                    value: 2.0,
                }],
                metadata: None,
            }],
        );

        let compacted = compact_wal_records(&records);

        assert!(compacted.len() == 1);
        check!(compacted[0].float_rows.len() == 2);
        assert!(compacted[0].exemplar_rows.len() == 1);
        check!(compacted[0].exemplar_rows[0].fingerprint == labels.fingerprint());
        check!(compacted[0].exemplar_rows[0].trace_id.as_deref() == Some("abc"));
    }

    #[test]
    fn compact_wal_records_extracts_metric_metadata() {
        let record = WalRecord {
            tenant: "tenant-a".into(),
            labels: vec![("__name__".into(), "http_requests_total".into())],
            payload: SamplePayload::Metadata {
                metric_family_name: "http_requests_total".into(),
                metric_type: "counter".into(),
                help: "Total HTTP requests.".into(),
                unit: "requests".into(),
            },
            exemplars: Vec::new(),
        };

        let compacted = compact_wal_records(std::slice::from_ref(&record));

        assert!(compacted.len() == 1);
        assert!(
            compacted[0].metadata_rows
                == vec![super::MetadataRow {
                    fingerprint: record.series_fingerprint(),
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: "counter".to_string(),
                    help: "Total HTTP requests.".to_string(),
                    unit: "requests".to_string(),
                }]
        );
    }

    #[test]
    fn metadata_index_queries_tenant_metric_metadata() {
        let rows = compact_wal_records(&[
            WalRecord {
                tenant: "tenant-a".into(),
                labels: vec![("__name__".into(), "http_requests_total".into())],
                payload: SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".into(),
                    metric_type: "counter".into(),
                    help: "Total HTTP requests.".into(),
                    unit: "requests".into(),
                },
                exemplars: Vec::new(),
            },
            WalRecord {
                tenant: "tenant-a".into(),
                labels: vec![("__name__".into(), "http_requests_total".into())],
                payload: SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".into(),
                    metric_type: "counter".into(),
                    help: "Total HTTP requests.".into(),
                    unit: "requests".into(),
                },
                exemplars: Vec::new(),
            },
            WalRecord {
                tenant: "tenant-a".into(),
                labels: vec![("__name__".into(), "up".into())],
                payload: SamplePayload::Metadata {
                    metric_family_name: "up".into(),
                    metric_type: "gauge".into(),
                    help: "Target health.".into(),
                    unit: String::new(),
                },
                exemplars: Vec::new(),
            },
            WalRecord {
                tenant: "tenant-b".into(),
                labels: vec![("__name__".into(), "http_requests_total".into())],
                payload: SamplePayload::Metadata {
                    metric_family_name: "http_requests_total".into(),
                    metric_type: "gauge".into(),
                    help: "Wrong tenant.".into(),
                    unit: String::new(),
                },
                exemplars: Vec::new(),
            },
        ]);

        let index = crate::MetadataIndex::from_compaction_rows(&rows);
        let tenant_a_all = index.metadata("tenant-a", None);
        let tenant_a_http = index.metadata("tenant-a", Some("http_requests_total"));

        assert!(tenant_a_all.len() == 2);
        check!(tenant_a_all[0].metric_family_name == "http_requests_total");
        check!(tenant_a_all[1].metric_family_name == "up");
        assert!(tenant_a_http.len() == 1);
        check!(tenant_a_http[0].metric_type == "counter");
        check!(tenant_a_http[0].help == "Total HTTP requests.");
        check!(index.metadata("tenant-b", Some("http_requests_total"))[0].metric_type == "gauge");
    }

    #[test]
    fn encode_tenant_batches_builds_float_and_histogram_batches() {
        let compacted = compact_wal_records(&[
            float_record("tenant-a", "up", "api", 10),
            WalRecord {
                tenant: "tenant-a".into(),
                labels: vec![("__name__".into(), "latency".into())],
                payload: SamplePayload::Hist {
                    timestamp_ms: 20,
                    hist: hist(),
                },
                exemplars: Vec::new(),
            },
        ]);

        let batches = encode_tenant_batches(&compacted[0]).unwrap();

        assert!(batches.float.as_ref().unwrap().num_rows() == 1);
        assert!(batches.native_histograms.as_ref().unwrap().num_rows() == 1);
    }

    #[test]
    fn encode_tenant_batches_builds_exemplar_sidecar_batch() {
        let mut record = float_record("tenant-a", "request_duration_seconds", "api", 20);
        record.exemplars = vec![WalExemplar {
            labels: vec![
                ("trace_id".into(), "abc".into()),
                ("span_id".into(), "def".into()),
                ("kind".into(), "slow".into()),
            ],
            value: 2.0,
            timestamp_ms: 19,
        }];
        let compacted = compact_wal_records(std::slice::from_ref(&record));

        let batches = encode_tenant_batches(&compacted[0]).unwrap();

        let batch = batches.exemplars.as_ref().expect("exemplar sidecar");
        assert!(batch.num_rows() == 1);
        assert!(batch.schema() == crate::exemplar_schema());

        let trace_ids = batch
            .column_by_name("trace_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let span_ids = batch
            .column_by_name("span_id")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let labels = batch
            .column_by_name("labels")
            .unwrap()
            .as_any()
            .downcast_ref::<arrow::array::MapArray>()
            .unwrap();
        let label_entries = labels.value(0);

        check!(trace_ids.value(0) == "abc");
        check!(span_ids.value(0) == "def");
        assert!(label_entries.column(0).len() == 1);
        check!(
            label_entries
                .column(0)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0)
                == "kind"
        );
        check!(
            label_entries
                .column(1)
                .as_any()
                .downcast_ref::<arrow::array::StringArray>()
                .unwrap()
                .value(0)
                == "slow"
        );
    }
}

mod clock_columns;
mod clock_reading_row;
mod compact_metric_blocks_once;
mod compact_wal_records;
mod compacted_block_request;
mod compacted_block_write;
mod compacted_metric_object_key;
mod compaction_batch_result;
mod compaction_batch_span;
mod compaction_buffer;
mod compaction_clock;
mod compaction_commit_error;
mod compaction_consumer_commit;
mod compaction_consumer_commit_error;
mod compaction_consumer_commit_mut;
mod compaction_consumer_committer;
mod compaction_consumer_poll;
mod compaction_consumer_poll_error;
mod compaction_consumer_record_error;
mod compaction_index_error;
mod compaction_index_key;
mod compaction_index_manifest;
mod compaction_index_sink;
mod compaction_loop_config;
mod compaction_loop_context;
mod compaction_loop_result;
mod compaction_manifest_error;
mod compaction_object_key;
mod compaction_object_plan;
mod compaction_object_plan_for_rows;
mod compaction_object_prefix;
mod compaction_offset_committer;
mod compaction_partition_object_key;
mod compaction_partition_object_plan;
mod compaction_partition_offset;
mod compaction_poll_error;
mod compaction_poll_result;
mod compaction_retention_error;
mod compaction_retention_phase;
mod compaction_retention_stats;
mod compaction_series_labels;
mod compaction_wal_record;
mod compaction_wal_records_from_consumer_records;
mod compaction_window_error;
mod compaction_window_result;
mod compaction_write_error;
mod consumer;
mod consumer_build_error;
mod deduplicate_series_timestamp_runs;
mod default_flush_max_age;
mod default_flush_max_rows;
mod deferred_block_deletions;
mod durable_compaction_consumer;
mod encode_clock_reading_rows;
mod encode_exemplar_rows;
mod encode_metadata_rows;
mod encode_tenant_batches;
mod enforce_compaction_retention;
mod exemplar_row;
mod float_row;
mod flush_buffer;
mod flush_buffer_with_consumer;
mod list_compaction_manifests;
mod materialize_metric_erasure_requests;
mod merge_metric_blocks;
mod metadata_row;
mod metric_block_kind;
mod metric_compaction_error;
mod metric_compaction_job;
mod metric_compaction_pass;
mod metrics_compactor_build_error;
mod metrics_compactor_config;
mod metrics_compactor_config_error;
mod metrics_compactor_runtime;
mod native_histogram_row;
mod object_store_compaction_index_sink;
mod plan_metric_compactions;
mod poll_compactor_consumer_once;
mod poll_compactor_once;
mod process_compaction_partition_window;
mod process_compaction_record_batch;
mod process_compaction_record_batch_with_consumer;
mod run_compactor_consumer_loop;
mod run_compactor_consumer_loop_with_clock;
mod run_compactor_loop;
mod run_compactor_loop_with_clock;
mod series_labels_for_kind;
mod system_compaction_clock;
mod tenant_batches;
mod tenant_compaction_rows;
mod validate_non_empty;
mod write_compacted_block;
mod write_compacted_tenant_blocks;
mod write_compacted_tenant_blocks_with_partition;
mod write_compacted_tenant_partition_blocks;
mod write_compaction_partition_window;

use clock_columns::ClockColumns;
pub use clock_reading_row::ClockReadingRow;
pub use compact_metric_blocks_once::compact_metric_blocks_once;
pub use compact_wal_records::compact_wal_records;
use compacted_block_request::CompactedBlockRequest;
pub use compacted_block_write::CompactedBlockWrite;
use compacted_metric_object_key::compacted_metric_object_key;
pub use compaction_batch_result::CompactionBatchResult;
use compaction_batch_span::compaction_batch_span;
use compaction_buffer::CompactionBuffer;
pub use compaction_clock::CompactionClock;
pub use compaction_commit_error::CompactionCommitError;
pub use compaction_consumer_commit::CompactionConsumerCommit;
pub use compaction_consumer_commit_error::CompactionConsumerCommitError;
pub use compaction_consumer_commit_mut::CompactionConsumerCommitMut;
pub use compaction_consumer_committer::CompactionConsumerCommitter;
pub use compaction_consumer_poll::CompactionConsumerPoll;
pub use compaction_consumer_poll_error::CompactionConsumerPollError;
pub use compaction_consumer_record_error::CompactionConsumerRecordError;
pub use compaction_index_error::CompactionIndexError;
#[cfg_attr(test, mutants::skip)]
use compaction_index_key::compaction_index_key;
pub use compaction_index_manifest::CompactionIndexManifest;
pub use compaction_index_sink::CompactionIndexSink;
pub use compaction_loop_config::CompactionLoopConfig;
pub use compaction_loop_context::CompactionLoopContext;
pub use compaction_loop_result::CompactionLoopResult;
pub use compaction_manifest_error::CompactionManifestError;
pub use compaction_object_key::compaction_object_key;
pub use compaction_object_plan::CompactionObjectPlan;
#[cfg_attr(test, mutants::skip)]
pub use compaction_object_plan::compaction_object_plan;
pub use compaction_object_plan_for_rows::compaction_object_plan_for_rows;
use compaction_object_prefix::COMPACTION_OBJECT_PREFIX;
pub use compaction_offset_committer::CompactionOffsetCommitter;
pub use compaction_partition_object_key::compaction_partition_object_key;
#[cfg_attr(test, mutants::skip)]
pub use compaction_partition_object_plan::compaction_partition_object_plan;
pub use compaction_partition_offset::CompactionPartitionOffset;
pub use compaction_poll_error::CompactionPollError;
pub use compaction_poll_result::CompactionPollResult;
pub use compaction_retention_error::CompactionRetentionError;
pub use compaction_retention_phase::CompactionRetentionPhase;
pub use compaction_retention_stats::CompactionRetentionStats;
pub use compaction_series_labels::CompactionSeriesLabels;
pub use compaction_wal_record::CompactionWalRecord;
pub use compaction_wal_records_from_consumer_records::compaction_wal_records_from_consumer_records;
pub use compaction_window_error::CompactionWindowError;
pub use compaction_window_result::CompactionWindowResult;
pub use compaction_write_error::CompactionWriteError;
pub use consumer::WalAssignmentConsumer;
use consumer_build_error::consumer_build_error;
use deduplicate_series_timestamp_runs::deduplicate_series_timestamp_runs;
pub use default_flush_max_age::DEFAULT_FLUSH_MAX_AGE;
pub use default_flush_max_rows::DEFAULT_FLUSH_MAX_ROWS;
pub use deferred_block_deletions::DeferredBlockDeletions;
pub use durable_compaction_consumer::DurableCompactionConsumer;
use encode_clock_reading_rows::encode_clock_reading_rows;
use encode_exemplar_rows::encode_exemplar_rows;
use encode_metadata_rows::encode_metadata_rows;
pub use encode_tenant_batches::encode_tenant_batches;
pub use enforce_compaction_retention::enforce_compaction_retention;
pub use exemplar_row::ExemplarRow;
use exemplar_row::exemplar_row;
pub use float_row::FloatRow;
use flush_buffer::flush_buffer;
use flush_buffer_with_consumer::flush_buffer_with_consumer;
pub use list_compaction_manifests::list_compaction_manifests;
use materialize_metric_erasure_requests::materialize_metric_erasure_requests;
use merge_metric_blocks::merge_metric_blocks;
pub use metadata_row::MetadataRow;
pub use metric_block_kind::MetricBlockKind;
pub use metric_compaction_error::MetricCompactionError;
pub use metric_compaction_job::MetricCompactionJob;
pub use metric_compaction_pass::MetricCompactionPass;
pub use metrics_compactor_build_error::MetricsCompactorBuildError;
pub use metrics_compactor_config::MetricsCompactorConfig;
pub use metrics_compactor_config_error::MetricsCompactorConfigError;
pub use metrics_compactor_runtime::MetricsCompactorRuntime;
pub use native_histogram_row::NativeHistogramRow;
pub use object_store_compaction_index_sink::ObjectStoreCompactionIndexSink;
pub use plan_metric_compactions::plan_metric_compactions;
pub use poll_compactor_consumer_once::poll_compactor_consumer_once;
pub use poll_compactor_once::poll_compactor_once;
pub use process_compaction_partition_window::process_compaction_partition_window;
pub use process_compaction_record_batch::process_compaction_record_batch;
use process_compaction_record_batch_with_consumer::process_compaction_record_batch_with_consumer;
pub use run_compactor_consumer_loop::run_compactor_consumer_loop;
pub use run_compactor_consumer_loop_with_clock::run_compactor_consumer_loop_with_clock;
pub use run_compactor_loop::run_compactor_loop;
pub use run_compactor_loop_with_clock::run_compactor_loop_with_clock;
use series_labels_for_kind::series_labels_for_kind;
pub use system_compaction_clock::SystemCompactionClock;
pub use tenant_batches::TenantBatches;
pub use tenant_compaction_rows::TenantCompactionRows;
use validate_non_empty::validate_non_empty;
use write_compacted_block::write_compacted_block;
pub use write_compacted_tenant_blocks::write_compacted_tenant_blocks;
use write_compacted_tenant_blocks_with_partition::write_compacted_tenant_blocks_with_partition;
pub use write_compacted_tenant_partition_blocks::write_compacted_tenant_partition_blocks;
use write_compaction_partition_window::write_compaction_partition_window;
