use super::*;

mod a_float_only_store_says_it_holds_no_histograms {
    use super::*;

    #[tokio::test]
    pub(crate) async fn a_float_only_store_says_it_holds_no_histograms() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut floats = BlockStore::new(Arc::clone(&object_store), base.clone());
        let series = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        write_float_block(
            &mut floats,
            "metrics/float/0001.parquet",
            &series,
            1_000,
            1.0,
        )
        .await;
        let matchers = [crate::PromqlMatcher {
            name: "__name__".to_string(),
            op: krabka_blockstore::MatchOp::Eq,
            value: "up".to_string().into(),
        }];

        // `None` is a store built without a histogram block store, and an empty
        // histogram block store is the one `from_compaction_manifests` builds when
        // no manifest names a histogram block.
        struct StoreCase {
            description: &'static str,
            store: MetricBlockStore,
        }
        for case in [
            StoreCase {
                description: "no histogram store",
                store: MetricBlockStore::new(floats.clone()),
            },
            StoreCase {
                description: "an empty histogram store",
                store: MetricBlockStore::with_histograms(
                    floats,
                    BlockStore::new(object_store, base),
                ),
            },
        ] {
            check!(
                !case
                    .store
                    .may_have_histograms("tenant-a", &matchers, 0, 10_000)
                    .await
                    .unwrap(),
                "{}",
                case.description
            );
        }
    }
}

mod an_instant_query_reads_each_float_block_a_fixed_number_of_times {
    use krabka_metrics::{
        FloatRow, ObjectStoreCompactionIndexSink, TenantCompactionRows, list_compaction_manifests,
        write_compacted_tenant_blocks,
    };

    use super::*;

    const QUERY: &str = "count(count_over_time(soak_metric[60s]))";
    const QUERY_TIME_MS: i64 = 100_000;
    const SERIES_PER_BLOCK: u64 = 3;

    /// Writes `blocks` float-only metric blocks the way the compactor does and
    /// returns their manifests.
    async fn write_float_only_blocks(
        object_store: &Arc<dyn ObjectStore>,
        blocks: i64,
    ) -> Vec<CompactionIndexManifest> {
        let writer = krabka_blockstore::BlockWriter::new(Arc::clone(object_store));
        let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(object_store));
        for block in 0..blocks {
            let mut series_labels = std::collections::BTreeMap::new();
            let mut float_rows = Vec::new();
            for series in 0..SERIES_PER_BLOCK {
                let labels = labels(&[
                    crate::test_support::TestLabelPair::new("__name__", "soak_metric"),
                    crate::test_support::TestLabelPair::new("series", &series.to_string()),
                ]);
                let fingerprint = labels.fingerprint();
                series_labels.insert(fingerprint, labels.into());
                float_rows.push(FloatRow {
                    fingerprint,
                    timestamp_ms: QUERY_TIME_MS - 1_000 * (block + 1),
                    value: 1.0,
                    start_timestamp_ms: None,
                });
            }
            let rows = TenantCompactionRows {
                tenant: "tenant-a".to_string(),
                series_labels,
                float_rows,
                histogram_rows: Vec::new(),
                exemplar_rows: Vec::new(),
                metadata_rows: Vec::new(),
                clock_rows: Vec::new(),
            };
            write_compacted_tenant_blocks(&writer, &sink, &rows, block, block)
                .await
                .unwrap();
        }
        list_compaction_manifests(object_store).await.unwrap()
    }

    /// The requests a cold query and then a warm query made over `blocks` blocks.
    async fn query_requests(blocks: i64) -> QueryRequests {
        let backing: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let manifests = write_float_only_blocks(&backing, blocks).await;
        let counting_store = CountingObjectStore::wrap(backing);
        let counting = counting_store.store;
        let counts = counting_store.counts;
        let base = url::Url::parse("memory:///").unwrap();
        let store = MetricBlockStore::from_compaction_manifests(
            BlockStore::new(Arc::clone(&counting), base.clone()),
            Some(BlockStore::new(counting, base)),
            &manifests,
        );
        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let expected = QueryResult::InstantVector(vec![InstantSample {
            labels: Labels::new().into(),
            ts_ms: QUERY_TIME_MS,
            value: SampleValue::Float(3.0),
            drop_name: false,
        }]);

        let mut per_query = Vec::new();
        for _ in 0..2 {
            let before = *counts.lock().unwrap();
            let result = engine
                .query_instant(&tenant_id("tenant-a"), QUERY, QUERY_TIME_MS)
                .await
                .unwrap();
            assert2::assert!(result == expected);
            per_query.push(counts.lock().unwrap().since(before));
        }
        QueryRequests {
            cold: per_query[0],
            warm: per_query[1],
        }
    }

    struct QueryRequests {
        cold: RequestCounts,
        warm: RequestCounts,
    }

    #[tokio::test]
    pub(crate) async fn an_instant_query_reads_each_float_block_a_fixed_number_of_times() {
        // Per block, each query makes one `head`: it finds a block that deletion
        // removed and validates a cached footer. The cold query then reads the
        // footer and its page index as one range and the column chunks as a
        // second. The warm query has the footer cached and reads only the column
        // chunks. Nothing lists, and nothing reads a whole object.
        for blocks in [1_usize, 10] {
            let requests = query_requests(i64::try_from(blocks).unwrap()).await;
            check!(requests.cold == RequestCounts::per_block(blocks, 1, 2));
            check!(requests.warm == RequestCounts::per_block(blocks, 1, 1));
        }
    }
}

mod histogram_blocks_answer_count_over_time_and_rate {
    use krabka_metrics::{
        NativeHistogram, NativeHistogramRow, ObjectStoreCompactionIndexSink, ResetHint,
        list_compaction_manifests, write_compacted_tenant_blocks,
    };

    use super::*;
    use crate::test_support::nh;

    struct HistogramPoint {
        timestamp_ms: i64,
        count: f64,
    }

    struct AvailabilityWindow {
        start_ms: i64,
        end_ms: i64,
        expected: bool,
    }

    struct HistogramQueryCase {
        query: &'static str,
        expected: SampleValue,
    }

    fn histogram(count: f64) -> NativeHistogram {
        nh(
            count,
            count,
            0,
            &[crate::test_support::BucketCount { index: 0, count }],
        )
    }

    /// A store whose only blocks hold native histograms, written the way the
    /// compactor writes them, so no float block exists.
    async fn histogram_only_store(series: &Labels) -> MetricBlockStore {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let rows = compaction_rows_for_series(
            series,
            Vec::new(),
            [
                HistogramPoint {
                    timestamp_ms: 30_000,
                    count: 1.0,
                },
                HistogramPoint {
                    timestamp_ms: 60_000,
                    count: 3.0,
                },
                HistogramPoint {
                    timestamp_ms: 90_000,
                    count: 5.0,
                },
            ]
            .into_iter()
            .map(|point| NativeHistogramRow {
                fingerprint: series.fingerprint(),
                timestamp_ms: point.timestamp_ms,
                hist: histogram(point.count),
            })
            .collect(),
        );
        write_compacted_tenant_blocks(
            &krabka_blockstore::BlockWriter::new(Arc::clone(&object_store)),
            &ObjectStoreCompactionIndexSink::new(Arc::clone(&object_store)),
            &rows,
            0,
            0,
        )
        .await
        .unwrap();
        let manifests = list_compaction_manifests(&object_store).await.unwrap();
        let base = url::Url::parse("memory:///").unwrap();
        MetricBlockStore::from_compaction_manifests(
            BlockStore::new(Arc::clone(&object_store), base.clone()),
            Some(BlockStore::new(object_store, base)),
            &manifests,
        )
    }

    #[tokio::test]
    pub(crate) async fn histogram_blocks_answer_count_over_time_and_rate() {
        let series = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "request_duration_seconds"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        let store = histogram_only_store(&series).await;
        let matchers = [crate::PromqlMatcher {
            name: "__name__".to_string(),
            op: krabka_blockstore::MatchOp::Eq,
            value: "request_duration_seconds".to_string().into(),
        }];

        // The store says where histograms can be, and says it from its index.
        for window in [
            AvailabilityWindow {
                start_ms: 0,
                end_ms: 100_000,
                expected: true,
            },
            AvailabilityWindow {
                start_ms: 100_001,
                end_ms: 200_000,
                expected: false,
            },
        ] {
            check!(
                store
                    .may_have_histograms("tenant-a", &matchers, window.start_ms, window.end_ms)
                    .await
                    .unwrap()
                    == window.expected,
                "[{}, {}]",
                window.start_ms,
                window.end_ms
            );
        }

        let engine = PromqlEngine::new(Arc::new(store), EngineOpts::default());
        let job = labels(&[crate::test_support::TestLabelPair::new("job", "api")]);
        for case in [
            HistogramQueryCase {
                query: "count_over_time(request_duration_seconds[2m])",
                expected: SampleValue::Float(3.0),
            },
            // The samples rise by 4 over 60s. Prometheus extrapolates the rise
            // by half a sample interval toward the window start, to 5 over the
            // 120s window.
            HistogramQueryCase {
                query: "rate(request_duration_seconds[2m])",
                expected: SampleValue::Histogram(NativeHistogram {
                    reset_hint: ResetHint::Gauge,
                    ..histogram(5.0 / 120.0)
                }),
            },
        ] {
            let result = engine
                .query_instant(&tenant_id("tenant-a"), case.query, 90_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("{}: expected vector", case.query);
            };
            assert2::assert!(samples.len() == 1, "{}", case.query);
            let expected_labels: crate::PromqlLabels = job.clone().into();
            assert2::assert!(samples[0].labels == expected_labels);
            assert2::assert!(samples[0].ts_ms == 90_000);
            assert2::assert!(samples[0].value == case.expected, "{}", case.query);
        }
    }
}

mod prometheus_query_reads_float_samples_from_blockstore {
    use super::*;

    #[tokio::test]
    pub(crate) async fn prometheus_query_reads_float_samples_from_blockstore() {
        let mut block_store = MemoryBlockStoreFixture::build().writer();

        let series_labels = up_api_labels();
        let fp = series_labels.fingerprint();
        let batch = up_api_sample_batch(&[(1_000, 1.0)]);
        let block_meta = block_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/float/0001.parquet",
                float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        block_store
            .index_mut()
            .add_series("tenant-a", fp, &series_labels);
        block_store.index_mut().add_block(&block_meta);

        assert_up_is_one(MetricBlockStore::new(block_store), series_labels).await;
    }
}

mod prometheus_query_rebuilds_float_index_from_compaction_manifest {
    use krabka_blockstore::{
        BlockLevel, BlockTimestampUnit, BlockWriter, CompactionPolicy, DEFAULT_BLOCK_READ_MAX,
        ERASURE_REQUEST_PREFIX, ErasureRequest, LabelMatcher, MatchOp, put_erasure_request,
    };
    use krabka_metrics::{
        DeferredBlockDeletions, FloatRow, ObjectStoreCompactionIndexSink, TenantCompactionRows,
        compact_metric_blocks_once, list_compaction_manifests, write_compacted_tenant_blocks,
    };
    use krabka_units::hours;

    use super::*;

    struct FloatRowCase {
        timestamp_ms: i64,
        value: f64,
    }

    struct TenantCompactionCase {
        tenant: &'static str,
        offset: i64,
        samples: Vec<FloatRowCase>,
    }

    struct ExpectedQueryCase {
        query: &'static str,
        value: f64,
    }

    struct TenantLedgerCase {
        tenant: &'static str,
        total: f64,
        delta: f64,
    }

    #[tokio::test]
    pub(crate) async fn prometheus_query_rebuilds_float_index_from_compaction_manifest() {
        let series_labels = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        let batch = encode_float_samples(&[float_sample_row(
            series_labels.fingerprint(),
            1_000,
            1.0,
            None,
        )])
        .unwrap();
        let store = manifest_store(ManifestBlock {
            kind: MetricBlockKind::Float,
            block_key: "metrics/float/0001.parquet",
            schema: float_sample_schema(),
            batch,
            last_offset: 0,
            series_labels: series_labels.clone(),
        })
        .await;
        assert_up_is_one(store, series_labels).await;
    }

    #[tokio::test]
    async fn composed_queries_survive_compaction_publication_deletion_and_reload() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let series = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "requests_total"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        // The replay duplicates one timestamp. The foreign tenant deliberately has
        // the same fingerprint and timestamps, with different values.
        for case in [
            TenantCompactionCase {
                tenant: "tenant-a",
                offset: 1,
                samples: vec![
                    FloatRowCase {
                        timestamp_ms: 30_000,
                        value: 2.0,
                    },
                    FloatRowCase {
                        timestamp_ms: 60_000,
                        value: 5.0,
                    },
                ],
            },
            TenantCompactionCase {
                tenant: "tenant-a",
                offset: 3,
                samples: vec![
                    FloatRowCase {
                        timestamp_ms: 60_000,
                        value: 5.0,
                    },
                    FloatRowCase {
                        timestamp_ms: 90_000,
                        value: 9.0,
                    },
                ],
            },
            TenantCompactionCase {
                tenant: "tenant-b",
                offset: 5,
                samples: vec![
                    FloatRowCase {
                        timestamp_ms: 30_000,
                        value: 100.0,
                    },
                    FloatRowCase {
                        timestamp_ms: 60_000,
                        value: 200.0,
                    },
                    FloatRowCase {
                        timestamp_ms: 90_000,
                        value: 300.0,
                    },
                ],
            },
        ] {
            let mut series_labels_by_fingerprint = std::collections::BTreeMap::new();
            series_labels_by_fingerprint.insert(series.fingerprint(), series.clone().into());
            let rows = TenantCompactionRows {
                tenant: case.tenant.to_string(),
                series_labels: series_labels_by_fingerprint,
                float_rows: case
                    .samples
                    .into_iter()
                    .map(|sample| FloatRow {
                        fingerprint: series.fingerprint(),
                        timestamp_ms: sample.timestamp_ms,
                        value: sample.value,
                        start_timestamp_ms: None,
                    })
                    .collect(),
                histogram_rows: Vec::new(),
                exemplar_rows: Vec::new(),
                metadata_rows: Vec::new(),
                clock_rows: Vec::new(),
            };
            write_compacted_tenant_blocks(
                &BlockWriter::new(object_store.clone()),
                &ObjectStoreCompactionIndexSink::new(object_store.clone()),
                &rows,
                case.offset,
                case.offset + 1,
            )
            .await
            .unwrap();
        }
        let policy = CompactionPolicy::new(
            8,
            1_000_000,
            BlockLevel(4),
            hours(24),
            BlockTimestampUnit::Millis,
        );
        let mut deferred = DeferredBlockDeletions::new();
        let mut retired = Vec::new();
        for stage in 0..5 {
            if stage == 3 {
                let request = ErasureRequest::new(
                    "tenant-a",
                    "requests_total",
                    vec![vec![LabelMatcher::new(
                        "__name__",
                        MatchOp::Eq,
                        "requests_total",
                    )]],
                    60_000_000_000,
                    60_000_000_000,
                    90_000_000_000,
                );
                put_erasure_request(&object_store, ERASURE_REQUEST_PREFIX, &request)
                    .await
                    .unwrap();
            }
            if stage > 0 {
                compact_metric_blocks_once(
                    &object_store,
                    &BlockWriter::new(object_store.clone()),
                    &ObjectStoreCompactionIndexSink::new(object_store.clone()),
                    policy,
                    DEFAULT_BLOCK_READ_MAX,
                    &mut deferred,
                )
                .await
                .unwrap();
                if stage == 1 {
                    retired = deferred.keys().to_vec();
                    assert2::assert!(retired.len() == 2);
                }
                if stage == 3 {
                    assert2::assert!(deferred.keys().len() == 1);
                }
                if stage == 4 {
                    assert2::assert!(deferred.is_empty());
                }
                for key in &retired {
                    assert2::assert!(
                        object_store
                            .head(&ObjectPath::from(key.clone()))
                            .await
                            .is_ok()
                            == (stage == 1)
                    );
                }
            }
            let manifests = list_compaction_manifests(&object_store).await.unwrap();
            let cold = MetricBlockStore::from_compaction_manifests(
                BlockStore::new(object_store.clone(), url::Url::parse("memory:///").unwrap()),
                None,
                &manifests,
            );
            let engine = PromqlEngine::new(Arc::new(cold), EngineOpts::default());
            // These constants come from the independent three-point ledger, rather
            // than a previous execution of the query engine.
            for tenant_case in [
                TenantLedgerCase {
                    tenant: "tenant-a",
                    total: if stage < 3 { 16.0 } else { 11.0 },
                    delta: 7.0,
                },
                TenantLedgerCase {
                    tenant: "tenant-b",
                    total: 600.0,
                    delta: 200.0,
                },
            ] {
                for query_case in [
                    ExpectedQueryCase {
                        query: "sum(sum_over_time(requests_total[90s]))",
                        value: tenant_case.total,
                    },
                    ExpectedQueryCase {
                        query: r#"sum(sum_over_time({__name__="requests_total", job="api" or __name__="requests_total"}[90s]))"#,
                        value: tenant_case.total,
                    },
                    ExpectedQueryCase {
                        query: "sum(count_over_time(requests_total[90s]))",
                        value: if tenant_case.tenant == "tenant-a" && stage >= 3 {
                            2.0
                        } else {
                            3.0
                        },
                    },
                    ExpectedQueryCase {
                        query: "sum(idelta(requests_total[90s]))",
                        value: if tenant_case.tenant == "tenant-a" {
                            if stage < 3 { 4.0 } else { 7.0 }
                        } else {
                            100.0
                        },
                    },
                    ExpectedQueryCase {
                        query: "sum(max_over_time(requests_total[90s])-min_over_time(requests_total[90s]))",
                        value: tenant_case.delta,
                    },
                ] {
                    let actual = engine
                        .query_instant(&tenant_id(tenant_case.tenant), query_case.query, 90_000)
                        .await
                        .unwrap();
                    assert2::assert!(
                        actual
                            == QueryResult::InstantVector(vec![InstantSample {
                                labels: Labels::new().into(),
                                ts_ms: 90_000,
                                value: SampleValue::Float(query_case.value),
                                drop_name: false
                            }]),
                        "stage={stage} tenant={} query={}",
                        tenant_case.tenant,
                        query_case.query
                    );
                }
            }
        }
    }
}

mod instant_offset_and_at_selectors_read_the_right_blocks {
    use super::*;

    struct SelectorExpectation {
        query: &'static str,
        labels: Labels,
        value: f64,
    }

    #[tokio::test]
    pub(crate) async fn instant_offset_and_at_selectors_read_the_right_blocks() {
        // One block per minute from 0s to 600s, and each sample's value is its
        // timestamp in seconds.
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let mut block_store = BlockStore::new(object_store, base);
        let series = labels(&[
            crate::test_support::TestLabelPair::new("__name__", "up"),
            crate::test_support::TestLabelPair::new("job", "api"),
        ]);
        for minute in 0..=10_i64 {
            let ts_ms = minute * 60_000;
            let value = f64::from(u32::try_from(minute * 60).unwrap());
            write_float_block(
                &mut block_store,
                &format!("metrics/float/{minute:04}.parquet"),
                &series,
                ts_ms,
                value,
            )
            .await;
        }
        let engine = PromqlEngine::new(
            Arc::new(MetricBlockStore::new(block_store)),
            EngineOpts::default(),
        );
        let job = labels(&[crate::test_support::TestLabelPair::new("job", "api")]);

        // The same matcher set appears with different windows in one query, so
        // the per-query scan cache must not answer one window with another.
        for case in [
            SelectorExpectation {
                query: "up",
                labels: series.clone(),
                value: 600.0,
            },
            SelectorExpectation {
                query: "up offset 5m",
                labels: series.clone(),
                value: 300.0,
            },
            SelectorExpectation {
                query: "up @ 120",
                labels: series.clone(),
                value: 120.0,
            },
            SelectorExpectation {
                query: "up - up offset 5m",
                labels: job.clone(),
                value: 300.0,
            },
            SelectorExpectation {
                query: "up @ 120 + up offset 5m",
                labels: job.clone(),
                value: 420.0,
            },
            SelectorExpectation {
                query: "count_over_time(up[2m])",
                labels: job.clone(),
                value: 2.0,
            },
            SelectorExpectation {
                query: "count_over_time(up[2m] offset 5m)",
                labels: job.clone(),
                value: 2.0,
            },
            SelectorExpectation {
                query: "sum_over_time(up[2m] @ 120)",
                labels: job.clone(),
                value: 180.0,
            },
            SelectorExpectation {
                query: "sum_over_time(up[2m]) - sum_over_time(up[2m] offset 5m)",
                labels: job.clone(),
                value: 600.0,
            },
        ] {
            let result = engine
                .query_instant(&tenant_id("tenant-a"), case.query, 600_000)
                .await
                .unwrap();
            let QueryResult::InstantVector(samples) = result else {
                panic!("{}: expected an instant vector", case.query);
            };
            assert2::assert!(samples.len() == 1, "{}", case.query);
            let sample = &samples[0];
            check!(
                sample.labels == crate::PromqlLabels::from(case.labels)
                    && sample.ts_ms == 600_000
                    && sample.value == SampleValue::Float(case.value),
                "{}",
                case.query
            );
        }
    }
}
