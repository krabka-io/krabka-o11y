//! Role-selectable metrics service for the Prometheus/Mimir-compatible backend.

// Proving the async service futures `Send` traverses DataFusion's deep
// `sqlparser` AST type graph (reached through `SessionContext` held across
// awaits in the PromQL operator-path evaluation); the default limit is too low.
#![recursion_limit = "256"]

use std::{
    collections::{BTreeMap, BTreeSet},
    net::SocketAddr,
    path::{Path as StdPath, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

mod ids;

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use bytes::Bytes;
use futures::TryStreamExt;
pub use ids::{Offset, PartitionIndex};
use krabka_blockstore::{BlockStore, LabelMatcher, Labels};
use krabka_client_consumer::{Consumer, ConsumerRecord};
use krabka_client_producer::{Producer, ProducerRecord};
use krabka_metrics::{CompactionIndexManifest, WalRecord, partition_key};
use krabka_promql::{
    AlertmanagerSink, EngineOpts, ExemplarRecord, InMemoryMetricStore, LabelNameCardinality,
    LabelValueCardinality, MergedMetricStore, MetadataRecord, MetricBlockStore, MetricStore,
    PrometheusApiState, QueryFrontendOptions, RecordingRuleWalSink, RulerAlertState,
    RulerAlertStateRecord, RulerGroupEvaluation, RulerGroupState, RulerGroupStateRecord,
    RulerShard, RulerStateSink, RulerWalError, ScanResult, TsdbBlock, WalHead,
    evaluate_and_persist_ruler_rule_set_for_shard_due_for_eval, prometheus_router,
};
use krabka_units::prelude::*;
use object_store::{ObjectStore, ObjectStoreExt, path::Path};
use tokio::{net::TcpListener, task::JoinHandle};
use tower::ServiceExt as _;
use url::Url;

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    /// Manifest listing has three rules with no test between them: the range
    /// filter is an *overlap* test, so a manifest ending exactly when the query
    /// starts still counts; the extension match is deliberately case
    /// insensitive; and a manifest deleted from the store has to leave the
    /// cache rather than be served from it forever.
    #[tokio::test]
    async fn manifest_listing_covers_its_boundaries() {
        use krabka_metrics::{CompactionIndexManifest, MetricBlockKind};
        use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};

        fn manifest(index_key: &str, min_ts: i64, max_ts: i64) -> CompactionIndexManifest {
            CompactionIndexManifest {
                tenant: "tenant-a".to_string(),
                kind: MetricBlockKind::Float,
                block_key: format!("{index_key}.parquet"),
                index_key: index_key.to_string(),
                first_offset: 0,
                last_offset: 0,
                row_count: 1,
                min_ts,
                max_ts,
                fingerprints: vec![1],
                series: Vec::new(),
            }
        }

        let store: Arc<dyn ObjectStore> = Arc::new(object_store::memory::InMemory::new());
        for entry in [
            manifest("m/a.index", 0, 100),
            manifest("m/b.INDEX", 300, 400),
        ] {
            store
                .put(
                    &Path::from(entry.index_key.clone()),
                    PutPayload::from(entry.encode().unwrap()),
                )
                .await
                .unwrap();
        }
        // Not an index sidecar, so it must never be decoded.
        store
            .put(&Path::from("m/notes.txt"), PutPayload::from("ignore me"))
            .await
            .unwrap();

        // Case-insensitive extension: both sidecars are found, the .txt is not.
        let all = super::load_compaction_manifests(store.clone(), "m")
            .await
            .unwrap();
        assert2::assert!(all.len() == 2, "an uppercase .INDEX is still an index");

        // Overlap, not containment: `a` ends exactly when the range starts.
        let touching = super::load_compaction_manifests_for_range(store.clone(), "m", 100, 200)
            .await
            .unwrap();
        assert2::assert!(
            touching
                .iter()
                .map(|m| m.index_key.as_str())
                .collect::<Vec<_>>()
                == vec!["m/a.index"],
            "a manifest ending at the range start overlaps it"
        );

        // The other end of the same overlap test: `b` begins exactly when the
        // range stops, so it overlaps too.
        let both = super::load_compaction_manifests_for_range(store.clone(), "m", 100, 300)
            .await
            .unwrap();
        assert2::assert!(
            both.iter()
                .map(|m| m.index_key.as_str())
                .collect::<Vec<_>>()
                == vec!["m/a.index", "m/b.INDEX"],
            "a manifest starting at the range end overlaps it"
        );

        // A manifest that leaves the store leaves the cache with it.
        let cache = tokio::sync::RwLock::new(std::collections::BTreeMap::new());
        super::load_compaction_manifests_filtered_with_cache(
            store.clone(),
            "m",
            None,
            Some(&cache),
        )
        .await
        .unwrap();
        assert2::assert!(cache.read().await.len() == 2);

        store.delete(&Path::from("m/a.index")).await.unwrap();
        super::load_compaction_manifests_filtered_with_cache(
            store.clone(),
            "m",
            None,
            Some(&cache),
        )
        .await
        .unwrap();
        assert2::assert!(
            cache.read().await.keys().cloned().collect::<Vec<_>>() == vec!["m/b.INDEX".to_string()],
            "the deleted manifest is evicted"
        );
    }

    /// The compaction key decides which records replace one another on a
    /// compacted topic. Two entities must never share a key, and one entity
    /// must always produce the same key, so the parts are checked for being
    /// present, ordered, and separated.
    #[test]
    fn ruler_state_compaction_keys_identify_one_entity_each() {
        let group = |tenant: &str, namespace: &str, name: &str| {
            super::ruler_state_compaction_key(&super::RulerStateWalRecord::Group(
                super::RulerGroupStateRecord {
                    tenant: tenant.into(),
                    namespace: namespace.into(),
                    group: name.into(),
                    // Not part of the key: the record replaces its predecessor.
                    last_eval_ms: 1,
                },
            ))
        };

        check!(group("t", "ns", "g") == Bytes::from("group\0t\0ns\0g"));
        check!(
            group("t", "ns", "g") == group("t", "ns", "g"),
            "the same entity keys alike"
        );
        check!(
            group("t", "ns", "g") != group("t", "ns", "h"),
            "a different group differs"
        );
        check!(
            group("t", "ns", "g") != group("u", "ns", "g"),
            "so does a different tenant"
        );
        check!(
            group("t", "ns", "g") != group("t", "n", "sg"),
            "the separator stops a shifted split from colliding"
        );

        let alert = |rule: &str, labels: &[(&str, &str)]| {
            super::ruler_state_compaction_key(&super::RulerStateWalRecord::Alert(
                super::RulerAlertStateRecord {
                    tenant: "t".into(),
                    rule_id: rule.into(),
                    labels: labels
                        .iter()
                        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                        .collect(),
                    active_since_ms: Some(1),
                    keep_firing_until_ms: None,
                },
            ))
        };

        check!(alert("r", &[]) == Bytes::from("alert\0t\0r"));
        check!(alert("r", &[("a", "1")]) == Bytes::from("alert\0t\0r\0a=1"));
        check!(
            alert("r", &[("a", "1"), ("b", "2")]) == Bytes::from("alert\0t\0r\0a=1\0b=2"),
            "every label is part of the identity"
        );
        check!(
            alert("r", &[("a", "1")]) != alert("r", &[("a", "2")]),
            "an alert with different label values is a different alert"
        );

        // A group key and an alert key can never collide, whatever they hold.
        check!(group("t", "ns", "g") != alert("t\0ns\0g", &[]));
    }

    /// The producer record both ruler sinks build, checked without a broker.
    #[test]
    fn a_ruler_record_carries_its_topic_key_and_value() {
        let record = super::keyed_producer_record(
            "ruler-state".to_string(),
            Bytes::from_static(b"the-key"),
            b"the-value".to_vec(),
        );

        check!(record.topic == "ruler-state");
        check!(
            record.partition == None,
            "partitioning is left to the producer"
        );
        check!(record.key.as_deref() == Some(&b"the-key"[..]));
        check!(record.value.as_deref() == Some(&b"the-value"[..]));
    }

    use super::{current_time_ms, duration_ms, unix_time_ms};

    /// The Noop sink's only job is to say so. It drops every alert, and the
    /// warning is the entire behaviour -- an operator whose alertmanager is
    /// unconfigured learns it from this line or not at all. A body replaced by
    /// `Ok(())` is silent, and the negation dropped warns on the empty batch
    /// and stays silent on the one that had alerts in it.
    #[test]
    fn the_noop_alertmanager_sink_warns_only_when_it_drops_something() {
        use std::sync::{Arc, Mutex};

        use krabka_promql::AlertmanagerSink as _;
        use tracing::subscriber::with_default;
        use tracing_subscriber::{fmt::MakeWriter, layer::SubscriberExt as _, registry};

        #[derive(Clone, Default)]
        struct Captured(Arc<Mutex<Vec<u8>>>);

        impl std::io::Write for Captured {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> MakeWriter<'a> for Captured {
            type Writer = Self;
            fn make_writer(&'a self) -> Self::Writer {
                self.clone()
            }
        }

        fn dispatch(alerts: Vec<krabka_promql::AlertmanagerAlert>) -> String {
            let captured = Captured::default();
            let subscriber = registry().with(
                tracing_subscriber::fmt::layer()
                    .with_writer(captured.clone())
                    .with_ansi(false)
                    .without_time(),
            );
            // `with_default` sets the subscriber for this thread and is sync, so
            // the runtime is built inside it rather than around it -- a
            // `#[tokio::test]` would already be driving one and `block_on`
            // cannot nest.
            let runtime = tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("current-thread runtime");
            with_default(subscriber, || {
                runtime.block_on(async {
                    super::NoopAlertmanagerSink
                        .dispatch_alerts(alerts)
                        .await
                        .expect("the noop sink always succeeds");
                });
            });
            String::from_utf8(captured.0.lock().unwrap().clone()).unwrap()
        }

        let alert = krabka_promql::AlertmanagerAlert {
            labels: std::collections::BTreeMap::from([("alertname".into(), "Down".into())]),
            annotations: std::collections::BTreeMap::new(),
            starts_at_ms: 1,
            ends_at_ms: None,
            generator_url: String::new(),
        };
        let with_alerts = dispatch(vec![alert]);
        check!(
            with_alerts.contains("alertmanager sink is not configured"),
            "captured: {with_alerts:?}"
        );
        check!(
            with_alerts.contains("alert_count=1"),
            "captured: {with_alerts:?}"
        );

        check!(dispatch(Vec::new()).is_empty());
    }

    /// Both clocks convert a duration to milliseconds and saturate rather than
    /// wrap. A constant in either place makes every block-store refresh window
    /// and every ruler timestamp agree on a time that never happened.
    #[test]
    fn millisecond_clocks_convert_and_saturate() {
        check!(duration_ms(Duration::from_millis(1_500)) == 1_500);
        check!(duration_ms(Duration::ZERO) == 0);
        check!(duration_ms(Duration::from_secs(u64::MAX)) == i64::MAX);

        // Later than 2020-01-01, and not a sentinel.
        let now = current_time_ms();
        check!(now > 1_577_836_800_000);
        check!(unix_time_ms() > 1_577_836_800_000);
    }

    use assert2::check;
    use axum::{
        body::{Body, to_bytes},
        http::{Request, StatusCode},
    };
    use bytes::Bytes;
    use futures::{StreamExt, stream::BoxStream};
    use krabka_client_consumer::ConsumerRecord;
    use krabka_promql::{AlertmanagerSink, MetricStore};
    use krabka_units::prelude::*;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
        PutMultipartOptions, PutOptions, PutPayload, PutResult, memory::InMemory, path::Path,
    };
    use tower::ServiceExt;

    struct RecordingWalHeadConsumer {
        batches: Vec<Vec<ConsumerRecord>>,
        commit_calls: usize,
    }

    #[async_trait::async_trait]
    impl super::WalHeadConsumerPoll for RecordingWalHeadConsumer {
        async fn poll(
            &mut self,
            _timeout: Time,
        ) -> Result<Vec<ConsumerRecord>, super::WalHeadConsumerError> {
            Ok(self.batches.remove(0))
        }
    }

    #[async_trait::async_trait]
    impl super::WalHeadConsumerCommit for RecordingWalHeadConsumer {
        async fn commit_sync(&mut self) -> Result<(), super::WalHeadConsumerError> {
            self.commit_calls += 1;
            Ok(())
        }
    }

    fn consumer_record(
        topic: &str,
        partition: i32,
        offset: i64,
        value: Option<Vec<u8>>,
    ) -> ConsumerRecord {
        ConsumerRecord {
            topic: topic.to_string(),
            partition,
            offset,
            leader_epoch: -1,
            timestamp: 0,
            key: None,
            value: value.map(Bytes::from),
            headers: Vec::new(),
        }
    }

    struct CountingObjectStore {
        inner: Arc<InMemory>,
        list_calls: Arc<AtomicUsize>,
        get_calls: Arc<AtomicUsize>,
        list_delay: Time,
    }

    impl CountingObjectStore {
        fn new(list_calls: Arc<AtomicUsize>, list_delay: Time) -> Self {
            Self {
                inner: Arc::new(InMemory::new()),
                list_calls,
                get_calls: Arc::new(AtomicUsize::new(0)),
                list_delay,
            }
        }
    }

    impl std::fmt::Debug for CountingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CountingObjectStore")
        }
    }

    impl std::fmt::Display for CountingObjectStore {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("CountingObjectStore")
        }
    }

    #[async_trait::async_trait]
    impl ObjectStore for CountingObjectStore {
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
            if std::path::Path::new(location.as_ref())
                .extension()
                .is_some_and(|ext| ext.eq_ignore_ascii_case("index"))
            {
                self.get_calls.fetch_add(1, Ordering::SeqCst);
            }
            self.inner.get_opts(location, options).await
        }

        fn delete_stream(
            &self,
            locations: BoxStream<'static, object_store::Result<Path>>,
        ) -> BoxStream<'static, object_store::Result<Path>> {
            self.inner.delete_stream(locations)
        }

        fn list(
            &self,
            prefix: Option<&Path>,
        ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
            self.list_calls.fetch_add(1, Ordering::SeqCst);
            let delay = self.list_delay.to_std();
            Box::pin(self.inner.list(prefix).then(move |item| async move {
                tokio::time::sleep(delay).await;
                item
            }))
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

    #[tokio::test]
    async fn in_memory_router_serves_prometheus_query_api() {
        let response = super::in_memory_prometheus_router()
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=vector(1)&time=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let status_is_success = response.status().is_success();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(status_is_success);
        assert2::assert!(body["status"].as_str() == Some("success"));
        assert2::assert!(body["data"]["resultType"].as_str() == Some("vector"));
    }

    #[tokio::test]
    async fn in_memory_router_serves_mimir_prefixed_query_api() {
        let response = super::in_memory_prometheus_router()
            .oneshot(
                Request::builder()
                    .uri("/prometheus/api/v1/query?query=vector(1)&time=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let status_is_success = response.status().is_success();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(status_is_success);
        assert2::assert!(body["status"].as_str() == Some("success"));
        assert2::assert!(body["data"]["resultType"].as_str() == Some("vector"));
    }

    #[tokio::test]
    async fn router_for_store_serves_samples_from_supplied_store() {
        let mut store = krabka_promql::InMemoryMetricStore::new();
        let mut labels = krabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        store.push_float("tenant-a", labels, 10_000, 1.0);

        let response = super::prometheus_router_for_store(store)
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=up&time=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        let status_is_success = response.status().is_success();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(status_is_success);
        assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
        assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("1"));
    }

    /// The `MetricStore` impl on the refreshing store is delegation: resolve
    /// the store covering the range, then forward. Nothing drove it, so a
    /// reader replaced by an empty vec described a tenant with no series at
    /// all -- which is exactly what an empty store looks like, so an assertion
    /// against one proves nothing. The head is seeded first, and each reader
    /// asserted against what was seeded.
    #[tokio::test]
    async fn refreshing_store_readers_forward_to_the_covering_store() {
        use krabka_metrics::{SamplePayload, WalRecord};

        let head = krabka_promql::WalHead::new();
        head.apply_wal_record(&WalRecord {
            tenant: "acme".into(),
            labels: vec![
                ("__name__".into(), "http_requests_total".into()),
                ("route".into(), "/orders".into()),
            ],
            payload: SamplePayload::Float {
                timestamp_ms: 1_000,
                value: 7.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        });

        let store = super::RefreshingMetricBlockStore::new(
            Arc::new(InMemory::new()) as Arc<dyn ObjectStore>,
            url::Url::parse("memory:///").unwrap(),
            "metrics",
            head,
        );

        let names = store.label_names("acme", &[], 0, 10_000).await.unwrap();
        check!(names.contains(&"__name__".to_string()), "names: {names:?}");
        check!(names.contains(&"route".to_string()), "names: {names:?}");

        let routes = store
            .label_values("acme", "route", &[], 0, 10_000)
            .await
            .unwrap();
        check!(routes == vec!["/orders".to_string()], "routes: {routes:?}");

        let active = store.cardinality_active_series("acme").await.unwrap();
        check!(!active.is_empty(), "active series: {active:?}");

        // The remaining readers delegate the same way. Each is asserted
        // non-empty against the seeded series, which is what a reader replaced
        // by an empty vec cannot produce.
        let card_names = store.cardinality_label_names("acme").await.unwrap();
        check!(!card_names.is_empty(), "cardinality names: {card_names:?}");

        let card_values = store.cardinality_label_values("acme").await.unwrap();
        check!(
            !card_values.is_empty(),
            "cardinality values: {card_values:?}"
        );

        let blocks = store.tsdb_blocks("acme").await;
        check!(blocks.is_ok(), "tsdb blocks: {blocks:?}");
    }

    #[test]
    fn refreshing_blockstore_policy_defaults_and_overrides() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let defaults = super::RefreshingMetricBlockStore::new(
            Arc::clone(&object_store),
            url::Url::parse("memory:///").unwrap(),
            "metrics",
            krabka_promql::WalHead::new(),
        );
        check!(defaults.cold_cache_ttl == super::DEFAULT_COLD_CACHE_TTL);
        check!(
            defaults.unbounded_compatibility_lookback
                == super::DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK
        );

        let configured = super::RefreshingMetricBlockStore::new(
            object_store,
            url::Url::parse("memory:///").unwrap(),
            "metrics",
            krabka_promql::WalHead::new(),
        )
        .with_cold_cache_ttl(secs(5))
        .with_unbounded_compatibility_lookback(minutes(10));
        check!(configured.cold_cache_ttl == secs(5));
        check!(configured.unbounded_compatibility_lookback == minutes(10));
    }

    #[test]
    fn configured_lookback_normalizes_only_unbounded_range() {
        check!(
            super::normalize_refresh_range(i64::MIN, i64::MAX, minutes(10), 1_000_000)
                == (400_000, i64::MAX)
        );
        check!(super::normalize_refresh_range(100, 200, minutes(10), 1_000_000) == (100, 200));
        // Bounded on one side only. Read as `||`, either sentinel alone is
        // enough to rewrite the caller's range to the lookback window.
        check!(
            super::normalize_refresh_range(i64::MIN, 5_000, minutes(10), 1_000_000)
                == (i64::MIN, 5_000)
        );
        check!(
            super::normalize_refresh_range(100, i64::MAX, minutes(10), 1_000_000)
                == (100, i64::MAX)
        );
    }

    #[test]
    fn configured_cold_cache_ttl_controls_freshness() {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let cold = krabka_promql::MetricBlockStore::new(krabka_blockstore::BlockStore::new(
            object_store,
            url::Url::parse("memory:///").unwrap(),
        ));
        let cached = super::CachedMetricBlockStore {
            cached_at: std::time::Instant::now()
                .checked_sub(std::time::Duration::from_secs(2))
                .expect("two seconds before now is representable"),
            start_ms: 0,
            end_ms: 100,
            cold,
        };
        check!(cached.covers(0, 100, secs(3)));
        check!(!cached.covers(0, 100, secs(1)));
    }

    #[tokio::test]
    async fn query_frontend_router_serves_split_range_query() {
        let mut store = krabka_promql::InMemoryMetricStore::new();
        let mut labels = krabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        for (ts_ms, value) in [(0, 1.0), (60_000, 2.0), (120_000, 3.0)] {
            store.push_float("tenant-a", labels.clone(), ts_ms, value);
        }

        let response = super::query_frontend_prometheus_router_for_store(
            store,
            krabka_promql::QueryFrontendOptions {
                split_interval: minutes(1),
                shard_count: 1,
            },
        )
        .oneshot(
            Request::builder()
                .uri("/api/v1/query_range?query=up&start=0&end=120&step=60")
                .header("x-scope-orgid", "tenant-a")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

        let status_is_success = response.status().is_success();
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(status_is_success);
        assert2::assert!(body["data"]["resultType"].as_str() == Some("matrix"));
        assert2::assert!(
            body["data"]["result"][0]["values"]
                .as_array()
                .unwrap()
                .len()
                == 3
        );
    }

    #[derive(Default)]
    struct RecordingRulerWalSink {
        records: std::sync::Mutex<Vec<krabka_metrics::WalRecord>>,
    }

    impl RecordingRulerWalSink {
        fn records(&self) -> Vec<krabka_metrics::WalRecord> {
            self.records
                .lock()
                .expect("recording ruler sink poisoned")
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl krabka_promql::RecordingRuleWalSink for RecordingRulerWalSink {
        async fn append_recording_rule_record(
            &self,
            record: krabka_metrics::WalRecord,
        ) -> Result<(), krabka_promql::RulerWalError> {
            self.records
                .lock()
                .expect("recording ruler sink poisoned")
                .push(record);
            Ok(())
        }
    }

    /// Shared by handle so the fanout can own one and the test can still read
    /// it: the fanout takes its sinks by value, and the trait is implemented for
    /// the recorder itself rather than for `Arc<_>`.
    #[derive(Clone, Default)]
    struct RecordingRulerStateSink {
        groups: std::sync::Arc<std::sync::Mutex<Vec<krabka_promql::RulerGroupStateRecord>>>,
        alerts: std::sync::Arc<std::sync::Mutex<Vec<krabka_promql::RulerAlertStateRecord>>>,
    }

    #[async_trait::async_trait]
    impl krabka_promql::RulerStateSink for RecordingRulerStateSink {
        async fn persist_ruler_group_state(
            &self,
            record: krabka_promql::RulerGroupStateRecord,
        ) -> Result<(), krabka_promql::RulerWalError> {
            self.groups.lock().expect("poisoned").push(record);
            Ok(())
        }

        async fn persist_ruler_alert_state(
            &self,
            record: krabka_promql::RulerAlertStateRecord,
        ) -> Result<(), krabka_promql::RulerWalError> {
            self.alerts.lock().expect("poisoned").push(record);
            Ok(())
        }
    }

    /// The fanout sink exists so ruler state reaches two destinations at once,
    /// and nothing asserted that it reaches either. Both methods replaced by
    /// `Ok(())` report success while writing to neither -- which is exactly
    /// what losing ruler state looks like from the caller's side.
    #[tokio::test]
    async fn ruler_state_fanout_reaches_both_sinks() {
        use krabka_promql::RulerStateSink as _;

        let first = RecordingRulerStateSink::default();
        let second = RecordingRulerStateSink::default();
        let fanout = super::RulerStateFanoutSink::new(first.clone(), second.clone());

        let group = krabka_promql::RulerGroupStateRecord {
            tenant: "acme".into(),
            namespace: "ns".into(),
            group: "g".into(),
            last_eval_ms: 1_234,
        };
        let alert = krabka_promql::RulerAlertStateRecord {
            tenant: "acme".into(),
            rule_id: "r1".into(),
            labels: std::collections::BTreeMap::new(),
            active_since_ms: Some(9),
            keep_firing_until_ms: None,
        };

        fanout
            .persist_ruler_group_state(group.clone())
            .await
            .unwrap();
        fanout
            .persist_ruler_alert_state(alert.clone())
            .await
            .unwrap();

        for sink in [&first, &second] {
            check!(*sink.groups.lock().expect("poisoned") == vec![group.clone()]);
            check!(*sink.alerts.lock().expect("poisoned") == vec![alert.clone()]);
        }
    }

    struct RecordingAlertmanagerSink;

    #[async_trait::async_trait]
    impl krabka_promql::AlertmanagerSink for RecordingAlertmanagerSink {
        async fn dispatch_alerts(
            &self,
            _alerts: Vec<krabka_promql::AlertmanagerAlert>,
        ) -> Result<(), krabka_promql::RulerWalError> {
            Ok(())
        }
    }

    #[tokio::test]
    async fn ruler_evaluation_reads_api_rules_and_appends_recording_wal_records() {
        let mut store = krabka_promql::InMemoryMetricStore::new();
        let mut labels = krabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        store.push_float("tenant-a", labels, 10_000, 1.0);
        let state = super::prometheus_api_state_for_store(store);
        let router = krabka_promql::prometheus_router(std::sync::Arc::clone(&state));

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/prometheus/config/v1/rules/team-a")
                    .header("x-scope-orgid", "tenant-a")
                    .header("content-type", "application/yaml")
                    .body(Body::from(
                        r"
name: recording
interval: 1m
rules:
  - record: job:up:sum
    expr: sum by (job) (up)
",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status().is_success());

        let wal_sink = RecordingRulerWalSink::default();
        let alert_sink = RecordingAlertmanagerSink;
        let state_sink = super::PrometheusRulerStateSink::new(std::sync::Arc::clone(&state));
        let mut alert_state = krabka_promql::RulerAlertState::default();
        let mut group_state = krabka_promql::RulerGroupState::default();
        let evaluation = super::evaluate_ruler_once(
            &state,
            (&wal_sink, &alert_sink, &state_sink),
            &mut alert_state,
            &mut group_state,
            "tenant-a",
            krabka_promql::RulerShard::new(1, 1).unwrap(),
            10_000,
        )
        .await
        .unwrap();

        assert2::assert!(evaluation.recording_records == 1);
        let records = wal_sink.records();
        let record_labels = records[0].labels();
        assert2::assert!(records.len() == 1);
        assert2::assert!(records[0].tenant.as_str() == "tenant-a");
        assert2::assert!(record_labels.get("__name__") == Some("job:up:sum"));
        assert2::assert!(record_labels.get("job") == Some("api"));
        assert2::assert!(matches!(
            records[0].payload,
            krabka_metrics::SamplePayload::Float { value, .. } if (value - 1.0).abs() < f64::EPSILON
        ));
    }

    #[tokio::test]
    async fn ruler_evaluation_applies_runtime_max_samples_per_query() {
        let mut store = krabka_promql::InMemoryMetricStore::new();
        let mut api_labels = krabka_blockstore::Labels::new();
        api_labels.insert("__name__", "up");
        api_labels.insert("job", "api");
        store.push_float("tenant-a", api_labels, 10_000, 1.0);
        let mut web_labels = krabka_blockstore::Labels::new();
        web_labels.insert("__name__", "up");
        web_labels.insert("job", "web");
        store.push_float("tenant-a", web_labels, 10_000, 1.0);
        let limits = krabka_metrics::Limits {
            max_samples_per_query: 1,
            ..krabka_metrics::Limits::default()
        };
        let state = std::sync::Arc::new(
            krabka_promql::PrometheusApiState::new(
                std::sync::Arc::new(store),
                krabka_promql::EngineOpts::default(),
            )
            .with_query_limits(krabka_metrics::OverridesProvider::new(limits)),
        );
        let router = krabka_promql::prometheus_router(std::sync::Arc::clone(&state));

        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/prometheus/config/v1/rules/team-a")
                    .header("x-scope-orgid", "tenant-a")
                    .header("content-type", "application/yaml")
                    .body(Body::from(
                        r"
name: recording
interval: 1m
rules:
  - record: job:up:sum
    expr: sum(up)
",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status().is_success());

        let wal_sink = RecordingRulerWalSink::default();
        let alert_sink = RecordingAlertmanagerSink;
        let state_sink = super::PrometheusRulerStateSink::new(std::sync::Arc::clone(&state));
        let mut alert_state = krabka_promql::RulerAlertState::default();
        let mut group_state = krabka_promql::RulerGroupState::default();
        let error = super::evaluate_ruler_once(
            &state,
            (&wal_sink, &alert_sink, &state_sink),
            &mut alert_state,
            &mut group_state,
            "tenant-a",
            krabka_promql::RulerShard::new(1, 1).unwrap(),
            10_000,
        )
        .await
        .unwrap_err();

        assert2::assert!(format!("{error}").contains("query exceeds max_samples=1"));
        assert2::assert!(wal_sink.records().is_empty());
    }

    #[tokio::test]
    async fn alertmanager_http_sink_posts_v2_alert_payloads() {
        let received = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let received_for_route = std::sync::Arc::clone(&received);
        let router = axum::Router::new().route(
            "/api/v2/alerts",
            axum::routing::post(move |body: bytes::Bytes| {
                let received = std::sync::Arc::clone(&received_for_route);
                async move {
                    received
                        .lock()
                        .expect("received alerts poisoned")
                        .push(body.to_vec());
                    axum::http::StatusCode::OK
                }
            }),
        );
        let bound = super::serve_prometheus_router("127.0.0.1:0".parse().unwrap(), router, async {
            std::future::pending::<()>().await;
        })
        .await
        .unwrap();

        let sink = super::AlertmanagerHttpSink::new(format!("http://{bound}/api/v2/alerts"));
        sink.dispatch_alerts(vec![krabka_promql::AlertmanagerAlert {
            labels: std::collections::BTreeMap::from([
                ("alertname".to_string(), "InstanceDown".to_string()),
                ("severity".to_string(), "page".to_string()),
            ]),
            annotations: std::collections::BTreeMap::from([(
                "summary".to_string(),
                "instance is down".to_string(),
            )]),
            starts_at_ms: 60_000,
            ends_at_ms: None,
            generator_url: "http://krabka.example/graph".to_string(),
        }])
        .await
        .unwrap();

        let bodies = received.lock().expect("received alerts poisoned");
        assert2::assert!(bodies.len() == 1);
        let body: serde_json::Value = serde_json::from_slice(&bodies[0]).unwrap();
        let expected = serde_json::json!([{
            "labels": {
                "alertname": "InstanceDown",
                "severity": "page",
            },
            "annotations": {
                "summary": "instance is down",
            },
            "startsAt": "1970-01-01T00:01:00Z",
            "endsAt": null,
            "generatorURL": "http://krabka.example/graph",
        }]);
        assert2::assert!(body == expected);
    }

    #[test]
    fn ruler_state_records_round_trip_with_compacted_keys() {
        let group = krabka_promql::RulerGroupStateRecord {
            tenant: "tenant-a".to_string(),
            namespace: "team-a".to_string(),
            group: "recording".to_string(),
            last_eval_ms: 60_000,
        };
        let group_record = super::RulerStateWalRecord::Group(group.clone());
        let group_encoded = group_record.encode().unwrap();
        assert2::assert!(
            super::RulerStateWalRecord::decode(&group_encoded).unwrap() == group_record
        );
        assert2::assert!(
            super::ruler_state_compaction_key(&group_record)
                == bytes::Bytes::from_static(b"group\0tenant-a\0team-a\0recording")
        );

        let alert = krabka_promql::RulerAlertStateRecord {
            tenant: "tenant-a".to_string(),
            rule_id: "InstanceDown\nup == 0".to_string(),
            labels: std::collections::BTreeMap::from([
                ("alertname".to_string(), "InstanceDown".to_string()),
                ("job".to_string(), "api".to_string()),
            ]),
            active_since_ms: Some(120_000),
            keep_firing_until_ms: Some(180_000),
        };
        let alert_record = super::RulerStateWalRecord::Alert(alert);
        let alert_encoded = alert_record.encode().unwrap();
        assert2::assert!(
            super::RulerStateWalRecord::decode(&alert_encoded).unwrap() == alert_record
        );
        assert2::assert!(
            super::ruler_state_compaction_key(&alert_record)
                == bytes::Bytes::from_static(
                    b"alert\0tenant-a\0InstanceDown\nup == 0\0alertname=InstanceDown\0job=api"
                )
        );
    }

    #[tokio::test]
    async fn replay_ruler_state_records_applies_state_and_reports_commit_offsets() {
        let state =
            super::prometheus_api_state_for_store(krabka_promql::InMemoryMetricStore::new());
        let router = krabka_promql::prometheus_router(std::sync::Arc::clone(&state));
        let response = router
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/prometheus/config/v1/rules/team-a")
                    .header("X-Scope-OrgID", "tenant-a")
                    .header("Content-Type", "application/yaml")
                    .body(Body::from(
                        r"
name: recording
rules:
  - record: job:up:sum
    expr: sum by (job) (up)
",
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status() == StatusCode::ACCEPTED);

        let group_record =
            super::RulerStateWalRecord::Group(krabka_promql::RulerGroupStateRecord {
                tenant: "tenant-a".to_string(),
                namespace: "team-a".to_string(),
                group: "recording".to_string(),
                last_eval_ms: 60_000,
            });
        let alert_record =
            super::RulerStateWalRecord::Alert(krabka_promql::RulerAlertStateRecord {
                tenant: "tenant-a".to_string(),
                rule_id: "InstanceDown\nup == 0".to_string(),
                labels: std::collections::BTreeMap::from([(
                    "alertname".to_string(),
                    "InstanceDown".to_string(),
                )]),
                active_since_ms: Some(120_000),
                keep_firing_until_ms: Some(180_000),
            });
        let records = vec![
            super::WalHeadConsumerRecord {
                topic: "ignored".to_string(),
                partition: super::PartitionIndex(0),
                offset: super::Offset(10),
                value: Some(group_record.encode().unwrap()),
            },
            super::WalHeadConsumerRecord {
                topic: super::RULER_STATE_TOPIC.to_string(),
                partition: super::PartitionIndex(2),
                offset: super::Offset(20),
                value: Some(group_record.encode().unwrap()),
            },
            super::WalHeadConsumerRecord {
                topic: super::RULER_STATE_TOPIC.to_string(),
                partition: super::PartitionIndex(2),
                offset: super::Offset(21),
                value: Some(alert_record.encode().unwrap()),
            },
        ];

        let result =
            super::replay_ruler_state_records(&state, super::RULER_STATE_TOPIC, &records).unwrap();

        let expected = super::WalHeadReplayResult {
            polled_records: 3,
            replayed_records: 2,
            committed_offsets: vec![super::WalHeadPartitionOffset {
                partition: super::PartitionIndex(2),
                offset: super::Offset(22),
            }],
        };
        assert2::assert!(result == expected);
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/rules")
                    .header("X-Scope-OrgID", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert2::assert!(response.status() == StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(body["data"]["groups"][0]["lastEvaluation"] == "1970-01-01T00:01:00Z");
    }

    /// The consumer loop polls until told to stop, accumulating what each
    /// poll saw. Every field of the running total has to advance on every
    /// poll, and the committed offsets have to accumulate rather than be
    /// replaced -- a caller reading the summary to decide when it has caught
    /// up would otherwise stop early or never.
    #[tokio::test]
    async fn the_consumer_loop_accumulates_every_polls_result() {
        let state =
            super::prometheus_api_state_for_store(krabka_promql::InMemoryMetricStore::new());
        let record = |group: &str| {
            super::RulerStateWalRecord::Group(krabka_promql::RulerGroupStateRecord {
                tenant: "tenant-a".to_string(),
                namespace: "team-a".to_string(),
                group: group.to_string(),
                last_eval_ms: 60_000,
            })
            .encode()
            .unwrap()
        };

        // Three polls: two records, then one, then none. The batches differ
        // so a loop that reused one poll's result would not add up.
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![
                vec![
                    consumer_record(super::RULER_STATE_TOPIC, 0, 1, Some(record("a"))),
                    consumer_record(super::RULER_STATE_TOPIC, 1, 5, Some(record("b"))),
                ],
                vec![consumer_record(
                    super::RULER_STATE_TOPIC,
                    2,
                    9,
                    Some(record("c")),
                )],
                vec![],
            ],
            commit_calls: 0,
        };

        let summary = super::run_ruler_state_consumer_loop(
            &mut consumer,
            &state,
            super::RULER_STATE_TOPIC,
            millis(1),
            |summary| summary.polls >= 3,
        )
        .await
        .unwrap();

        assert2::assert!(
            summary.polls == 3,
            "one count per poll, including the empty one"
        );
        assert2::assert!(summary.polled_records == 3, "2 + 1 + 0");
        assert2::assert!(summary.replayed_records == 3);
        assert2::assert!(
            summary.committed_offsets
                == vec![
                    super::WalHeadPartitionOffset {
                        partition: super::PartitionIndex(0),
                        offset: super::Offset(2),
                    },
                    super::WalHeadPartitionOffset {
                        partition: super::PartitionIndex(1),
                        offset: super::Offset(6),
                    },
                    super::WalHeadPartitionOffset {
                        partition: super::PartitionIndex(2),
                        offset: super::Offset(10),
                    },
                ],
            "offsets from every poll, in order, got {:?}",
            summary.committed_offsets
        );

        // The empty third poll replayed nothing, so it committed nothing.
        assert2::assert!(consumer.commit_calls == 2);
    }

    /// The stop predicate is consulted after each poll, so a loop told to
    /// stop immediately still does exactly one poll's worth of work.
    #[tokio::test]
    async fn the_consumer_loop_stops_after_the_poll_that_satisfies_it() {
        let state =
            super::prometheus_api_state_for_store(krabka_promql::InMemoryMetricStore::new());
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![vec![], vec![]],
            commit_calls: 0,
        };

        let summary = super::run_ruler_state_consumer_loop(
            &mut consumer,
            &state,
            super::RULER_STATE_TOPIC,
            millis(1),
            |_| true,
        )
        .await
        .unwrap();

        assert2::assert!(summary.polls == 1, "stopping at once still polls once");
        assert2::assert!(
            consumer.batches.len() == 1,
            "and consumes exactly one batch"
        );
    }

    /// A poll that replays nothing must not commit. Committing on an empty
    /// batch would advance the group past records it never applied, and the
    /// state they carry would be lost on the next restart.
    #[tokio::test]
    async fn poll_ruler_state_consumer_once_does_not_commit_without_progress() {
        let state =
            super::prometheus_api_state_for_store(krabka_promql::InMemoryMetricStore::new());

        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![vec![]],
            commit_calls: 0,
        };
        let result = super::poll_ruler_state_consumer_once(
            &mut consumer,
            &state,
            super::RULER_STATE_TOPIC,
            millis(1),
        )
        .await
        .unwrap();

        assert2::assert!(
            result
                == super::WalHeadReplayResult {
                    polled_records: 0,
                    replayed_records: 0,
                    committed_offsets: vec![],
                }
        );
        assert2::assert!(consumer.commit_calls == 0, "an empty poll commits nothing");

        // Polled but not replayed: a record from another topic is counted as
        // seen and applied to nothing. Committing here would advance this
        // group's offsets on the strength of someone else's records.
        let state_record =
            super::RulerStateWalRecord::Group(krabka_promql::RulerGroupStateRecord {
                tenant: "tenant-a".to_string(),
                namespace: "team-a".to_string(),
                group: "recording".to_string(),
                last_eval_ms: 60_000,
            });
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![vec![consumer_record(
                "some-other-topic",
                1,
                7,
                Some(state_record.encode().unwrap()),
            )]],
            commit_calls: 0,
        };
        let result = super::poll_ruler_state_consumer_once(
            &mut consumer,
            &state,
            super::RULER_STATE_TOPIC,
            millis(1),
        )
        .await
        .unwrap();

        assert2::assert!(result.polled_records == 1, "the record was seen");
        assert2::assert!(result.replayed_records == 0, "but it was not ours to apply");
        assert2::assert!(
            consumer.commit_calls == 0,
            "a poll that applies nothing commits nothing"
        );
    }

    /// A record with no value cannot be replayed. It is reported with the
    /// partition and offset it sits at rather than skipped, so the record
    /// that stalled the replay can be found, and nothing is committed past it.
    #[tokio::test]
    async fn a_valueless_ruler_state_record_stops_the_replay_where_it_sits() {
        let state =
            super::prometheus_api_state_for_store(krabka_promql::InMemoryMetricStore::new());
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![vec![consumer_record(super::RULER_STATE_TOPIC, 3, 11, None)]],
            commit_calls: 0,
        };

        let error = super::poll_ruler_state_consumer_once(
            &mut consumer,
            &state,
            super::RULER_STATE_TOPIC,
            millis(1),
        )
        .await
        .unwrap_err();

        let message = error.to_string();
        assert2::assert!(message.contains("11"), "the offset is named: {message}");
        assert2::assert!(message.contains('3'), "so is the partition: {message}");
        assert2::assert!(consumer.commit_calls == 0, "nothing is committed past it");
    }

    #[tokio::test]
    async fn poll_ruler_state_consumer_once_replays_records_and_commits_on_progress() {
        let state =
            super::prometheus_api_state_for_store(krabka_promql::InMemoryMetricStore::new());
        let state_record =
            super::RulerStateWalRecord::Group(krabka_promql::RulerGroupStateRecord {
                tenant: "tenant-a".to_string(),
                namespace: "team-a".to_string(),
                group: "recording".to_string(),
                last_eval_ms: 60_000,
            });
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![vec![consumer_record(
                super::RULER_STATE_TOPIC,
                1,
                7,
                Some(state_record.encode().unwrap()),
            )]],
            commit_calls: 0,
        };

        let result = super::poll_ruler_state_consumer_once(
            &mut consumer,
            &state,
            super::RULER_STATE_TOPIC,
            millis(1),
        )
        .await
        .unwrap();

        let expected = super::WalHeadReplayResult {
            polled_records: 1,
            replayed_records: 1,
            committed_offsets: vec![super::WalHeadPartitionOffset {
                partition: super::PartitionIndex(1),
                offset: super::Offset(8),
            }],
        };
        assert2::assert!(result == expected);
        assert2::assert!(consumer.commit_calls == 1);
    }

    #[tokio::test]
    async fn blockstore_router_loads_compaction_manifests_from_object_store() {
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = krabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let mut labels = krabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let fp = labels.fingerprint();
        let batch = krabka_metrics::encode_float_samples(&[(fp, 10_000, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/0001.parquet",
                krabka_metrics::float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let plan = krabka_metrics::CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/tenant-a/float/0001.index".to_string(),
            first_offset: 0,
            last_offset: 0,
            row_count: block_meta.row_count,
        };
        let manifest = krabka_metrics::CompactionIndexManifest::from_block_meta(
            krabka_metrics::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![krabka_metrics::CompactionSeriesLabels {
                fingerprint: fp,
                labels,
            }],
        );
        krabka_metrics::CompactionIndexSink::write_manifest(
            &krabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone()),
            &manifest,
        )
        .await
        .unwrap();

        let router = super::blockstore_prometheus_router(object_store, base, "metrics/tenant-a")
            .await
            .unwrap();
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=up&time=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(response.status().is_success());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
        assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("1"));
    }

    #[tokio::test]
    async fn blockstore_router_sees_manifests_written_after_startup() {
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let router = super::refreshing_blockstore_prometheus_router(
            object_store.clone(),
            base.clone(),
            "metrics/tenant-a",
        );

        let writer_store = krabka_blockstore::BlockStore::new(object_store.clone(), base);
        let mut labels = krabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let fp = labels.fingerprint();
        let batch = krabka_metrics::encode_float_samples(&[(fp, 10_000, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/0002.parquet",
                krabka_metrics::float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let plan = krabka_metrics::CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/tenant-a/float/0002.index".to_string(),
            first_offset: 1,
            last_offset: 1,
            row_count: block_meta.row_count,
        };
        let manifest = krabka_metrics::CompactionIndexManifest::from_block_meta(
            krabka_metrics::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![krabka_metrics::CompactionSeriesLabels {
                fingerprint: fp,
                labels,
            }],
        );
        krabka_metrics::CompactionIndexSink::write_manifest(
            &krabka_metrics::ObjectStoreCompactionIndexSink::new(object_store),
            &manifest,
        )
        .await
        .unwrap();

        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=up&time=10")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(response.status().is_success());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
        assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("1"));
    }

    #[tokio::test]
    async fn refreshing_blockstore_singleflights_concurrent_cold_cache_loads() {
        let list_calls = Arc::new(AtomicUsize::new(0));
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(
            CountingObjectStore::new(Arc::clone(&list_calls), millis(25)),
        );
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = krabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let mut labels = krabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let fp = labels.fingerprint();
        let batch = krabka_metrics::encode_float_samples(&[(fp, 10_000, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/0005.parquet",
                krabka_metrics::float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let plan = krabka_metrics::CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/tenant-a/float/0005.index".to_string(),
            first_offset: 4,
            last_offset: 4,
            row_count: block_meta.row_count,
        };
        let manifest = krabka_metrics::CompactionIndexManifest::from_block_meta(
            krabka_metrics::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![krabka_metrics::CompactionSeriesLabels {
                fingerprint: fp,
                labels,
            }],
        );
        krabka_metrics::CompactionIndexSink::write_manifest(
            &krabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone()),
            &manifest,
        )
        .await
        .unwrap();

        let metric_store = super::RefreshingMetricBlockStore::new(
            object_store,
            base,
            "metrics/tenant-a",
            krabka_promql::WalHead::new(),
        );
        let matchers = Vec::<krabka_blockstore::LabelMatcher>::new();

        let (a, b, c, d) = tokio::join!(
            metric_store.series("tenant-a", &matchers, 0, 20_000),
            metric_store.series("tenant-a", &matchers, 0, 20_000),
            metric_store.series("tenant-a", &matchers, 0, 20_000),
            metric_store.series("tenant-a", &matchers, 0, 20_000),
        );

        let cases = [("a", a), ("b", b), ("c", c), ("d", d)];
        for (_name, result) in cases {
            assert2::assert!(result.unwrap().len() == 1);
        }
        assert2::assert!(list_calls.load(Ordering::SeqCst) == 1);
    }

    #[tokio::test]
    async fn refreshing_blockstore_bounds_cold_manifests_to_query_time() {
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = krabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let sink = krabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone());

        let mut old_labels = krabka_blockstore::Labels::new();
        old_labels.insert("__name__", "up");
        old_labels.insert("job", "old");
        let old_fp = old_labels.fingerprint();
        let old_batch = krabka_metrics::encode_float_samples(&[(old_fp, 10_000, 1.0)]).unwrap();
        let old_block = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/old.parquet",
                krabka_metrics::float_sample_schema(),
                &[old_batch],
            )
            .await
            .unwrap();
        let old_plan = krabka_metrics::CompactionObjectPlan {
            block_key: old_block.object_key.clone(),
            index_key: "metrics/tenant-a/float/old.index".to_string(),
            first_offset: 1,
            last_offset: 1,
            row_count: old_block.row_count,
        };
        let old_manifest = krabka_metrics::CompactionIndexManifest::from_block_meta(
            krabka_metrics::MetricBlockKind::Float,
            &old_plan,
            &old_block,
            vec![krabka_metrics::CompactionSeriesLabels {
                fingerprint: old_fp,
                labels: old_labels,
            }],
        );
        krabka_metrics::CompactionIndexSink::write_manifest(&sink, &old_manifest)
            .await
            .unwrap();

        let mut new_labels = krabka_blockstore::Labels::new();
        new_labels.insert("__name__", "up");
        new_labels.insert("job", "new");
        let new_fp = new_labels.fingerprint();
        let new_batch = krabka_metrics::encode_float_samples(&[(new_fp, 1_000_000, 1.0)]).unwrap();
        let new_block = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/new.parquet",
                krabka_metrics::float_sample_schema(),
                &[new_batch],
            )
            .await
            .unwrap();
        let new_plan = krabka_metrics::CompactionObjectPlan {
            block_key: new_block.object_key.clone(),
            index_key: "metrics/tenant-a/float/new.index".to_string(),
            first_offset: 2,
            last_offset: 2,
            row_count: new_block.row_count,
        };
        let new_manifest = krabka_metrics::CompactionIndexManifest::from_block_meta(
            krabka_metrics::MetricBlockKind::Float,
            &new_plan,
            &new_block,
            vec![krabka_metrics::CompactionSeriesLabels {
                fingerprint: new_fp,
                labels: new_labels,
            }],
        );
        krabka_metrics::CompactionIndexSink::write_manifest(&sink, &new_manifest)
            .await
            .unwrap();

        let metric_store = super::RefreshingMetricBlockStore::new(
            object_store,
            base,
            "metrics/tenant-a",
            krabka_promql::WalHead::new(),
        );
        let matchers = [krabka_blockstore::LabelMatcher::new(
            "__name__",
            krabka_blockstore::MatchOp::Eq,
            "up",
        )];

        let recent = metric_store
            .series("tenant-a", &matchers, 990_000, 1_010_000)
            .await
            .unwrap();
        assert2::assert!(recent.len() == 1);
        assert2::assert!(recent[0].get("job") == Some("new"));

        let old = metric_store
            .series("tenant-a", &matchers, 0, 20_000)
            .await
            .unwrap();
        assert2::assert!(old.len() == 1);
        assert2::assert!(old[0].get("job") == Some("old"));
    }

    #[tokio::test]
    async fn refreshing_blockstore_reuses_decoded_manifests_across_cold_refreshes() {
        let list_calls = Arc::new(AtomicUsize::new(0));
        let object_store = Arc::new(CountingObjectStore::new(
            Arc::clone(&list_calls),
            Time::ZERO,
        ));
        let get_calls = Arc::clone(&object_store.get_calls);
        let object_store: std::sync::Arc<dyn ObjectStore> = object_store;
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = krabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let sink = krabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone());

        write_float_manifest(
            &writer_store,
            &sink,
            "tenant-a",
            "old",
            10_000,
            "metrics/tenant-a/float/old.parquet",
            1,
        )
        .await;
        write_float_manifest(
            &writer_store,
            &sink,
            "tenant-a",
            "new",
            1_000_000,
            "metrics/tenant-a/float/new.parquet",
            2,
        )
        .await;

        let metric_store = super::RefreshingMetricBlockStore::new(
            object_store,
            base,
            "metrics/tenant-a",
            krabka_promql::WalHead::new(),
        );
        let matchers = [krabka_blockstore::LabelMatcher::new(
            "__name__",
            krabka_blockstore::MatchOp::Eq,
            "up",
        )];

        let old = metric_store
            .series("tenant-a", &matchers, 0, 20_000)
            .await
            .unwrap();
        let new = metric_store
            .series("tenant-a", &matchers, 990_000, 1_010_000)
            .await
            .unwrap();

        check!(old.len() == 1);
        check!(old[0].get("job").unwrap() == "old");
        check!(new.len() == 1);
        check!(new[0].get("job").unwrap() == "new");
        check!(
            list_calls.load(Ordering::SeqCst) == 2,
            "cold refresh should list for new manifest keys but not re-download known .index objects"
        );
        check!(
            get_calls.load(Ordering::SeqCst) == 2,
            "cold refresh should list for new manifest keys but not re-download known .index objects"
        );
    }

    #[tokio::test]
    async fn refreshing_blockstore_tsdb_stats_ignores_stale_compacted_blocks() {
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = krabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let sink = krabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone());
        let now_ms = super::duration_ms(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock before Unix epoch"),
        );

        write_float_manifest(
            &writer_store,
            &sink,
            "tenant-a",
            "old",
            now_ms - 2 * 60 * 60 * 1_000,
            "metrics/tenant-a/float/stale.parquet",
            1,
        )
        .await;
        write_float_manifest(
            &writer_store,
            &sink,
            "tenant-a",
            "recent",
            now_ms - 60_000,
            "metrics/tenant-a/float/recent.parquet",
            2,
        )
        .await;

        let metric_store = super::RefreshingMetricBlockStore::new(
            object_store,
            base,
            "metrics/tenant-a",
            krabka_promql::WalHead::new(),
        );

        let stats = metric_store.tsdb_stats("tenant-a").await.unwrap();

        let has_recent_series = stats
            .series_count_by_label_value_pair
            .iter()
            .any(|stat| stat.name == "job=recent" && stat.value == 1);
        let has_stale_series = stats
            .series_count_by_label_value_pair
            .iter()
            .any(|stat| stat.name == "job=old");
        check!(stats.head_stats.num_series == 1);
        check!(has_recent_series);
        check!(!has_stale_series);
    }

    #[tokio::test]
    async fn refreshing_router_merges_hot_head_with_compacted_blocks() {
        let object_store: std::sync::Arc<dyn ObjectStore> = std::sync::Arc::new(InMemory::new());
        let base = url::Url::parse("memory:///").unwrap();
        let writer_store = krabka_blockstore::BlockStore::new(object_store.clone(), base.clone());
        let mut labels = krabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", "api");
        let fp = labels.fingerprint();
        let batch = krabka_metrics::encode_float_samples(&[(fp, 10_000, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                "tenant-a",
                "metrics/tenant-a/float/0003.parquet",
                krabka_metrics::float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let plan = krabka_metrics::CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: "metrics/tenant-a/float/0003.index".to_string(),
            first_offset: 2,
            last_offset: 2,
            row_count: block_meta.row_count,
        };
        let manifest = krabka_metrics::CompactionIndexManifest::from_block_meta(
            krabka_metrics::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![krabka_metrics::CompactionSeriesLabels {
                fingerprint: fp,
                labels: labels.clone(),
            }],
        );
        krabka_metrics::CompactionIndexSink::write_manifest(
            &krabka_metrics::ObjectStoreCompactionIndexSink::new(object_store.clone()),
            &manifest,
        )
        .await
        .unwrap();
        let hot_store = krabka_promql::WalHead::new();
        hot_store.apply_wal_record(&krabka_metrics::WalRecord {
            tenant: "tenant-a".to_string(),
            labels: labels
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            payload: krabka_metrics::SamplePayload::Float {
                timestamp_ms: 20_000,
                value: 2.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        });

        let router = super::refreshing_blockstore_prometheus_router_with_hot_store(
            object_store,
            base,
            "metrics/tenant-a",
            hot_store,
        );
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/api/v1/query?query=up&time=20")
                    .header("x-scope-orgid", "tenant-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert2::assert!(response.status().is_success());
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert2::assert!(body["data"]["result"][0]["metric"]["job"].as_str() == Some("api"));
        assert2::assert!(body["data"]["result"][0]["value"][1].as_str() == Some("2"));
    }

    async fn write_float_manifest(
        writer_store: &krabka_blockstore::BlockStore,
        sink: &krabka_metrics::ObjectStoreCompactionIndexSink,
        tenant: &str,
        job: &str,
        ts_ms: i64,
        object_key: &str,
        offset: i64,
    ) {
        let mut labels = krabka_blockstore::Labels::new();
        labels.insert("__name__", "up");
        labels.insert("job", job);
        let fp = labels.fingerprint();
        let batch = krabka_metrics::encode_float_samples(&[(fp, ts_ms, 1.0)]).unwrap();
        let block_meta = writer_store
            .writer()
            .write_block(
                tenant,
                object_key,
                krabka_metrics::float_sample_schema(),
                &[batch],
            )
            .await
            .unwrap();
        let plan = krabka_metrics::CompactionObjectPlan {
            block_key: block_meta.object_key.clone(),
            index_key: format!("{object_key}.index"),
            first_offset: offset,
            last_offset: offset,
            row_count: block_meta.row_count,
        };
        let manifest = krabka_metrics::CompactionIndexManifest::from_block_meta(
            krabka_metrics::MetricBlockKind::Float,
            &plan,
            &block_meta,
            vec![krabka_metrics::CompactionSeriesLabels {
                fingerprint: fp,
                labels,
            }],
        );
        krabka_metrics::CompactionIndexSink::write_manifest(sink, &manifest)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn replay_wal_head_records_decodes_applies_and_reports_commit_offsets() {
        let head = krabka_promql::WalHead::new();
        let record = krabka_metrics::WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![("__name__".to_string(), "up".to_string())],
            payload: krabka_metrics::SamplePayload::Float {
                timestamp_ms: 10_000,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };
        let encoded = record.encode().unwrap();

        let result = super::replay_wal_head_records(
            &head,
            krabka_metrics::WAL_TOPIC,
            &[
                super::WalHeadConsumerRecord {
                    topic: "other".to_string(),
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(3),
                    value: Some(encoded.clone()),
                },
                super::WalHeadConsumerRecord {
                    topic: krabka_metrics::WAL_TOPIC.to_string(),
                    partition: super::PartitionIndex(2),
                    offset: super::Offset(41),
                    value: Some(encoded),
                },
            ],
        )
        .unwrap();

        let expected = super::WalHeadReplayResult {
            polled_records: 2,
            replayed_records: 1,
            committed_offsets: vec![super::WalHeadPartitionOffset {
                partition: super::PartitionIndex(2),
                offset: super::Offset(42),
            }],
        };
        assert2::assert!(result == expected);
        let values = head
            .series("tenant-a", &[], 0, 20_000)
            .await
            .expect("series");
        assert2::assert!(values.len() == 1);
    }

    #[tokio::test]
    async fn replay_wal_head_records_prunes_outside_head_retention() {
        let head = krabka_promql::WalHead::with_retention(secs(1));
        let record = |job: &str, timestamp_ms: i64| krabka_metrics::WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![
                ("__name__".to_string(), "up".to_string()),
                ("job".to_string(), job.to_string()),
            ],
            payload: krabka_metrics::SamplePayload::Float {
                timestamp_ms,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };

        super::replay_wal_head_records(
            &head,
            krabka_metrics::WAL_TOPIC,
            &[
                super::WalHeadConsumerRecord {
                    topic: krabka_metrics::WAL_TOPIC.to_string(),
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(1),
                    value: Some(record("old", 1_000).encode().unwrap()),
                },
                super::WalHeadConsumerRecord {
                    topic: krabka_metrics::WAL_TOPIC.to_string(),
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(2),
                    value: Some(record("new", 10_000).encode().unwrap()),
                },
            ],
        )
        .unwrap();

        let matchers = [krabka_blockstore::LabelMatcher::new(
            "__name__",
            krabka_blockstore::MatchOp::Eq,
            "up",
        )];
        let jobs = head
            .label_values("tenant-a", "job", &matchers, i64::MIN, i64::MAX)
            .await
            .expect("label values");

        assert2::assert!(jobs == vec!["new".to_string()]);
    }

    #[test]
    fn replay_wal_head_records_rejects_missing_wal_values() {
        let head = krabka_promql::WalHead::new();
        let error = super::replay_wal_head_records(
            &head,
            krabka_metrics::WAL_TOPIC,
            &[super::WalHeadConsumerRecord {
                topic: krabka_metrics::WAL_TOPIC.to_string(),
                partition: super::PartitionIndex(1),
                offset: super::Offset(9),
                value: None,
            }],
        )
        .unwrap_err();

        assert2::assert!(matches!(
            error,
            super::WalHeadReplayError::MissingValue {
                partition: super::PartitionIndex(1),
                offset: super::Offset(9)
            }
        ));
    }

    #[tokio::test]
    async fn poll_wal_head_consumer_once_replays_records_and_commits_on_progress() {
        let head = krabka_promql::WalHead::new();
        let record = krabka_metrics::WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![("__name__".to_string(), "up".to_string())],
            payload: krabka_metrics::SamplePayload::Float {
                timestamp_ms: 10_000,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![vec![consumer_record(
                krabka_metrics::WAL_TOPIC,
                0,
                4,
                Some(record.encode().unwrap()),
            )]],
            commit_calls: 0,
        };

        let result = super::poll_wal_head_consumer_once(
            &mut consumer,
            &head,
            krabka_metrics::WAL_TOPIC,
            millis(1),
        )
        .await
        .unwrap();

        let expected = super::WalHeadReplayResult {
            polled_records: 1,
            replayed_records: 1,
            committed_offsets: vec![super::WalHeadPartitionOffset {
                partition: super::PartitionIndex(0),
                offset: super::Offset(5),
            }],
        };
        assert2::assert!(result == expected);
        assert2::assert!(consumer.commit_calls == 1);
    }

    #[tokio::test]
    async fn poll_wal_head_consumer_once_skips_commit_when_no_wal_records_replayed() {
        let head = krabka_promql::WalHead::new();
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![vec![consumer_record("other", 0, 4, Some(vec![1, 2, 3]))]],
            commit_calls: 0,
        };

        let result = super::poll_wal_head_consumer_once(
            &mut consumer,
            &head,
            krabka_metrics::WAL_TOPIC,
            millis(1),
        )
        .await
        .unwrap();

        assert2::assert!(result.replayed_records == 0);
        assert2::assert!(consumer.commit_calls == 0);
    }

    #[tokio::test]
    async fn run_wal_head_consumer_loop_accumulates_until_stop_predicate() {
        let head = krabka_promql::WalHead::new();
        let record = krabka_metrics::WalRecord {
            tenant: "tenant-a".to_string(),
            labels: vec![("__name__".to_string(), "up".to_string())],
            payload: krabka_metrics::SamplePayload::Float {
                timestamp_ms: 10_000,
                value: 1.0,
                start_timestamp_ms: None,
            },
            exemplars: Vec::new(),
        };
        let encoded = record.encode().unwrap();
        let mut consumer = RecordingWalHeadConsumer {
            batches: vec![
                vec![consumer_record(
                    krabka_metrics::WAL_TOPIC,
                    0,
                    4,
                    Some(encoded.clone()),
                )],
                vec![consumer_record(
                    krabka_metrics::WAL_TOPIC,
                    0,
                    5,
                    Some(encoded),
                )],
            ],
            commit_calls: 0,
        };

        let summary = super::run_wal_head_consumer_loop(
            &mut consumer,
            &head,
            krabka_metrics::WAL_TOPIC,
            millis(1),
            |summary| summary.polls == 2,
        )
        .await
        .unwrap();

        let expected = super::WalHeadConsumerLoopSummary {
            polls: 2,
            polled_records: 2,
            replayed_records: 2,
            committed_offsets: vec![
                super::WalHeadPartitionOffset {
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(5),
                },
                super::WalHeadPartitionOffset {
                    partition: super::PartitionIndex(0),
                    offset: super::Offset(6),
                },
            ],
        };
        assert2::assert!(summary == expected);
        assert2::assert!(consumer.commit_calls == 2);
    }

    #[tokio::test]
    async fn in_memory_prometheus_server_binds_to_listen_address() {
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let bound = super::serve_in_memory_prometheus("127.0.0.1:0".parse().unwrap(), async {
            let _ = stop_rx.await;
        })
        .await
        .unwrap();
        let _ = stop_tx.send(());

        assert2::assert!(bound.port() != 0);
    }

    #[tokio::test]
    async fn joinable_server_task_completes_after_shutdown_signal() {
        // The joinable variant hands back the server `JoinHandle` so callers can
        // await graceful drain. Signalling `shutdown` must let the task run to
        // completion (axum's `with_graceful_shutdown` returns), so the join
        // resolves rather than the task living forever detached.
        let (stop_tx, stop_rx) = tokio::sync::oneshot::channel();
        let (bound, server) = super::serve_prometheus_router_joinable(
            "127.0.0.1:0".parse().unwrap(),
            super::in_memory_prometheus_router(),
            async {
                let _ = stop_rx.await;
            },
        )
        .await
        .unwrap();
        assert2::assert!(bound.port() != 0);

        let _ = stop_tx.send(());
        // Bounded so a regression (handle never resolving) fails instead of hanging.
        let joined = tokio::time::timeout(std::time::Duration::from_secs(5), server).await;
        assert2::assert!(matches!(joined, Ok(Ok(()))));
    }
}

mod alertmanager_http_sink;
mod alertmanager_payload;
mod apply_ruler_state_record;
mod blockstore_prometheus_router;
mod bundled_group_label;
mod bundled_rule_file;
mod bundled_rules_error;
mod bundled_rules_namespace;
mod bundled_rules_response_max;
mod cached_metric_block_store;
mod consumer;
mod current_time_ms;
mod default_cold_cache_ttl;
mod default_unbounded_compatibility_lookback;
mod duration_ms;
mod evaluate_ruler_once;
mod in_memory_prometheus_router;
mod install_bundled_rule_groups;
mod kafka_recording_rule_wal_sink;
mod kafka_ruler_state_sink;
mod keyed_producer_record;
mod load_compaction_manifests;
mod load_compaction_manifests_filtered;
mod load_compaction_manifests_filtered_with_cache;
mod load_compaction_manifests_for_range;
mod load_compaction_manifests_for_range_with_cache;
mod metrics_service_error;
mod noop_alertmanager_sink;
mod normalize_refresh_range;
mod poll_ruler_state_consumer_once;
mod poll_wal_head_consumer_once;
mod prometheus_api_state_for_store;
mod prometheus_router_for_store;
mod prometheus_ruler_state_sink;
mod promql_error;
mod query_frontend_prometheus_router_for_store;
mod query_frontend_prometheus_router_for_store_with_cache;
mod refreshing_blockstore_prometheus_router;
mod refreshing_blockstore_prometheus_router_with_hot_store;
mod refreshing_metric_block_store;
mod replay_ruler_state_records;
mod replay_wal_head_records;
mod ruler_alertmanager_sink;
mod ruler_state_compaction_key;
mod ruler_state_consumer_error;
mod ruler_state_fanout_sink;
mod ruler_state_replay_error;
mod ruler_state_topic;
mod ruler_state_wal_record;
mod ruler_state_wal_record_error;
mod run_ruler_evaluation_loop;
mod run_ruler_state_consumer_loop;
mod run_wal_head_consumer_loop;
mod serve_in_memory_prometheus;
mod serve_prometheus_router;
mod serve_prometheus_router_joinable;
mod unix_ms_to_rfc3339;
mod unix_time_ms;
mod wal_head_consumer_commit;
mod wal_head_consumer_error;
mod wal_head_consumer_loop_summary;
mod wal_head_consumer_poll;
mod wal_head_consumer_record;
mod wal_head_partition_offset;
mod wal_head_replay_error;
mod wal_head_replay_result;
mod wal_record_max_timestamp_ms;

pub use alertmanager_http_sink::AlertmanagerHttpSink;
use alertmanager_payload::alertmanager_payload;
pub use apply_ruler_state_record::apply_ruler_state_record;
pub use blockstore_prometheus_router::blockstore_prometheus_router;
use bundled_group_label::bundled_group_label;
use bundled_rule_file::BundledRuleFile;
pub use bundled_rules_error::BundledRulesError;
use bundled_rules_namespace::bundled_rules_namespace;
use bundled_rules_response_max::BUNDLED_RULES_RESPONSE_MAX;
use cached_metric_block_store::CachedMetricBlockStore;
use current_time_ms::current_time_ms;
pub use default_cold_cache_ttl::DEFAULT_COLD_CACHE_TTL;
pub use default_unbounded_compatibility_lookback::DEFAULT_UNBOUNDED_COMPATIBILITY_LOOKBACK;
use duration_ms::duration_ms;
pub use evaluate_ruler_once::evaluate_ruler_once;
pub use in_memory_prometheus_router::in_memory_prometheus_router;
pub use install_bundled_rule_groups::install_bundled_rule_groups;
pub use kafka_recording_rule_wal_sink::KafkaRecordingRuleWalSink;
pub use kafka_ruler_state_sink::KafkaRulerStateSink;
use keyed_producer_record::keyed_producer_record;
pub use load_compaction_manifests::load_compaction_manifests;
use load_compaction_manifests_filtered::load_compaction_manifests_filtered;
use load_compaction_manifests_filtered_with_cache::load_compaction_manifests_filtered_with_cache;
pub use load_compaction_manifests_for_range::load_compaction_manifests_for_range;
use load_compaction_manifests_for_range_with_cache::load_compaction_manifests_for_range_with_cache;
pub use metrics_service_error::MetricsServiceError;
pub use noop_alertmanager_sink::NoopAlertmanagerSink;
use normalize_refresh_range::normalize_refresh_range;
pub use poll_ruler_state_consumer_once::poll_ruler_state_consumer_once;
pub use poll_wal_head_consumer_once::poll_wal_head_consumer_once;
pub use prometheus_api_state_for_store::prometheus_api_state_for_store;
pub use prometheus_router_for_store::prometheus_router_for_store;
pub use prometheus_ruler_state_sink::PrometheusRulerStateSink;
pub use query_frontend_prometheus_router_for_store::query_frontend_prometheus_router_for_store;
pub use query_frontend_prometheus_router_for_store_with_cache::query_frontend_prometheus_router_for_store_with_cache;
pub use refreshing_blockstore_prometheus_router::refreshing_blockstore_prometheus_router;
pub use refreshing_blockstore_prometheus_router_with_hot_store::refreshing_blockstore_prometheus_router_with_hot_store;
pub use refreshing_metric_block_store::RefreshingMetricBlockStore;
pub use replay_ruler_state_records::replay_ruler_state_records;
pub use replay_wal_head_records::replay_wal_head_records;
pub use ruler_alertmanager_sink::RulerAlertmanagerSink;
pub use ruler_state_compaction_key::ruler_state_compaction_key;
pub use ruler_state_consumer_error::RulerStateConsumerError;
pub use ruler_state_fanout_sink::RulerStateFanoutSink;
pub use ruler_state_replay_error::RulerStateReplayError;
pub use ruler_state_topic::RULER_STATE_TOPIC;
pub use ruler_state_wal_record::RulerStateWalRecord;
pub use ruler_state_wal_record_error::RulerStateWalRecordError;
pub use run_ruler_evaluation_loop::run_ruler_evaluation_loop;
pub use run_ruler_state_consumer_loop::run_ruler_state_consumer_loop;
pub use run_wal_head_consumer_loop::run_wal_head_consumer_loop;
pub use serve_in_memory_prometheus::serve_in_memory_prometheus;
pub use serve_prometheus_router::serve_prometheus_router;
pub use serve_prometheus_router_joinable::serve_prometheus_router_joinable;
use unix_ms_to_rfc3339::unix_ms_to_rfc3339;
use unix_time_ms::unix_time_ms;
pub use wal_head_consumer_commit::WalHeadConsumerCommit;
pub use wal_head_consumer_error::WalHeadConsumerError;
pub use wal_head_consumer_loop_summary::WalHeadConsumerLoopSummary;
pub use wal_head_consumer_poll::WalHeadConsumerPoll;
pub use wal_head_consumer_record::WalHeadConsumerRecord;
pub use wal_head_partition_offset::WalHeadPartitionOffset;
pub use wal_head_replay_error::WalHeadReplayError;
pub use wal_head_replay_result::WalHeadReplayResult;
use wal_record_max_timestamp_ms::wal_record_max_timestamp_ms;
