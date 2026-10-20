//! Compare cold refresh work while unrelated tenants and historical blocks grow.
//! `cargo run --manifest-path benches/Cargo.toml --example metrics_manifest_refresh_profile`
//! LIST pages use a 1,000-object page model over the real `ObjectStore` stream.

use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Instant,
};

use assert2::assert;
use futures::{StreamExt, stream::BoxStream};
use krabka_blockstore::{BlockLevel, BlockStore, Labels};
use krabka_metrics::{
    CompactionIndexManifest, CompactionIndexSink, CompactionSeriesLabels, MetricBlockKind,
    ObjectStoreCompactionIndexSink,
};
use krabka_metrics_service::{RefreshingMetricBlockStore, load_compaction_manifests_for_range};
use krabka_promql::{MergedMetricStore, MetricBlockStore, MetricStore, WalHead};
use krabka_units::Time;
use object_store::{
    GetOptions, GetResult, ObjectMeta, ObjectStore, ObjectStoreExt, memory::InMemory, path::Path,
};

const NOW: i64 = 1_790_000_000_000;
const ITERATIONS: usize = 5;

#[derive(Default)]
struct Counts {
    lists: AtomicUsize,
    pages: AtomicUsize,
    listed: AtomicUsize,
    gets: AtomicUsize,
    heads: AtomicUsize,
}

impl Counts {
    fn take(&self) -> [usize; 5] {
        [
            &self.lists,
            &self.pages,
            &self.listed,
            &self.gets,
            &self.heads,
        ]
        .map(|count| count.swap(0, Ordering::Relaxed))
    }
}

#[derive(Default)]
struct CountedStore {
    inner: InMemory,
    counts: Arc<Counts>,
}
impl fmt::Debug for CountedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CountedStore")
    }
}
impl fmt::Display for CountedStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("CountedStore")
    }
}

krabka_blockstore::delegate_object_store! {
    CountedStore => inner;
    forward [put_opts, put_multipart_opts, delete_stream, get_ranges, list_with_delimiter, copy_opts];
    async fn get_opts(&self, location: &Path, options: GetOptions) -> object_store::Result<GetResult> {
        if options.head {
            self.counts.heads.fetch_add(1, Ordering::Relaxed);
        } else {
            self.counts.gets.fetch_add(1, Ordering::Relaxed);
        }
        self.inner.get_opts(location, options).await
    }
    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.counts.lists.fetch_add(1, Ordering::Relaxed);
        self.counts.pages.fetch_add(1, Ordering::Relaxed);
        let counts = Arc::clone(&self.counts);
        self.inner.list(prefix)
            .enumerate()
            .map(move |(position, object)| {
                counts.listed.fetch_add(1, Ordering::Relaxed);
                if position > 0 && position % 1000 == 0 {
                    counts.pages.fetch_add(1, Ordering::Relaxed);
                }
                object
            })
            .boxed()
    }
}

async fn publish(
    sink: &ObjectStoreCompactionIndexSink,
    legacy: &dyn ObjectStore,
    tenant: &str,
    timestamp: i64,
    number: usize,
) {
    let mut labels = Labels::new();
    labels.insert("__name__", "up");
    labels.insert("job", &format!("job-{number}"));
    labels.insert("payload", &"x".repeat(512));
    let fingerprint = labels.fingerprint();
    let stem = format!("metrics/{tenant}/float/{number}");
    let manifest = CompactionIndexManifest {
        tenant: tenant.to_owned(),
        kind: MetricBlockKind::Float,
        block_key: format!("{stem}.parquet"),
        index_key: format!("{stem}.index"),
        level: BlockLevel::INGESTED,
        first_offset: 0,
        last_offset: 0,
        row_count: 1,
        min_ts: timestamp,
        max_ts: timestamp,
        fingerprints: vec![fingerprint],
        series: vec![CompactionSeriesLabels {
            fingerprint,
            labels: labels.into(),
        }],
    };
    legacy
        .put(
            &Path::from(manifest.index_key.as_str()),
            manifest.encode().unwrap().into(),
        )
        .await
        .unwrap();
    sink.write_manifest(&manifest).await.unwrap();
}

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    println!(
        "mode,unrelated_tenants,historical,lists,list_pages,listed_objects,gets,heads,allocations,allocated_bytes,mean_us"
    );
    for noise in [0, 1000, 10_000] {
        let counted = Arc::new(CountedStore::default());
        let counts = Arc::clone(&counted.counts);
        let objects: Arc<dyn ObjectStore> = counted;
        let legacy = Arc::new(CountedStore::default());
        let legacy_counts = Arc::clone(&legacy.counts);
        let legacy: Arc<dyn ObjectStore> = legacy;
        runtime.block_on(async {
            let sink = ObjectStoreCompactionIndexSink::new(Arc::clone(&objects));
            for number in 0..3 {
                publish(&sink, legacy.as_ref(), "target", NOW, number).await;
            }
            for number in 0..noise {
                publish(
                    &sink,
                    legacy.as_ref(),
                    &format!("unrelated-{number}"),
                    NOW,
                    number,
                )
                .await;
                publish(
                    &sink,
                    legacy.as_ref(),
                    "target",
                    NOW - 86_400_000 * (1 + i64::try_from(number / 10).unwrap()),
                    number + 3,
                )
                .await;
            }
        });
        for mode in ["global", "tenant_time"] {
            let (objects, counts) = if mode == "global" {
                (&legacy, &legacy_counts)
            } else {
                (&objects, &counts)
            };
            counts.take();
            let mut elapsed = 0;
            let mut allocs = 0;
            let mut bytes = 0;
            for _ in 0..ITERATIONS {
                let start = Instant::now();
                let measured = allocation_counter::measure(|| {
                    runtime.block_on(async {
                        let base = url::Url::parse("memory:///").unwrap();
                        let result = if mode == "global" {
                            assert!(matches!(
                                objects
                                    .head(&Path::from("mimir-tenant-deletions/target.json"))
                                    .await,
                                Err(object_store::Error::NotFound { .. })
                            ));
                            let manifests = load_compaction_manifests_for_range(
                                Arc::clone(objects),
                                "metrics",
                                NOW,
                                NOW + 1000,
                            )
                            .await
                            .unwrap();
                            let blocks = BlockStore::new(Arc::clone(objects), base);
                            let cold = MetricBlockStore::from_compaction_manifests(
                                blocks.empty_like(),
                                Some(blocks.empty_like()),
                                &manifests,
                            );
                            let store = MergedMetricStore::new(cold, WalHead::new());
                            store.series("target", &[], NOW, NOW + 1000).await.unwrap()
                        } else {
                            let store = RefreshingMetricBlockStore::new(
                                Arc::clone(objects),
                                base,
                                "metrics",
                                WalHead::new(),
                            )
                            .with_cold_cache_ttl(Time::ZERO);
                            store.series("target", &[], NOW, NOW + 1000).await.unwrap()
                        };
                        assert!(result.len() == 3);
                    })
                });
                elapsed += start.elapsed().as_micros();
                allocs += measured.count_total;
                bytes += measured.bytes_total;
            }
            let [lists, pages, listed, gets, heads] = counts.take().map(|n| n / ITERATIONS);
            println!(
                "{mode},{noise},{noise},{lists},{pages},{listed},{gets},{heads},{},{},{}",
                allocs / u64::try_from(ITERATIONS).unwrap(),
                bytes / u64::try_from(ITERATIONS).unwrap(),
                elapsed / ITERATIONS as u128
            );
        }
    }
}
