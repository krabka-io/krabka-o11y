use std::{
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicI64, AtomicUsize, Ordering},
    },
    time::Duration,
};

use assert2::assert;
use async_trait::async_trait;
use futures::TryStreamExt as _;
use object_store::{ObjectStoreExt as _, PutPayload, path::Path};

use super::*;

#[derive(Default)]
struct ManualClock(AtomicI64);

impl ManualClock {
    fn set(&self, now_ms: i64) {
        self.0.store(now_ms, Ordering::Relaxed);
    }
}

impl Clock for ManualClock {
    fn now_epoch_millis(&self) -> i64 {
        self.0.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TestError {
    Transient,
    Permanent,
}

struct ActiveGuard<'a>(&'a AtomicUsize);

impl Drop for ActiveGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl std::fmt::Display for TestError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{self:?}")
    }
}

struct TestAdapter {
    queries: Vec<usize>,
    ends: Vec<i64>,
    active: AtomicUsize,
    max_active: AtomicUsize,
    attempts: Mutex<Vec<usize>>,
    transient_failures: usize,
    permanent: Option<usize>,
    cache_results: bool,
    admission_limits: Option<AdmissionLimits>,
}

impl TestAdapter {
    fn new(queries: Vec<usize>, ends: Vec<i64>) -> Self {
        let count = queries.iter().copied().max().map_or(0, |id| id + 1);
        Self {
            queries,
            ends,
            active: AtomicUsize::new(0),
            max_active: AtomicUsize::new(0),
            attempts: Mutex::new(vec![0; count]),
            transient_failures: 0,
            permanent: None,
            cache_results: true,
            admission_limits: None,
        }
    }
}

#[async_trait]
impl QueryFrontendAdapter for TestAdapter {
    type Request = ();
    type Query = usize;
    type Output = usize;
    type Response = Vec<usize>;
    type Error = TestError;

    fn plan(&self, (): &()) -> Result<Vec<PlannedQuery<usize>>, TestError> {
        Ok(self
            .queries
            .iter()
            .copied()
            .zip(self.ends.iter().copied())
            .map(|(id, end_epoch_millis)| PlannedQuery {
                query: id,
                cache_key: CacheKey::new("test", id.to_le_bytes()),
                end_epoch_millis,
                estimated_bytes: 0,
            })
            .collect())
    }

    async fn execute(&self, id: &usize) -> Result<usize, TestError> {
        let attempt = {
            let mut attempts = self.attempts.lock().unwrap();
            attempts[*id] += 1;
            attempts[*id]
        };
        if self.permanent == Some(*id) {
            return Err(TestError::Permanent);
        }
        if attempt <= self.transient_failures {
            return Err(TestError::Transient);
        }

        let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
        let _active = ActiveGuard(&self.active);
        self.max_active.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(10 * (4 - *id) as u64)).await;
        Ok(*id)
    }

    fn is_retryable(&self, error: &TestError) -> bool {
        *error == TestError::Transient
    }

    fn should_cache(&self, _result: &usize) -> bool {
        self.cache_results
    }

    fn admission_limits(&self, (): &()) -> Option<AdmissionLimits> {
        self.admission_limits
    }

    fn merge(&self, (): &(), results: Vec<usize>) -> Result<Vec<usize>, TestError> {
        Ok(results)
    }
}

fn options(max_parallelism: usize, max_retries: usize, freshness_ms: u64) -> ExecutionOptions {
    ExecutionOptions {
        max_parallelism: NonZeroUsize::new(max_parallelism).unwrap(),
        max_retries,
        max_cache_freshness: Duration::from_millis(freshness_ms),
    }
}

#[tokio::test]
async fn fan_out_is_bounded_and_preserves_plan_order() {
    let adapter = TestAdapter::new(vec![0, 1, 2, 3], vec![0; 4]);
    let frontend = QueryFrontend::new(InMemoryCache::new(Duration::from_mins(1)), options(2, 0, 0));

    let result = frontend.execute(&adapter, &()).await.unwrap();

    assert!(result == vec![0, 1, 2, 3]);
    assert!(adapter.max_active.load(Ordering::SeqCst) == 2);
}

#[tokio::test]
async fn canceling_a_request_stops_backend_work_and_releases_admission() {
    let limits = AdmissionLimits {
        max_concurrent_requests: 1,
        max_concurrent_requests_per_tenant: 1,
        ..AdmissionLimits::default()
    };
    let controller = AdmissionController::new(limits);
    let frontend = Arc::new(
        QueryFrontend::new(InMemoryCache::new(Duration::from_mins(1)), options(1, 0, 0))
            .with_admission(Arc::clone(&controller)),
    );
    let mut adapter = TestAdapter::new(vec![0], vec![0]);
    adapter.admission_limits = Some(limits);
    let adapter = Arc::new(adapter);
    let task = tokio::spawn({
        let frontend = Arc::clone(&frontend);
        let adapter = Arc::clone(&adapter);
        async move { frontend.execute(adapter.as_ref(), &()).await }
    });
    while adapter.active.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert!(adapter.active.load(Ordering::SeqCst) == 0);
    assert!(
        tokio::time::timeout(
            Duration::from_secs(1),
            controller.acquire("other".into(), 1, 1),
        )
        .await
        .unwrap()
        .is_ok()
    );
}

#[tokio::test]
async fn retries_only_transient_errors_up_to_the_configured_limit() {
    let mut transient = TestAdapter::new(vec![0], vec![0]);
    transient.transient_failures = 2;
    let frontend = QueryFrontend::new(InMemoryCache::new(Duration::from_mins(1)), options(1, 2, 0));
    assert!(frontend.execute(&transient, &()).await.unwrap() == vec![0]);
    assert!(transient.attempts.lock().unwrap()[0] == 3);

    let mut exhausted = TestAdapter::new(vec![0], vec![0]);
    exhausted.transient_failures = 3;
    let frontend = QueryFrontend::new(InMemoryCache::new(Duration::from_mins(1)), options(1, 2, 0));
    let error = frontend.execute(&exhausted, &()).await.unwrap_err();
    assert!(matches!(
        error,
        QueryFrontendError::Adapter(TestError::Transient)
    ));
    assert!(exhausted.attempts.lock().unwrap()[0] == 3);

    let mut permanent = TestAdapter::new(vec![0], vec![0]);
    permanent.permanent = Some(0);
    let frontend = QueryFrontend::new(InMemoryCache::new(Duration::from_mins(1)), options(1, 2, 0));
    let error = frontend.execute(&permanent, &()).await.unwrap_err();
    assert!(matches!(
        error,
        QueryFrontendError::Adapter(TestError::Permanent)
    ));
    assert!(permanent.attempts.lock().unwrap()[0] == 1);
}

#[tokio::test]
async fn in_memory_cache_expires_entries_after_ttl() {
    let clock = Arc::new(ManualClock::default());
    let cache = InMemoryCache::new(Duration::from_millis(10)).with_clock(clock.clone());
    let key = CacheKey::new("tenant-a", b"key".to_vec());
    QueryCache::<usize>::insert(&cache, &key, &7).await.unwrap();
    clock.set(11);

    assert!(QueryCache::<usize>::get(&cache, &key).await == Ok(None));
}

#[tokio::test]
async fn bounded_in_memory_cache_evicts_the_oldest_entry() {
    let cache = InMemoryCache::new_bounded(Duration::from_mins(1), NonZeroUsize::new(2).unwrap());
    for id in 0_u8..3 {
        QueryCache::insert(&cache, &CacheKey::new("tenant-a", [id]), &id)
            .await
            .unwrap();
    }

    assert!(QueryCache::<u8>::get(&cache, &CacheKey::new("tenant-a", [0])).await == Ok(None));
    assert!(QueryCache::<u8>::get(&cache, &CacheKey::new("tenant-a", [1])).await == Ok(Some(1)));
    assert!(QueryCache::<u8>::get(&cache, &CacheKey::new("tenant-a", [2])).await == Ok(Some(2)));
}

#[tokio::test]
async fn in_memory_cache_enforces_per_tenant_bytes_and_exports_metrics() {
    let cache = InMemoryCache::new(Duration::from_mins(1))
        .with_policy(CachePolicy {
            max_objects: NonZeroUsize::new(10),
            max_bytes: NonZeroUsize::new(20),
            max_objects_per_tenant: NonZeroUsize::new(10),
            max_bytes_per_tenant: NonZeroUsize::new(5),
        })
        .with_weigher(Vec::len);
    let first = CacheKey::new("tenant-a", b"first");
    let second = CacheKey::new("tenant-a", b"second");
    let other = CacheKey::new("tenant-b", b"other");
    QueryCache::insert(&cache, &first, &vec![1; 4])
        .await
        .unwrap();
    QueryCache::insert(&cache, &other, &vec![2; 4])
        .await
        .unwrap();
    QueryCache::insert(&cache, &second, &vec![3; 4])
        .await
        .unwrap();

    assert!(QueryCache::<Vec<u8>>::get(&cache, &first).await == Ok(None));
    assert!(QueryCache::<Vec<u8>>::get(&cache, &second).await == Ok(Some(vec![3; 4])));
    assert!(QueryCache::<Vec<u8>>::get(&cache, &other).await == Ok(Some(vec![2; 4])));
    let metrics = cache.metrics().snapshot();
    assert!(metrics.evictions == 1);
    assert!(metrics.hits == 2);
    assert!(metrics.misses == 1);
    assert!(metrics.bytes == 8);

    let mut registry = prometheus_client::registry::Registry::default();
    cache.metrics().register(&mut registry);
    let mut encoded = String::new();
    prometheus_client::encoding::text::encode(&mut encoded, &registry).unwrap();
    for name in [
        "query_cache_hits_total",
        "query_cache_misses_total",
        "query_cache_stale_total",
        "query_cache_evictions_total",
        "query_cache_sweeps_total",
        "query_cache_bytes",
        "query_cache_errors_total",
    ] {
        assert!(encoded.contains(name), "missing {name}: {encoded}");
    }
}

#[tokio::test]
async fn tenant_invalidation_does_not_cross_namespaces() {
    let cache = InMemoryCache::new(Duration::from_mins(1));
    let tenant_a = CacheKey::new("tenant-a", b"same");
    let tenant_b = CacheKey::new("tenant-b", b"same");
    QueryCache::insert(&cache, &tenant_a, &1).await.unwrap();
    QueryCache::insert(&cache, &tenant_b, &2).await.unwrap();

    assert!(QueryCache::<usize>::invalidate_tenant(&cache, "tenant-a").await == Ok(1));
    assert!(QueryCache::<usize>::get(&cache, &tenant_a).await == Ok(None));
    assert!(QueryCache::<usize>::get(&cache, &tenant_b).await == Ok(Some(2)));
}

#[tokio::test]
async fn fresh_results_are_not_inserted() {
    let clock = Arc::new(ManualClock::default());
    clock.set(1_000);
    let cache = Arc::new(InMemoryCache::new(Duration::from_mins(1)));
    let frontend = QueryFrontend::new(cache.clone(), options(2, 0, 100)).with_clock(clock);
    let adapter = TestAdapter::new(vec![0, 1], vec![900, 901]);

    assert!(frontend.execute(&adapter, &()).await.unwrap() == vec![0, 1]);
    assert!(
        QueryCache::<usize>::get(&cache, &CacheKey::new("test", 0_usize.to_le_bytes()))
            .await
            .unwrap()
            == Some(0)
    );
    assert!(
        QueryCache::<usize>::get(&cache, &CacheKey::new("test", 1_usize.to_le_bytes()))
            .await
            .unwrap()
            == None
    );
}

#[tokio::test]
async fn adapter_can_refuse_to_cache_mutable_results() {
    let cache = Arc::new(InMemoryCache::new(Duration::from_mins(1)));
    let frontend = QueryFrontend::new(cache.clone(), options(1, 0, 0));
    let mut adapter = TestAdapter::new(vec![0], vec![0]);
    adapter.cache_results = false;

    assert!(frontend.execute(&adapter, &()).await.unwrap() == vec![0]);
    assert!(
        QueryCache::<usize>::get(&cache, &CacheKey::new("test", 0_usize.to_le_bytes()))
            .await
            .unwrap()
            == None
    );
}

#[tokio::test]
async fn object_store_cache_round_trips_and_expires() {
    let clock = Arc::new(ManualClock::default());
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let first = ObjectStoreCache::<Vec<usize>>::new(
        store.clone(),
        "query-cache",
        Duration::from_millis(10),
    )
    .with_clock(clock.clone());
    let second =
        ObjectStoreCache::<Vec<usize>>::new(store, "query-cache", Duration::from_millis(10))
            .with_clock(clock.clone());
    let key = CacheKey::new("tenant-a", b"key".to_vec());
    QueryCache::insert(&first, &key, &vec![1, 2]).await.unwrap();
    assert!(QueryCache::get(&second, &key).await.unwrap() == Some(vec![1, 2]));

    clock.set(11);
    assert!(QueryCache::get(&second, &key).await.unwrap() == None);
}

#[tokio::test]
async fn object_store_cache_counts_decode_errors() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let cache =
        ObjectStoreCache::<usize>::new(store.clone(), "query-cache", Duration::from_mins(1));
    store
        .put(
            &Path::from("query-cache/74656e616e742d61/6b6579.json"),
            PutPayload::from("not-json"),
        )
        .await
        .unwrap();

    assert!(
        QueryCache::get(&cache, &CacheKey::new("tenant-a", b"key"))
            .await
            .is_err()
    );
    assert!(cache.metrics().snapshot().errors == 1);
}

#[tokio::test]
async fn cache_keys_are_isolated_by_tenant() {
    let cache = InMemoryCache::default();
    let tenant_a = CacheKey::new("tenant-a", b"same".to_vec());
    let tenant_b = CacheKey::new("tenant-b", b"same".to_vec());
    QueryCache::insert(&cache, &tenant_a, &1).await.unwrap();
    QueryCache::insert(&cache, &tenant_b, &2).await.unwrap();

    assert!(QueryCache::<usize>::get(&cache, &tenant_a).await == Ok(Some(1)));
    assert!(QueryCache::<usize>::get(&cache, &tenant_b).await == Ok(Some(2)));
}

#[tokio::test]
async fn sweeps_expired_entries_without_looking_them_up() {
    let clock = Arc::new(ManualClock::default());
    let cache = InMemoryCache::new(Duration::from_millis(10)).with_clock(clock.clone());
    let key = CacheKey::new("tenant-a", b"stale".to_vec());
    QueryCache::<usize>::insert(&cache, &key, &1).await.unwrap();
    clock.set(11);

    assert!(QueryCache::<usize>::sweep(&cache).await == Ok(1));
    assert!(QueryCache::<usize>::get(&cache, &key).await == Ok(None));
}

#[tokio::test]
async fn object_store_sweep_removes_only_expired_cache_objects() {
    let clock = Arc::new(ManualClock::default());
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let cache = ObjectStoreCache::<usize>::new(store, "query-cache", Duration::from_millis(10))
        .with_clock(clock.clone());
    let stale = CacheKey::new("tenant-a", b"stale".to_vec());
    let live = CacheKey::new("tenant-b", b"live".to_vec());
    QueryCache::insert(&cache, &stale, &1).await.unwrap();
    clock.set(8);
    QueryCache::insert(&cache, &live, &2).await.unwrap();
    clock.set(11);

    assert!(QueryCache::sweep(&cache).await.unwrap() == 1);
    assert!(QueryCache::get(&cache, &stale).await.unwrap() == None);
    assert!(QueryCache::get(&cache, &live).await.unwrap() == Some(2));
}

#[tokio::test]
async fn object_store_cache_enforces_global_and_tenant_object_limits() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let cache = ObjectStoreCache::<usize>::new(store, "query-cache", Duration::from_mins(1))
        .with_policy(CachePolicy {
            max_objects: NonZeroUsize::new(2),
            max_bytes: NonZeroUsize::new(10_000),
            max_objects_per_tenant: NonZeroUsize::new(1),
            max_bytes_per_tenant: NonZeroUsize::new(10_000),
        });
    let first = CacheKey::new("tenant-a", b"first");
    let second = CacheKey::new("tenant-a", b"second");
    let other = CacheKey::new("tenant-b", b"other");
    QueryCache::insert(&cache, &first, &1).await.unwrap();
    QueryCache::insert(&cache, &other, &2).await.unwrap();
    QueryCache::insert(&cache, &second, &3).await.unwrap();

    assert!(QueryCache::get(&cache, &first).await.unwrap() == None);
    assert!(QueryCache::get(&cache, &second).await.unwrap() == Some(3));
    assert!(QueryCache::get(&cache, &other).await.unwrap() == Some(2));
    assert!(cache.metrics().snapshot().evictions == 1);
}

#[tokio::test]
async fn concurrent_cache_mutations_are_idempotent() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let cache = Arc::new(ObjectStoreCache::<usize>::new(
        store,
        "query-cache",
        Duration::from_mins(1),
    ));
    let key = CacheKey::new("tenant-a", b"same");
    let (left, right) = tokio::join!(
        QueryCache::insert(&cache, &key, &1),
        QueryCache::insert(&cache, &key, &1),
    );
    left.unwrap();
    right.unwrap();

    let (left, right) = tokio::join!(
        QueryCache::<usize>::invalidate_tenant(&cache, "tenant-a"),
        QueryCache::<usize>::invalidate_tenant(&cache, "tenant-a"),
    );
    assert!(left.unwrap() + right.unwrap() == 1);
    assert!(QueryCache::get(&cache, &key).await.unwrap() == None);
}

#[tokio::test]
async fn concurrent_cache_instances_converge_on_the_same_bound() {
    let store: Arc<dyn object_store::ObjectStore> = Arc::new(object_store::memory::InMemory::new());
    let policy = CachePolicy {
        max_objects: NonZeroUsize::new(1),
        max_bytes: NonZeroUsize::new(10_000),
        max_objects_per_tenant: NonZeroUsize::new(1),
        max_bytes_per_tenant: NonZeroUsize::new(10_000),
    };
    let left =
        ObjectStoreCache::<usize>::new(Arc::clone(&store), "query-cache", Duration::from_mins(1))
            .with_policy(policy);
    let right =
        ObjectStoreCache::<usize>::new(Arc::clone(&store), "query-cache", Duration::from_mins(1))
            .with_policy(policy);
    let left_key = CacheKey::new("tenant-a", b"left");
    let right_key = CacheKey::new("tenant-a", b"right");

    let (left_result, right_result) = tokio::join!(
        QueryCache::insert(&left, &left_key, &1),
        QueryCache::insert(&right, &right_key, &2),
    );
    left_result.unwrap();
    right_result.unwrap();

    let objects = store
        .list(Some(&Path::from("query-cache")))
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(objects.len() <= 1);
}

#[tokio::test]
async fn cache_storage_and_memory_converge_under_tenant_churn() {
    let clock = Arc::new(ManualClock::default());
    let policy = CachePolicy {
        max_objects: NonZeroUsize::new(20),
        max_bytes: NonZeroUsize::new(10_000),
        max_objects_per_tenant: NonZeroUsize::new(4),
        max_bytes_per_tenant: NonZeroUsize::new(2_000),
    };
    let memory = InMemoryCache::new(Duration::from_millis(10))
        .with_clock(clock.clone())
        .with_policy(policy)
        .with_weigher(Vec::len);
    let storage = ObjectStoreCache::new(
        Arc::new(object_store::memory::InMemory::new()),
        "query-cache",
        Duration::from_millis(10),
    )
    .with_clock(clock.clone())
    .with_policy(policy);

    for id in 0_usize..100 {
        let key = CacheKey::new(format!("tenant-{}", id % 10), id.to_le_bytes());
        QueryCache::insert(&memory, &key, &vec![0_u8; 8])
            .await
            .unwrap();
        QueryCache::insert(&storage, &key, &id).await.unwrap();
    }
    assert!(memory.metrics().snapshot().bytes <= 20 * 8);
    assert!(memory.metrics().snapshot().evictions >= 80);
    assert!(storage.metrics().snapshot().evictions >= 80);

    clock.set(11);
    assert!(QueryCache::<Vec<u8>>::sweep(&memory).await.unwrap() <= 20);
    assert!(QueryCache::<usize>::sweep(&storage).await.unwrap() <= 20);
    assert!(memory.metrics().snapshot().bytes == 0);
    assert!(storage.metrics().snapshot().bytes == 0);
}

#[tokio::test]
async fn empty_object_store_prefix_stays_in_its_cache_namespace() {
    let clock = Arc::new(ManualClock::default());
    let store = Arc::new(object_store::memory::InMemory::new());
    store
        .put(&Path::from("unrelated"), PutPayload::from("keep"))
        .await
        .unwrap();
    let cache = ObjectStoreCache::<usize>::new(store.clone(), "/", Duration::from_millis(10))
        .with_clock(clock.clone());
    QueryCache::insert(&cache, &CacheKey::new("tenant-a", b"stale"), &1)
        .await
        .unwrap();
    clock.set(11);

    assert!(QueryCache::sweep(&cache).await.unwrap() == 1);
    assert!(store.get(&Path::from("unrelated")).await.is_ok());
}

#[tokio::test]
async fn object_store_sweep_removes_the_pre_tenant_layout() {
    let store = Arc::new(object_store::memory::InMemory::new());
    let legacy = Path::from("query-cache/6b6579.json");
    store
        .put(&legacy, PutPayload::from("obsolete"))
        .await
        .unwrap();
    let cache =
        ObjectStoreCache::<usize>::new(store.clone(), "query-cache", Duration::from_millis(10));

    assert!(QueryCache::sweep(&cache).await.unwrap() == 1);
    assert!(matches!(
        store.get(&legacy).await,
        Err(object_store::Error::NotFound { .. })
    ));
}
