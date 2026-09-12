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
                cache_key: CacheKey::new(id.to_le_bytes()),
                end_epoch_millis,
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
        self.max_active.fetch_max(active, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_millis(10 * (4 - *id) as u64)).await;
        self.active.fetch_sub(1, Ordering::SeqCst);
        Ok(*id)
    }

    fn is_retryable(&self, error: &TestError) -> bool {
        *error == TestError::Transient
    }

    fn should_cache(&self, _result: &usize) -> bool {
        self.cache_results
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
    let key = CacheKey::new(b"key".to_vec());
    QueryCache::<usize>::insert(&cache, &key, &7).await.unwrap();
    clock.set(11);

    assert!(QueryCache::<usize>::get(&cache, &key).await == Ok(None));
}

#[tokio::test]
async fn bounded_in_memory_cache_evicts_the_oldest_entry() {
    let cache = InMemoryCache::new_bounded(Duration::from_mins(1), NonZeroUsize::new(2).unwrap());
    for id in 0_u8..3 {
        QueryCache::insert(&cache, &CacheKey::new([id]), &id)
            .await
            .unwrap();
    }

    assert!(QueryCache::<u8>::get(&cache, &CacheKey::new([0])).await == Ok(None));
    assert!(QueryCache::<u8>::get(&cache, &CacheKey::new([1])).await == Ok(Some(1)));
    assert!(QueryCache::<u8>::get(&cache, &CacheKey::new([2])).await == Ok(Some(2)));
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
        QueryCache::<usize>::get(&cache, &CacheKey::new(0_usize.to_le_bytes()))
            .await
            .unwrap()
            == Some(0)
    );
    assert!(
        QueryCache::<usize>::get(&cache, &CacheKey::new(1_usize.to_le_bytes()))
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
        QueryCache::<usize>::get(&cache, &CacheKey::new(0_usize.to_le_bytes()))
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
    let key = CacheKey::new(b"key".to_vec());
    QueryCache::insert(&first, &key, &vec![1, 2]).await.unwrap();
    assert!(QueryCache::get(&second, &key).await.unwrap() == Some(vec![1, 2]));

    clock.set(11);
    assert!(QueryCache::get(&second, &key).await.unwrap() == None);
}
