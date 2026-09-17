use std::{
    collections::{BTreeMap, VecDeque},
    fmt::Write as _,
    marker::PhantomData,
    num::NonZeroUsize,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use futures::TryStreamExt as _;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use prometheus_client::{
    metrics::{counter::Counter, gauge::Gauge},
    registry::Registry,
};
use serde::{Serialize, de::DeserializeOwned};

const DEFAULT_OBJECT_STORE_CACHE_PREFIX: &str = "_krabka_query_frontend_cache";

/// Object and byte ceilings for a query cache.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CachePolicy {
    pub max_objects: Option<NonZeroUsize>,
    pub max_bytes: Option<NonZeroUsize>,
    pub max_objects_per_tenant: Option<NonZeroUsize>,
    pub max_bytes_per_tenant: Option<NonZeroUsize>,
}

impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            max_objects: NonZeroUsize::new(10_000),
            max_bytes: NonZeroUsize::new(512 * 1024 * 1024),
            max_objects_per_tenant: NonZeroUsize::new(1_000),
            max_bytes_per_tenant: NonZeroUsize::new(64 * 1024 * 1024),
        }
    }
}

/// Stable cache counters and current stored bytes.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CacheMetricsSnapshot {
    pub hits: u64,
    pub misses: u64,
    pub stale: u64,
    pub evictions: u64,
    pub sweeps: u64,
    pub bytes: usize,
    pub errors: u64,
}

#[derive(Default)]
pub struct CacheMetrics {
    hits: Counter,
    misses: Counter,
    stale: Counter,
    evictions: Counter,
    sweeps: Counter,
    bytes: Gauge,
    errors: Counter,
}

impl CacheMetrics {
    #[must_use]
    pub fn snapshot(&self) -> CacheMetricsSnapshot {
        CacheMetricsSnapshot {
            hits: self.hits.get(),
            misses: self.misses.get(),
            stale: self.stale.get(),
            evictions: self.evictions.get(),
            sweeps: self.sweeps.get(),
            bytes: usize::try_from(self.bytes.get()).unwrap_or(0),
            errors: self.errors.get(),
        }
    }

    /// Register this cache's live counters in a service registry.
    pub fn register(&self, registry: &mut Registry) {
        registry.register("query_cache_hits", "Query cache hits.", self.hits.clone());
        registry.register(
            "query_cache_misses",
            "Query cache misses.",
            self.misses.clone(),
        );
        registry.register(
            "query_cache_stale",
            "Expired query cache entries observed.",
            self.stale.clone(),
        );
        registry.register(
            "query_cache_evictions",
            "Query cache entries evicted to enforce bounds.",
            self.evictions.clone(),
        );
        registry.register(
            "query_cache_sweeps",
            "Expired query cache entries removed by sweeps.",
            self.sweeps.clone(),
        );
        registry.register(
            "query_cache_bytes",
            "Current query cache bytes.",
            self.bytes.clone(),
        );
        registry.register(
            "query_cache_errors",
            "Query cache encoding, decoding, and storage errors.",
            self.errors.clone(),
        );
    }
}

/// Tenant-scoped opaque identity of a planned subquery.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CacheKey {
    tenant: String,
    bytes: Vec<u8>,
}

impl CacheKey {
    #[must_use]
    pub fn new(tenant: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            tenant: tenant.into(),
            bytes: bytes.into(),
        }
    }

    #[must_use]
    pub fn tenant(&self) -> &str {
        &self.tenant
    }
}

/// Wall clock used for TTL and freshness decisions.
pub trait Clock: Send + Sync {
    fn now_epoch_millis(&self) -> i64;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_epoch_millis(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |age| i64::try_from(age.as_millis()).unwrap_or(i64::MAX))
    }
}

#[async_trait]
pub trait QueryCache<V>: Send + Sync {
    type Error: Send;

    async fn get(&self, key: &CacheKey) -> Result<Option<V>, Self::Error>;
    async fn insert(&self, key: &CacheKey, value: &V) -> Result<(), Self::Error>;
    async fn sweep(&self) -> Result<usize, Self::Error> {
        Ok(0)
    }
    async fn invalidate_tenant(&self, _tenant: &str) -> Result<usize, Self::Error> {
        Ok(0)
    }
}

/// Process-local TTL cache, primarily useful for tests and single-process deployments.
pub struct InMemoryCache<V> {
    entries: Mutex<BTreeMap<CacheKey, (i64, usize, V)>>,
    order: Mutex<VecDeque<CacheKey>>,
    ttl: Option<Duration>,
    policy: CachePolicy,
    weigh: fn(&V) -> usize,
    clock: Arc<dyn Clock>,
    metrics: Arc<CacheMetrics>,
}

impl<V> Default for InMemoryCache<V> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            order: Mutex::new(VecDeque::new()),
            ttl: None,
            policy: CachePolicy::default(),
            weigh: std::mem::size_of_val,
            clock: Arc::new(SystemClock),
            metrics: Arc::new(CacheMetrics::default()),
        }
    }
}

impl<V> InMemoryCache<V> {
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self {
            ttl: Some(ttl),
            ..Self::default()
        }
    }

    #[must_use]
    pub fn new_bounded(ttl: Duration, max_entries: NonZeroUsize) -> Self {
        Self {
            policy: CachePolicy {
                max_objects: Some(max_entries),
                ..CachePolicy::default()
            },
            ..Self::new(ttl)
        }
    }

    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    #[must_use]
    pub fn with_policy(mut self, policy: CachePolicy) -> Self {
        self.policy = policy;
        self
    }

    #[must_use]
    pub fn with_weigher(mut self, weigh: fn(&V) -> usize) -> Self {
        self.weigh = weigh;
        self
    }

    #[must_use]
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.metrics)
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<CacheMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    fn sweep_expired(&self) -> usize {
        let Some(ttl) = self.ttl else {
            return 0;
        };
        let now_ms = self.clock.now_epoch_millis();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let expired = entries
            .iter()
            .filter(|(_, (stored_at_ms, _, _))| is_expired(*stored_at_ms, now_ms, ttl))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        let mut order = self
            .order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for key in &expired {
            remove_memory_entry(&mut entries, &mut order, key, &self.metrics);
        }
        self.metrics
            .sweeps
            .inc_by(u64::try_from(expired.len()).unwrap_or(u64::MAX));
        expired.len()
    }
}

#[async_trait]
impl<V> QueryCache<V> for InMemoryCache<V>
where
    V: Clone + Send + Sync,
{
    type Error = std::convert::Infallible;

    async fn get(&self, key: &CacheKey) -> Result<Option<V>, Self::Error> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some((stored_at_ms, _, value)) = entries.get(key) else {
            self.metrics.misses.inc();
            return Ok(None);
        };
        if self
            .ttl
            .is_some_and(|ttl| is_expired(*stored_at_ms, self.clock.now_epoch_millis(), ttl))
        {
            let mut order = self
                .order
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            remove_memory_entry(&mut entries, &mut order, key, &self.metrics);
            self.metrics.stale.inc();
            self.metrics.misses.inc();
            return Ok(None);
        }
        self.metrics.hits.inc();
        Ok(Some(value.clone()))
    }

    async fn insert(&self, key: &CacheKey, value: &V) -> Result<(), Self::Error> {
        self.sweep_expired();
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut order = self
            .order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if entries.contains_key(key) {
            remove_memory_entry(&mut entries, &mut order, key, &self.metrics);
        }
        let bytes = (self.weigh)(value);
        if self
            .policy
            .max_bytes
            .is_some_and(|limit| bytes > limit.get())
            || self
                .policy
                .max_bytes_per_tenant
                .is_some_and(|limit| bytes > limit.get())
        {
            self.metrics.errors.inc();
            return Ok(());
        }
        while tenant_objects(&entries, key.tenant())
            >= self
                .policy
                .max_objects_per_tenant
                .map_or(usize::MAX, NonZeroUsize::get)
            || tenant_bytes(&entries, key.tenant()).saturating_add(bytes)
                > self
                    .policy
                    .max_bytes_per_tenant
                    .map_or(usize::MAX, NonZeroUsize::get)
        {
            if !evict_oldest_memory(&mut entries, &mut order, Some(key.tenant()), &self.metrics) {
                break;
            }
        }
        while entries.len()
            >= self
                .policy
                .max_objects
                .map_or(usize::MAX, NonZeroUsize::get)
            || entries
                .values()
                .map(|(_, bytes, _)| bytes)
                .sum::<usize>()
                .saturating_add(bytes)
                > self.policy.max_bytes.map_or(usize::MAX, NonZeroUsize::get)
        {
            if !evict_oldest_memory(&mut entries, &mut order, None, &self.metrics) {
                break;
            }
        }
        order.push_back(key.clone());
        entries.insert(
            key.clone(),
            (self.clock.now_epoch_millis(), bytes, value.clone()),
        );
        self.metrics.bytes.inc_by(gauge_bytes(bytes));
        Ok(())
    }

    async fn sweep(&self) -> Result<usize, Self::Error> {
        Ok(self.sweep_expired())
    }

    async fn invalidate_tenant(&self, tenant: &str) -> Result<usize, Self::Error> {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut order = self
            .order
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let keys = entries
            .keys()
            .filter(|key| key.tenant() == tenant)
            .cloned()
            .collect::<Vec<_>>();
        for key in &keys {
            remove_memory_entry(&mut entries, &mut order, key, &self.metrics);
        }
        Ok(keys.len())
    }
}

fn tenant_objects<V>(entries: &BTreeMap<CacheKey, (i64, usize, V)>, tenant: &str) -> usize {
    entries.keys().filter(|key| key.tenant() == tenant).count()
}

fn tenant_bytes<V>(entries: &BTreeMap<CacheKey, (i64, usize, V)>, tenant: &str) -> usize {
    entries
        .iter()
        .filter(|(key, _)| key.tenant() == tenant)
        .map(|(_, (_, bytes, _))| *bytes)
        .sum()
}

fn remove_memory_entry<V>(
    entries: &mut BTreeMap<CacheKey, (i64, usize, V)>,
    order: &mut VecDeque<CacheKey>,
    key: &CacheKey,
    metrics: &CacheMetrics,
) -> bool {
    let Some((_, bytes, _)) = entries.remove(key) else {
        return false;
    };
    order.retain(|candidate| candidate != key);
    metrics.bytes.dec_by(gauge_bytes(bytes));
    true
}

fn evict_oldest_memory<V>(
    entries: &mut BTreeMap<CacheKey, (i64, usize, V)>,
    order: &mut VecDeque<CacheKey>,
    tenant: Option<&str>,
    metrics: &CacheMetrics,
) -> bool {
    let Some(key) = order
        .iter()
        .find(|key| tenant.is_none_or(|tenant| key.tenant() == tenant))
        .cloned()
    else {
        return false;
    };
    let removed = remove_memory_entry(entries, order, &key, metrics);
    if removed {
        metrics.evictions.inc();
    }
    removed
}

#[async_trait]
impl<C, V> QueryCache<V> for Arc<C>
where
    C: QueryCache<V> + ?Sized,
    V: Sync,
{
    type Error = C::Error;

    async fn get(&self, key: &CacheKey) -> Result<Option<V>, Self::Error> {
        self.as_ref().get(key).await
    }

    async fn insert(&self, key: &CacheKey, value: &V) -> Result<(), Self::Error> {
        self.as_ref().insert(key, value).await
    }

    async fn sweep(&self) -> Result<usize, Self::Error> {
        self.as_ref().sweep().await
    }

    async fn invalidate_tenant(&self, tenant: &str) -> Result<usize, Self::Error> {
        self.as_ref().invalidate_tenant(tenant).await
    }
}

#[async_trait]
impl<C, V> QueryCache<V> for &C
where
    C: QueryCache<V> + ?Sized,
    V: Sync,
{
    type Error = C::Error;

    async fn get(&self, key: &CacheKey) -> Result<Option<V>, Self::Error> {
        (*self).get(key).await
    }

    async fn insert(&self, key: &CacheKey, value: &V) -> Result<(), Self::Error> {
        (*self).insert(key, value).await
    }

    async fn sweep(&self) -> Result<usize, Self::Error> {
        (*self).sweep().await
    }

    async fn invalidate_tenant(&self, tenant: &str) -> Result<usize, Self::Error> {
        (*self).invalidate_tenant(tenant).await
    }
}

/// Serialization or object-store failure from [`ObjectStoreCache`].
#[derive(Debug, thiserror::Error)]
pub enum ObjectStoreCacheError {
    #[error("query frontend cache object-store operation failed: {0}")]
    ObjectStore(#[from] object_store::Error),
    #[error("query frontend cache encoding failed: {0}")]
    Encode(serde_json::Error),
    #[error("query frontend cache decoding failed: {0}")]
    Decode(serde_json::Error),
}

/// Object-store-backed JSON cache shared by query frontend instances.
pub struct ObjectStoreCache<V> {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    ttl: Duration,
    clock: Arc<dyn Clock>,
    policy: CachePolicy,
    metrics: Arc<CacheMetrics>,
    mutation: tokio::sync::Mutex<()>,
    value: PhantomData<fn() -> V>,
}

impl<V> ObjectStoreCache<V> {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>, ttl: Duration) -> Self {
        let prefix = prefix.into();
        let prefix = prefix.trim_matches('/');
        Self {
            store,
            prefix: if prefix.is_empty() {
                DEFAULT_OBJECT_STORE_CACHE_PREFIX.to_owned()
            } else {
                prefix.to_owned()
            },
            ttl,
            clock: Arc::new(SystemClock),
            policy: CachePolicy::default(),
            metrics: Arc::new(CacheMetrics::default()),
            mutation: tokio::sync::Mutex::new(()),
            value: PhantomData,
        }
    }

    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    #[must_use]
    pub fn with_policy(mut self, policy: CachePolicy) -> Self {
        self.policy = policy;
        self
    }

    #[must_use]
    pub fn metrics(&self) -> Arc<CacheMetrics> {
        Arc::clone(&self.metrics)
    }

    #[must_use]
    pub fn with_metrics(mut self, metrics: Arc<CacheMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    fn path(&self, key: &CacheKey) -> Path {
        let tenant = if key.tenant.is_empty() {
            "_".to_string()
        } else {
            hex(key.tenant.as_bytes())
        };
        let name = hex(&key.bytes);
        Path::from(format!("{}/{tenant}/{name}.json", self.prefix))
    }

    fn tenant_prefix(&self, tenant: &str) -> String {
        let tenant = if tenant.is_empty() {
            "_".to_string()
        } else {
            hex(tenant.as_bytes())
        };
        format!("{}/{tenant}/", self.prefix)
    }

    fn is_legacy_path(&self, path: &Path) -> bool {
        path.as_ref()
            .strip_prefix(&self.prefix)
            .and_then(|path| path.strip_prefix('/'))
            .is_some_and(|path| !path.contains('/'))
    }
}

#[derive(serde::Deserialize, serde::Serialize)]
struct StoredValue<V> {
    stored_at_ms: i64,
    value: V,
}

#[async_trait]
impl<V> QueryCache<V> for ObjectStoreCache<V>
where
    V: DeserializeOwned + Send + Sync + Serialize,
{
    type Error = ObjectStoreCacheError;

    async fn get(&self, key: &CacheKey) -> Result<Option<V>, Self::Error> {
        let result = async {
            let path = self.path(key);
            let bytes = match self.store.get(&path).await {
                Ok(result) => result.bytes().await?,
                Err(object_store::Error::NotFound { .. }) => {
                    self.metrics.misses.inc();
                    return Ok(None);
                }
                Err(error) => return Err(error.into()),
            };
            let stored: StoredValue<V> =
                serde_json::from_slice(&bytes).map_err(ObjectStoreCacheError::Decode)?;
            if is_expired(stored.stored_at_ms, self.clock.now_epoch_millis(), self.ttl) {
                if self.store.delete(&path).await.is_err() {
                    self.metrics.errors.inc();
                }
                self.metrics.stale.inc();
                self.metrics.misses.inc();
                let current = usize::try_from(self.metrics.bytes.get()).unwrap_or(0);
                self.metrics
                    .bytes
                    .dec_by(gauge_bytes(current.min(bytes.len())));
                return Ok(None);
            }
            self.metrics.hits.inc();
            Ok(Some(stored.value))
        }
        .await;
        if result.is_err() {
            self.metrics.errors.inc();
        }
        result
    }

    async fn insert(&self, key: &CacheKey, value: &V) -> Result<(), Self::Error> {
        let result = async {
            let bytes = serde_json::to_vec(&StoredValue {
                stored_at_ms: self.clock.now_epoch_millis(),
                value,
            })
            .map_err(ObjectStoreCacheError::Encode)?;
            let _guard = self.mutation.lock().await;
            if self
                .policy
                .max_bytes
                .is_some_and(|limit| bytes.len() > limit.get())
                || self
                    .policy
                    .max_bytes_per_tenant
                    .is_some_and(|limit| bytes.len() > limit.get())
            {
                self.metrics.errors.inc();
                return Ok(());
            }
            self.store
                .put(&self.path(key), PutPayload::from(bytes))
                .await?;
            self.prune_for_insert(key).await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            self.metrics.errors.inc();
        }
        result
    }

    async fn sweep(&self) -> Result<usize, Self::Error> {
        let result = async {
            let _guard = self.mutation.lock().await;
            let prefix = Path::from(self.prefix.clone());
            let mut objects = self.store.list(Some(&prefix));
            let now_ms = self.clock.now_epoch_millis();
            let mut swept = 0;
            while let Some(object) = objects.try_next().await? {
                if self.is_legacy_path(&object.location) {
                    self.store.delete(&object.location).await?;
                    swept += 1;
                    continue;
                }
                let bytes = self.store.get(&object.location).await?.bytes().await?;
                let stored: StoredValue<serde_json::Value> =
                    serde_json::from_slice(&bytes).map_err(ObjectStoreCacheError::Decode)?;
                if is_expired(stored.stored_at_ms, now_ms, self.ttl) {
                    self.store.delete(&object.location).await?;
                    swept += 1;
                }
            }
            self.metrics
                .sweeps
                .inc_by(u64::try_from(swept).unwrap_or(u64::MAX));
            self.refresh_object_store_bytes().await?;
            Ok(swept)
        }
        .await;
        if result.is_err() {
            self.metrics.errors.inc();
        }
        result
    }

    async fn invalidate_tenant(&self, tenant: &str) -> Result<usize, Self::Error> {
        let result = async {
            let _guard = self.mutation.lock().await;
            let prefix = Path::from(self.tenant_prefix(tenant));
            let objects = self
                .store
                .list(Some(&prefix))
                .try_collect::<Vec<_>>()
                .await?;
            for object in &objects {
                self.store.delete(&object.location).await?;
            }
            self.refresh_object_store_bytes().await?;
            Ok(objects.len())
        }
        .await;
        if result.is_err() {
            self.metrics.errors.inc();
        }
        result
    }
}

#[derive(Clone)]
struct StoredObject {
    path: Path,
    stored_at_ms: i64,
    bytes: usize,
    same_tenant: bool,
}

impl<V> ObjectStoreCache<V>
where
    V: DeserializeOwned + Send + Sync + Serialize,
{
    async fn prune_for_insert(&self, key: &CacheKey) -> Result<(), ObjectStoreCacheError> {
        let tenant_prefix = self.tenant_prefix(key.tenant());
        let prefix = Path::from(self.prefix.clone());
        let objects = self
            .store
            .list(Some(&prefix))
            .try_collect::<Vec<_>>()
            .await?;
        let mut stored = Vec::new();
        let mut total_bytes = 0_usize;
        let mut total_objects = 0_usize;
        let mut tenant_bytes = 0_usize;
        let mut tenant_objects = 0_usize;
        for object in objects {
            if self.is_legacy_path(&object.location) {
                self.store.delete(&object.location).await?;
                self.metrics.evictions.inc();
                continue;
            }
            let bytes = self.store.get(&object.location).await?.bytes().await?;
            let value: StoredValue<serde_json::Value> = match serde_json::from_slice(&bytes) {
                Ok(value) => value,
                Err(error) => {
                    self.metrics.errors.inc();
                    return Err(ObjectStoreCacheError::Decode(error));
                }
            };
            if is_expired(value.stored_at_ms, self.clock.now_epoch_millis(), self.ttl) {
                self.store.delete(&object.location).await?;
                self.metrics.stale.inc();
                self.metrics.sweeps.inc();
                continue;
            }
            let same_tenant = object.location.as_ref().starts_with(&tenant_prefix);
            total_objects += 1;
            total_bytes = total_bytes.saturating_add(bytes.len());
            if same_tenant {
                tenant_objects += 1;
                tenant_bytes = tenant_bytes.saturating_add(bytes.len());
            }
            stored.push(StoredObject {
                path: object.location,
                stored_at_ms: value.stored_at_ms,
                bytes: bytes.len(),
                same_tenant,
            });
        }
        stored.sort_by(|left, right| {
            (left.stored_at_ms, left.path.as_ref()).cmp(&(right.stored_at_ms, right.path.as_ref()))
        });
        while tenant_objects
            > self
                .policy
                .max_objects_per_tenant
                .map_or(usize::MAX, NonZeroUsize::get)
            || tenant_bytes
                > self
                    .policy
                    .max_bytes_per_tenant
                    .map_or(usize::MAX, NonZeroUsize::get)
        {
            let Some(position) = stored.iter().position(|object| object.same_tenant) else {
                break;
            };
            let object = stored.remove(position);
            self.store.delete(&object.path).await?;
            tenant_objects -= 1;
            tenant_bytes -= object.bytes;
            total_objects -= 1;
            total_bytes -= object.bytes;
            self.metrics.evictions.inc();
        }
        while total_objects
            > self
                .policy
                .max_objects
                .map_or(usize::MAX, NonZeroUsize::get)
            || total_bytes > self.policy.max_bytes.map_or(usize::MAX, NonZeroUsize::get)
        {
            let Some(object) = stored.first().cloned() else {
                break;
            };
            stored.remove(0);
            self.store.delete(&object.path).await?;
            total_objects -= 1;
            total_bytes -= object.bytes;
            self.metrics.evictions.inc();
        }
        self.metrics.bytes.set(gauge_bytes(total_bytes));
        Ok(())
    }

    async fn refresh_object_store_bytes(&self) -> Result<(), ObjectStoreCacheError> {
        let prefix = Path::from(self.prefix.clone());
        let bytes = self
            .store
            .list(Some(&prefix))
            .map_ok(|object| usize::try_from(object.size).unwrap_or(usize::MAX))
            .try_fold(0_usize, |total, bytes| async move {
                Ok(total.saturating_add(bytes))
            })
            .await?;
        self.metrics.bytes.set(gauge_bytes(bytes));
        Ok(())
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(encoded, "{byte:02x}");
    }
    encoded
}

fn gauge_bytes(bytes: usize) -> i64 {
    i64::try_from(bytes).unwrap_or(i64::MAX)
}

fn is_expired(stored_at_ms: i64, now_ms: i64, ttl: Duration) -> bool {
    let age_ms = now_ms.saturating_sub(stored_at_ms);
    u128::try_from(age_ms).unwrap_or(0) > ttl.as_millis()
}
