use std::{
    collections::BTreeMap,
    fmt::Write as _,
    marker::PhantomData,
    sync::{Arc, Mutex},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use async_trait::async_trait;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload, path::Path};
use serde::{Serialize, de::DeserializeOwned};

/// Opaque identity of a planned subquery.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CacheKey(Vec<u8>);

impl CacheKey {
    #[must_use]
    pub fn new(bytes: impl Into<Vec<u8>>) -> Self {
        Self(bytes.into())
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
}

/// Process-local TTL cache, primarily useful for tests and single-process deployments.
pub struct InMemoryCache<V> {
    entries: Mutex<BTreeMap<CacheKey, (i64, V)>>,
    ttl: Option<Duration>,
    clock: Arc<dyn Clock>,
}

impl<V> Default for InMemoryCache<V> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(BTreeMap::new()),
            ttl: None,
            clock: Arc::new(SystemClock),
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
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
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
        let Some((stored_at_ms, value)) = entries.get(key) else {
            return Ok(None);
        };
        if self
            .ttl
            .is_some_and(|ttl| is_expired(*stored_at_ms, self.clock.now_epoch_millis(), ttl))
        {
            entries.remove(key);
            return Ok(None);
        }
        Ok(Some(value.clone()))
    }

    async fn insert(&self, key: &CacheKey, value: &V) -> Result<(), Self::Error> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.clone(), (self.clock.now_epoch_millis(), value.clone()));
        Ok(())
    }
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
    value: PhantomData<fn() -> V>,
}

impl<V> ObjectStoreCache<V> {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>, ttl: Duration) -> Self {
        let prefix = prefix.into();
        Self {
            store,
            prefix: prefix.trim_matches('/').to_owned(),
            ttl,
            clock: Arc::new(SystemClock),
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

    fn path(&self, key: &CacheKey) -> Path {
        let mut name = String::with_capacity(key.0.len() * 2);
        for byte in &key.0 {
            let _ = write!(name, "{byte:02x}");
        }
        if self.prefix.is_empty() {
            Path::from(format!("{name}.json"))
        } else {
            Path::from(format!("{}/{name}.json", self.prefix))
        }
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
        let path = self.path(key);
        let bytes = match self.store.get(&path).await {
            Ok(result) => result.bytes().await?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let stored: StoredValue<V> =
            serde_json::from_slice(&bytes).map_err(ObjectStoreCacheError::Decode)?;
        if is_expired(stored.stored_at_ms, self.clock.now_epoch_millis(), self.ttl) {
            let _ = self.store.delete(&path).await;
            return Ok(None);
        }
        Ok(Some(stored.value))
    }

    async fn insert(&self, key: &CacheKey, value: &V) -> Result<(), Self::Error> {
        let bytes = serde_json::to_vec(&StoredValue {
            stored_at_ms: self.clock.now_epoch_millis(),
            value,
        })
        .map_err(ObjectStoreCacheError::Encode)?;
        self.store
            .put(&self.path(key), PutPayload::from(bytes))
            .await?;
        Ok(())
    }
}

fn is_expired(stored_at_ms: i64, now_ms: i64, ttl: Duration) -> bool {
    let age_ms = now_ms.saturating_sub(stored_at_ms);
    u128::try_from(age_ms).unwrap_or(0) > ttl.as_millis()
}
