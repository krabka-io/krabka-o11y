use super::*;

/// Object-store backed range-result cache for query-frontend fan-out responses.
///
/// Each cached object embeds the epoch-millis instant of its store operation.
/// When [`ObjectStoreQueryFrontendCache::with_ttl`] sets a TTL, a `get` for an
/// object older than the TTL reports a miss. That `get` also deletes the stale
/// object on a best-effort basis.
pub struct ObjectStoreQueryFrontendCache {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) prefix: String,
    pub(crate) ttl: Option<Time>,
    pub(crate) clock: Arc<dyn Clock>,
}

impl ObjectStoreQueryFrontendCache {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        Self {
            store,
            prefix: normalize_cache_prefix(&prefix),
            ttl: None,
            clock: Arc::new(SystemClock),
        }
    }

    /// Expires cached objects older than `ttl`.
    #[must_use]
    pub fn with_ttl(mut self, ttl: Time) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Overrides the wall clock, mainly for deterministic tests.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError> {
        <Self as RangeQueryCache>::get(self, tenant, query).await
    }

    /// # Errors
    /// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
    pub async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError> {
        <Self as RangeQueryCache>::insert(self, tenant, query, result).await
    }

    pub(crate) fn path(&self, tenant: &str, query: &FrontendRangeQuery) -> Path {
        Path::from(format!(
            "{}/{}.json",
            self.prefix,
            range_cache_key_object_name(tenant, query)
        ))
    }
}

#[async_trait]
impl RangeQueryCache for ObjectStoreQueryFrontendCache {
    async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError> {
        let path = self.path(tenant, query);
        let bytes = match self.store.get(&path).await {
            Ok(result) => result
                .bytes()
                .await
                .map_err(|error| cache_store_error(&error))?,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(error) => return Err(cache_store_error(&error)),
        };
        let stored: StoredRangeResult = serde_json::from_slice(&bytes).map_err(|error| {
            PromqlError::Store(format!("query frontend cache decode failed: {error}"))
        })?;
        if entry_is_expired(self.ttl, stored.stored_at_ms, self.clock.now_epoch_millis()) {
            // Best-effort eviction; a delete failure must not fail the read.
            let _ = self.store.delete(&path).await;
            return Ok(None);
        }
        Ok(Some(stored.result))
    }

    async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError> {
        let path = self.path(tenant, query);
        let stored = StoredRangeResult {
            stored_at_ms: self.clock.now_epoch_millis(),
            result,
        };
        let bytes = serde_json::to_vec(&stored).map_err(|error| {
            PromqlError::Store(format!("query frontend cache encode failed: {error}"))
        })?;
        self.store
            .put(&path, PutPayload::from(bytes))
            .await
            .map_err(|error| cache_store_error(&error))?;
        Ok(())
    }
}
