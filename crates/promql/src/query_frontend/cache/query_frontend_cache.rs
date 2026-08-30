use super::*;

/// In-memory range-result cache for query-frontend fan-out responses.
///
/// The backing store is small and swappable on purpose. Production wiring can
/// replace it with an object-store or topic-backed implementation and keep the
/// key contract that the tests cover here.
///
/// Each entry carries an insertion timestamp from the configured internal clock.
/// When [`QueryFrontendCache::with_ttl`] sets a TTL, a `get` for an entry older
/// than the TTL evicts that entry and reports a miss. With no TTL, the default,
/// entries never expire.
pub struct QueryFrontendCache {
    pub(crate) range_results: Mutex<BTreeMap<RangeCacheKey, (i64, QueryResult)>>,
    pub(crate) ttl: Option<Time>,
    pub(crate) clock: Arc<dyn Clock>,
}

impl Default for QueryFrontendCache {
    fn default() -> Self {
        Self {
            range_results: Mutex::new(BTreeMap::new()),
            ttl: None,
            clock: Arc::new(SystemClock),
        }
    }
}

impl QueryFrontendCache {
    /// Builds a cache that expires entries older than `ttl`.
    #[must_use]
    pub fn with_ttl(ttl: Time) -> Self {
        Self {
            ttl: Some(ttl),
            ..Self::default()
        }
    }

    /// Overrides the wall clock, mainly for deterministic tests.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    #[must_use]
    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn get(&self, tenant: &str, query: &FrontendRangeQuery) -> Option<QueryResult> {
        let key = RangeCacheKey::new(tenant, query);
        let mut entries = self
            .range_results
            .lock()
            .expect("query frontend cache poisoned");
        let (inserted, result) = entries.get(&key)?;
        if entry_is_expired(self.ttl, *inserted, self.clock.now_epoch_millis()) {
            entries.remove(&key);
            return None;
        }
        Some(result.clone())
    }

    /// # Panics
    /// Panics if shared metric state is poisoned or validated series data is missing an index entry required by the operation.
    pub fn insert(&self, tenant: &str, query: &FrontendRangeQuery, result: QueryResult) {
        let inserted = self.clock.now_epoch_millis();
        self.range_results
            .lock()
            .expect("query frontend cache poisoned")
            .insert(RangeCacheKey::new(tenant, query), (inserted, result));
    }
}

#[async_trait]
impl RangeQueryCache for QueryFrontendCache {
    async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<QueryResult>, PromqlError> {
        Ok(QueryFrontendCache::get(self, tenant, query))
    }

    async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: QueryResult,
    ) -> Result<(), PromqlError> {
        QueryFrontendCache::insert(self, tenant, query, result);
        Ok(())
    }
}
