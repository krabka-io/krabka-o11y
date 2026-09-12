use std::{fmt::Display, sync::Arc, time::Duration};

use async_trait::async_trait;
pub use krabka_query_frontend::Clock;
use krabka_query_frontend::{
    CacheKey, ExecutionOptions, InMemoryCache, ObjectStoreCache, QueryCache,
};
use krabka_units::prelude::*;
use object_store::ObjectStore;

use super::{FrontendRangeQuery, TimeExt};
use crate::{AnnotatedQueryResult, PromqlError};

const DEFAULT_RESULT_CACHE_TTL: Duration = Duration::from_hours(7 * 24);

/// A `PromQL` result cache backed by a shared query-frontend cache.
pub struct PromqlQueryFrontendCache<C> {
    inner: C,
    execution_options: ExecutionOptions,
}

/// The process-local `PromQL` range-result cache.
pub type QueryFrontendCache = PromqlQueryFrontendCache<InMemoryCache<AnnotatedQueryResult>>;

/// The object-store-backed `PromQL` range-result cache.
pub type ObjectStoreQueryFrontendCache =
    PromqlQueryFrontendCache<ObjectStoreCache<AnnotatedQueryResult>>;

impl Default for QueryFrontendCache {
    fn default() -> Self {
        Self {
            inner: InMemoryCache::default(),
            execution_options: ExecutionOptions::default(),
        }
    }
}

impl QueryFrontendCache {
    #[must_use]
    pub fn with_ttl(ttl: Time) -> Self {
        Self {
            inner: InMemoryCache::new(ttl.to_std()),
            execution_options: ExecutionOptions::default(),
        }
    }

    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.inner = self.inner.with_clock(clock);
        self
    }

    pub async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<AnnotatedQueryResult>, PromqlError> {
        QueryCache::get(self, &range_cache_key(tenant, query)).await
    }

    pub async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: AnnotatedQueryResult,
    ) -> Result<(), PromqlError> {
        QueryCache::insert(self, &range_cache_key(tenant, query), &result).await
    }
}

impl ObjectStoreQueryFrontendCache {
    #[must_use]
    pub fn new(store: Arc<dyn ObjectStore>, prefix: impl Into<String>) -> Self {
        Self {
            inner: ObjectStoreCache::new(store, prefix, DEFAULT_RESULT_CACHE_TTL),
            execution_options: ExecutionOptions::default(),
        }
    }

    #[must_use]
    pub fn with_ttl(mut self, ttl: Time) -> Self {
        self.inner = self.inner.with_ttl(ttl.to_std());
        self
    }

    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.inner = self.inner.with_clock(clock);
        self
    }

    pub async fn get(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
    ) -> Result<Option<AnnotatedQueryResult>, PromqlError> {
        QueryCache::get(self, &range_cache_key(tenant, query)).await
    }

    pub async fn insert(
        &self,
        tenant: &str,
        query: &FrontendRangeQuery,
        result: AnnotatedQueryResult,
    ) -> Result<(), PromqlError> {
        QueryCache::insert(self, &range_cache_key(tenant, query), &result).await
    }
}

impl<C> PromqlQueryFrontendCache<C> {
    #[must_use]
    pub fn with_execution_options(mut self, execution_options: ExecutionOptions) -> Self {
        self.execution_options = execution_options;
        self
    }
}

#[async_trait]
impl<C> QueryCache<AnnotatedQueryResult> for PromqlQueryFrontendCache<C>
where
    C: QueryCache<AnnotatedQueryResult>,
    C::Error: Display,
{
    type Error = PromqlError;

    async fn get(&self, key: &CacheKey) -> Result<Option<AnnotatedQueryResult>, Self::Error> {
        self.inner.get(key).await.map_err(cache_error)
    }

    async fn insert(
        &self,
        key: &CacheKey,
        result: &AnnotatedQueryResult,
    ) -> Result<(), Self::Error> {
        self.inner.insert(key, result).await.map_err(cache_error)
    }
}

/// A `PromQL` result cache that supplies shared fan-out policy.
pub trait RangeQueryCache:
    QueryCache<AnnotatedQueryResult, Error = PromqlError> + Send + Sync
{
    fn execution_options(&self) -> ExecutionOptions;
}

impl<C> RangeQueryCache for PromqlQueryFrontendCache<C>
where
    C: QueryCache<AnnotatedQueryResult>,
    C::Error: Display,
{
    fn execution_options(&self) -> ExecutionOptions {
        self.execution_options
    }
}

pub(super) fn range_cache_key(tenant: &str, query: &FrontendRangeQuery) -> CacheKey {
    CacheKey::new(
        serde_json::to_vec(&(
            tenant,
            &query.query,
            query.start_ms,
            query.end_ms,
            query.step.millis_i64(),
            query.shard.map(|shard| (shard.index, shard.total)),
        ))
        .expect("a range cache key contains only JSON primitives"),
    )
}

fn cache_error(error: impl Display) -> PromqlError {
    PromqlError::Store(format!("query frontend cache failed: {error}"))
}
