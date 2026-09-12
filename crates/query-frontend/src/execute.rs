use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{StreamExt as _, TryStreamExt as _, stream};

use crate::{CacheKey, Clock, QueryCache, SystemClock};

/// One independently executable query produced by a signal adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedQuery<Q> {
    pub query: Q,
    pub cache_key: CacheKey,
    pub end_epoch_millis: i64,
}

/// Shared fan-out and result-cache policy.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExecutionOptions {
    pub max_parallelism: NonZeroUsize,
    /// Retries after the first attempt.
    pub max_retries: usize,
    pub max_cache_freshness: Duration,
}

impl Default for ExecutionOptions {
    fn default() -> Self {
        Self {
            max_parallelism: NonZeroUsize::new(14).expect("14 is non-zero"),
            max_retries: 5,
            max_cache_freshness: Duration::from_mins(10),
        }
    }
}

/// Signal-specific planning, execution, retry classification, and merge behavior.
#[async_trait]
pub trait QueryFrontendAdapter: Sync {
    type Request: Sync + ?Sized;
    type Query: Send + Sync;
    type Output: Clone + Send + Sync;
    type Response;
    type Error: Send;

    /// # Errors
    /// Returns a signal-specific validation or planning error.
    fn plan(&self, request: &Self::Request) -> Result<Vec<PlannedQuery<Self::Query>>, Self::Error>;

    async fn execute(&self, query: &Self::Query) -> Result<Self::Output, Self::Error>;

    fn is_retryable(&self, error: &Self::Error) -> bool;

    /// Returns whether a successful adapter result is safe to cache.
    fn should_cache(&self, _result: &Self::Output) -> bool {
        true
    }

    /// # Errors
    /// Returns a signal-specific result merge error.
    fn merge(
        &self,
        request: &Self::Request,
        results: Vec<Self::Output>,
    ) -> Result<Self::Response, Self::Error>;
}

#[derive(Debug, thiserror::Error)]
pub enum QueryFrontendError<AdapterError, CacheError> {
    #[error("query adapter failed: {0}")]
    Adapter(AdapterError),
    #[error("query cache failed: {0}")]
    Cache(CacheError),
}

/// The single shared query-frontend pipeline used by signal adapters.
pub struct QueryFrontend<C> {
    cache: C,
    options: ExecutionOptions,
    clock: Arc<dyn Clock>,
}

impl<C> QueryFrontend<C> {
    #[must_use]
    pub fn new(cache: C, options: ExecutionOptions) -> Self {
        Self {
            cache,
            options,
            clock: Arc::new(SystemClock),
        }
    }

    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Plans, executes, and merges one request.
    ///
    /// Fan-out is bounded and results retain plan order. Only errors explicitly
    /// classified as transient by the adapter are retried.
    ///
    /// # Errors
    /// Returns the first planning, execution, cache, or merge error that cannot
    /// be recovered under the configured retry policy.
    pub async fn execute<A>(
        &self,
        adapter: &A,
        request: &A::Request,
    ) -> Result<A::Response, QueryFrontendError<A::Error, C::Error>>
    where
        A: QueryFrontendAdapter,
        C: QueryCache<A::Output>,
    {
        let planned = adapter.plan(request).map_err(QueryFrontendError::Adapter)?;
        let results = stream::iter(
            planned
                .into_iter()
                .map(|query| async move { self.execute_one(adapter, query).await }),
        )
        .buffered(self.options.max_parallelism.get())
        .try_collect()
        .await?;
        adapter
            .merge(request, results)
            .map_err(QueryFrontendError::Adapter)
    }

    async fn execute_one<A>(
        &self,
        adapter: &A,
        planned: PlannedQuery<A::Query>,
    ) -> Result<A::Output, QueryFrontendError<A::Error, C::Error>>
    where
        A: QueryFrontendAdapter,
        C: QueryCache<A::Output>,
    {
        if let Some(result) = self
            .cache
            .get(&planned.cache_key)
            .await
            .map_err(QueryFrontendError::Cache)?
        {
            return Ok(result);
        }

        let mut retries = 0;
        let result = loop {
            match adapter.execute(&planned.query).await {
                Ok(result) => break result,
                Err(error)
                    if retries < self.options.max_retries && adapter.is_retryable(&error) =>
                {
                    retries += 1;
                }
                Err(error) => return Err(QueryFrontendError::Adapter(error)),
            }
        };

        if adapter.should_cache(&result)
            && cacheable(
                planned.end_epoch_millis,
                self.clock.now_epoch_millis(),
                self.options.max_cache_freshness,
            )
        {
            self.cache
                .insert(&planned.cache_key, &result)
                .await
                .map_err(QueryFrontendError::Cache)?;
        }
        Ok(result)
    }
}

fn cacheable(end_epoch_millis: i64, now_epoch_millis: i64, freshness: Duration) -> bool {
    let freshness_ms = i64::try_from(freshness.as_millis()).unwrap_or(i64::MAX);
    end_epoch_millis <= now_epoch_millis.saturating_sub(freshness_ms)
}
