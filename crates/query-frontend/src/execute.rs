use std::{num::NonZeroUsize, sync::Arc, time::Duration};

use async_trait::async_trait;
use futures::{StreamExt as _, TryStreamExt as _, stream};

use crate::{
    AdmissionController, AdmissionError, AdmissionLimits, CacheKey, Clock, QueryCache, SystemClock,
};

/// One independently executable query produced by a signal adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlannedQuery<Q> {
    pub query: Q,
    pub cache_key: CacheKey,
    pub end_epoch_millis: i64,
    /// Conservative backend bytes reserved while this subquery runs.
    pub estimated_bytes: usize,
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

    /// The tenant override resolved for this request.
    fn admission_limits(&self, _request: &Self::Request) -> Option<AdmissionLimits> {
        None
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
    #[error("query admission failed: {0}")]
    Admission(AdmissionError),
}

/// The single shared query-frontend pipeline used by signal adapters.
pub struct QueryFrontend<C> {
    cache: C,
    options: ExecutionOptions,
    clock: Arc<dyn Clock>,
    admission: Arc<AdmissionController>,
}

impl<C> QueryFrontend<C> {
    #[must_use]
    pub fn new(cache: C, options: ExecutionOptions) -> Self {
        Self {
            cache,
            options,
            clock: Arc::new(SystemClock),
            admission: AdmissionController::global(),
        }
    }

    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    #[must_use]
    pub fn with_admission(mut self, admission: Arc<AdmissionController>) -> Self {
        self.admission = admission;
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
        let cache_keys = planned
            .iter()
            .map(|query| query.cache_key.clone())
            .collect::<Vec<_>>();
        let cached = stream::iter(cache_keys.into_iter().map(|cache_key| async move {
            self.cache
                .get(&cache_key)
                .await
                .map_err(QueryFrontendError::Cache)
        }))
        .buffered(self.options.max_parallelism.get())
        .try_collect::<Vec<_>>()
        .await?;
        let mut missing = planned
            .into_iter()
            .enumerate()
            .filter_map(|(index, query)| cached[index].is_none().then_some((index, query)))
            .collect::<Vec<_>>();
        let mut results = cached
            .into_iter()
            .enumerate()
            .filter_map(|(index, result)| result.map(|result| (index, result)))
            .collect::<Vec<_>>();
        let limits = adapter.admission_limits(request);
        let fallback_bytes = limits.map_or(1, |limits| limits.estimated_bytes_per_subquery);
        while !missing.is_empty() {
            let batch_len = missing.len().min(self.options.max_parallelism.get());
            let batch = missing.drain(..batch_len).collect::<Vec<_>>();
            let tenant = batch[0].1.cache_key.tenant().to_owned();
            let estimated_bytes = batch
                .iter()
                .map(|(_, query)| query.estimated_bytes.max(fallback_bytes))
                .fold(0_usize, usize::saturating_add);
            let permit = match limits {
                Some(limits) => {
                    self.admission
                        .acquire_with_limits(tenant, batch.len(), estimated_bytes, limits)
                        .await
                }
                None => {
                    self.admission
                        .acquire(tenant, batch.len(), estimated_bytes)
                        .await
                }
            }
            .map_err(QueryFrontendError::Admission)?;
            let executed = stream::iter(batch.into_iter().map(|(index, query)| async move {
                self.execute_uncached(adapter, query)
                    .await
                    .map(|result| (index, result))
            }))
            .buffer_unordered(self.options.max_parallelism.get())
            .try_collect::<Vec<_>>()
            .await?;
            drop(permit);
            results.extend(executed);
        }
        results.sort_unstable_by_key(|(index, _)| *index);
        let results = results.into_iter().map(|(_, result)| result).collect();
        adapter
            .merge(request, results)
            .map_err(QueryFrontendError::Adapter)
    }

    async fn execute_uncached<A>(
        &self,
        adapter: &A,
        planned: PlannedQuery<A::Query>,
    ) -> Result<A::Output, QueryFrontendError<A::Error, C::Error>>
    where
        A: QueryFrontendAdapter,
        C: QueryCache<A::Output>,
    {
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
