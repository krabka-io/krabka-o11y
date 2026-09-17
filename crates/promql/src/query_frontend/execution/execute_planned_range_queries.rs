use async_trait::async_trait;
use krabka_query_frontend::{
    AdmissionLimits, PlannedQuery, QueryFrontend, QueryFrontendAdapter, QueryFrontendError,
};

use super::{
    AnnotatedQueryResult, Annotations, FrontendRangeQuery, PromqlError, QueryResult,
    RangeQueryCache, RangeQueryExecutor, TenantId, range_cache_key,
};

struct PromqlRangeAdapter<'a, E> {
    executor: &'a E,
    tenant: &'a TenantId,
    admission_limits: AdmissionLimits,
}

#[async_trait]
impl<E> QueryFrontendAdapter for PromqlRangeAdapter<'_, E>
where
    E: RangeQueryExecutor,
{
    type Request = [FrontendRangeQuery];
    type Query = FrontendRangeQuery;
    type Output = AnnotatedQueryResult;
    type Response = (Vec<QueryResult>, Annotations);
    type Error = PromqlError;

    fn plan(
        &self,
        queries: &[FrontendRangeQuery],
    ) -> Result<Vec<PlannedQuery<FrontendRangeQuery>>, PromqlError> {
        Ok(queries
            .iter()
            .cloned()
            .map(|query| PlannedQuery {
                cache_key: range_cache_key(self.tenant.as_str(), &query),
                end_epoch_millis: query.end_ms,
                estimated_bytes: 0,
                query,
            })
            .collect())
    }

    async fn execute(
        &self,
        query: &FrontendRangeQuery,
    ) -> Result<AnnotatedQueryResult, PromqlError> {
        self.executor.execute_range_query(self.tenant, query).await
    }

    fn is_retryable(&self, error: &PromqlError) -> bool {
        self.executor.is_transient_error(error)
    }

    fn admission_limits(&self, _queries: &[FrontendRangeQuery]) -> Option<AdmissionLimits> {
        Some(self.admission_limits)
    }

    fn merge(
        &self,
        _queries: &[FrontendRangeQuery],
        results: Vec<AnnotatedQueryResult>,
    ) -> Result<(Vec<QueryResult>, Annotations), PromqlError> {
        let mut annotations = Annotations::new();
        let mut query_results = Vec::with_capacity(results.len());
        for result in results {
            annotations.extend(&result.annotations);
            query_results.push(result.result);
        }
        Ok((query_results, annotations))
    }
}

/// Executes planned subqueries concurrently and merges annotations in plan order.
pub(crate) async fn execute_planned_range_queries<E, C>(
    executor: &E,
    cache: &C,
    tenant: &TenantId,
    planned: Vec<FrontendRangeQuery>,
    admission_limits: AdmissionLimits,
) -> Result<(Vec<QueryResult>, Annotations), PromqlError>
where
    E: RangeQueryExecutor,
    C: RangeQueryCache + ?Sized,
{
    let adapter = PromqlRangeAdapter {
        executor,
        tenant,
        admission_limits,
    };
    QueryFrontend::new(cache, cache.execution_options())
        .execute(&adapter, planned.as_slice())
        .await
        .map_err(|error| match error {
            QueryFrontendError::Adapter(error) | QueryFrontendError::Cache(error) => error,
            QueryFrontendError::Admission(error) => PromqlError::Overloaded {
                retry_after_seconds: match error {
                    krabka_query_frontend::AdmissionError::QueueFull {
                        retry_after_seconds,
                    }
                    | krabka_query_frontend::AdmissionError::RequestTooLarge {
                        retry_after_seconds,
                    } => retry_after_seconds,
                },
            },
        })
}
