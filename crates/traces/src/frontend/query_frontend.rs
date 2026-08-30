use super::{QuerierBackend, BlockCatalog, Arc, FrontendConfig, SearchResponseJson, BackendError, catalog_error, job, queue, SearchJobRequest, SearchPartial, merge, TraceByIdResponseJson, Metrics, TraceStatus, TraceByIdJobRequest, TagNamesJobRequest, TagNamesPartial, TagValuesJobRequest, TagValuesPartial, MetricsResponseJson, MetricsJobRequest, JobShard, metrics_merge};

/// The query-frontend pipeline.
///
/// It runs plan jobs -> queue (bounded fan-out) -> per-job search ->
/// merge (limit/spss) -> render Tempo JSON. It sits in front of a
/// [`QuerierBackend`] pool, with a [`BlockCatalog`] for block enumeration.
///
/// By-id does **not** fan per-block. The querier reassembles a trace across
/// blocks and exposes no block-scoped by-id. By-id instead queries every
/// querier in the pool and unions their v2 responses. That union is meaningful
/// because different queriers' live-stores may hold different recent spans.
pub struct QueryFrontend<B: QuerierBackend, C: BlockCatalog> {
    pub(crate) backend: Arc<B>,
    pub(crate) catalog: Arc<C>,
    pub(crate) cfg: FrontendConfig,
}

impl<B: QuerierBackend + 'static, C: BlockCatalog + 'static> QueryFrontend<B, C> {
    #[must_use]
    pub fn new(backend: Arc<B>, catalog: Arc<C>, cfg: FrontendConfig) -> Self {
        Self {
            backend,
            catalog,
            cfg,
        }
    }

    /// Test and inspection accessor for the backend, such as
    /// `MockQuerier::search_calls`.
    #[must_use]
    pub fn backend_ref(&self) -> &B {
        &self.backend
    }

    /// The configured default trace limit.
    #[must_use]
    pub fn default_limit(&self) -> usize {
        self.cfg.default_limit
    }

    /// The configured default spans-per-spanSet.
    #[must_use]
    pub fn default_spss(&self) -> usize {
        self.cfg.default_spss
    }

    /// Run a `TraceQL` `/api/search` through the full pipeline.
    ///
    /// Search shards **partition** the data across the live tier and disjoint
    /// cold blocks, so a failed shard means missing results. Any job error
    /// therefore propagates. An invalid query fails on every shard and must
    /// surface. It must not silently return an empty 200.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn search(
        &self,
        tenant: &str,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        spss: usize,
    ) -> Result<SearchResponseJson, BackendError> {
        let blocks = self
            .catalog
            .blocks(tenant, start_ns, end_ns)
            .await
            .map_err(|e| catalog_error(&e))?;
        let plan = job::plan_search_jobs(
            &blocks,
            end_ns,
            self.cfg.hot_frontier_ns,
            self.cfg.target_per_job,
        );
        let total_jobs = plan.jobs.len() as u64;
        let total_blocks = plan.total_blocks;

        let backend = Arc::clone(&self.backend);
        let tenant_s = tenant.to_string();
        let query_s = query.to_string();
        let results = queue::run_jobs(plan.jobs, self.cfg.max_concurrency, move |shard| {
            let backend = Arc::clone(&backend);
            let req = SearchJobRequest {
                tenant: tenant_s.clone(),
                query: query_s.clone(),
                start_ns,
                end_ns,
                limit,
                spss,
                shard,
            };
            async move { backend.search_job(&req).await }
        })
        .await;
        let partials: Vec<SearchPartial> = results.into_iter().collect::<Result<_, _>>()?;

        let mut resp = merge::merge_search(partials, limit, spss);
        // Seed plan-derived totals (per-job metrics carry completed/bytes).
        resp.metrics.total_jobs = total_jobs;
        resp.metrics.total_blocks = total_blocks;
        Ok(resp)
    }

    /// Run a `/api/v2/traces/{id}` by-id lookup, with one job per querier.
    ///
    /// By-id queriers are **redundant** for a trace. Each one reassembles the
    /// trace from object storage, and their live-stores differ only in recent
    /// spans. This method therefore tolerates per-querier failures. It
    /// assembles the trace from any successes, and an error propagates only
    /// when *every* querier failed.
    ///
    /// # Errors
    /// Returns an error when every querier lookup fails.
    pub async fn trace_by_id(
        &self,
        tenant: &str,
        trace_id: [u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Option<TraceByIdResponseJson>, Metrics, TraceStatus), BackendError> {
        let queriers = self.backend.querier_count().max(1);
        let jobs: Vec<usize> = (0..queriers).collect();
        let total_jobs = jobs.len() as u64;

        let backend = Arc::clone(&self.backend);
        let tenant_s = tenant.to_string();
        let results = queue::run_jobs(jobs, self.cfg.max_concurrency, move |idx| {
            let backend = Arc::clone(&backend);
            let req = TraceByIdJobRequest {
                tenant: tenant_s.clone(),
                trace_id,
                start_ns,
                end_ns,
                querier: Some(idx),
            };
            async move { backend.trace_by_id_job(&req).await }
        })
        .await;

        let mut partials = Vec::new();
        let mut first_err = None;
        for r in results {
            match r {
                Ok(p) => partials.push(p),
                Err(e) => {
                    first_err.get_or_insert(e);
                }
            }
        }
        if partials.is_empty()
            && let Some(e) = first_err
        {
            return Err(e);
        }

        let (trace, mut metrics, status) = merge::assemble_trace(partials, self.cfg.max_trace);
        metrics.total_jobs = total_jobs;
        Ok((trace, metrics, status))
    }

    /// Run `/api/v2/search/tags`: fan over the planned shards, then union and
    /// dedupe.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<krabka_traceql::TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<krabka_traceql::ScopedTag>, Metrics), BackendError> {
        let blocks = self
            .catalog
            .blocks(tenant, start_ns, end_ns)
            .await
            .map_err(|e| catalog_error(&e))?;
        let plan = job::plan_search_jobs(
            &blocks,
            end_ns,
            self.cfg.hot_frontier_ns,
            self.cfg.target_per_job,
        );
        let total_jobs = plan.jobs.len() as u64;
        let total_blocks = plan.total_blocks;

        let backend = Arc::clone(&self.backend);
        let tenant_s = tenant.to_string();
        let results = queue::run_jobs(plan.jobs, self.cfg.max_concurrency, move |shard| {
            let backend = Arc::clone(&backend);
            let req = TagNamesJobRequest {
                tenant: tenant_s.clone(),
                scope,
                start_ns,
                end_ns,
                shard,
            };
            async move { backend.tag_names_job(&req).await }
        })
        .await;
        let partials: Vec<TagNamesPartial> = results.into_iter().collect::<Result<_, _>>()?;

        let (tags, mut metrics) = merge::merge_tag_names(partials);
        metrics.total_jobs = total_jobs;
        metrics.total_blocks = total_blocks;
        Ok((tags, metrics))
    }

    /// Run `/api/v2/search/tag/{tag}/values`: fan over shards, then union and
    /// dedupe.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<krabka_traceql::TypedValue>, Metrics), BackendError> {
        let blocks = self
            .catalog
            .blocks(tenant, start_ns, end_ns)
            .await
            .map_err(|e| catalog_error(&e))?;
        let plan = job::plan_search_jobs(
            &blocks,
            end_ns,
            self.cfg.hot_frontier_ns,
            self.cfg.target_per_job,
        );
        let total_jobs = plan.jobs.len() as u64;
        let total_blocks = plan.total_blocks;

        let backend = Arc::clone(&self.backend);
        let tenant_s = tenant.to_string();
        let tag_s = tag.to_string();
        let results = queue::run_jobs(plan.jobs, self.cfg.max_concurrency, move |shard| {
            let backend = Arc::clone(&backend);
            let req = TagValuesJobRequest {
                tenant: tenant_s.clone(),
                tag: tag_s.clone(),
                start_ns,
                end_ns,
                shard,
            };
            async move { backend.tag_values_job(&req).await }
        })
        .await;
        let partials: Vec<TagValuesPartial> = results.into_iter().collect::<Result<_, _>>()?;

        let (values, mut metrics) = merge::merge_tag_values(partials);
        metrics.total_jobs = total_jobs;
        metrics.total_blocks = total_blocks;
        Ok((values, metrics))
    }

    /// Run a `TraceQL`-metrics query as a **single unsharded job** against one
    /// querier.
    ///
    /// The query is `/api/metrics/query_range` or `query`.
    ///
    /// Metrics are NOT sharded across blocks, on purpose. The per-shard
    /// *reduced* results are not safely mergeable. A sum over them
    /// double-counts every cold block, because the no-restriction "live" job
    /// already scans cold-before-frontier and live, which overlaps the
    /// per-block jobs. A sum is also plain wrong for the non-additive
    /// aggregates `min`, `max`, `avg` and `quantile_over_time`.
    ///
    /// A single unrestricted job lets one querier compute the full hot and cold
    /// union correctly for every aggregate. This method applies only exemplar
    /// limiting.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn metrics_query(
        &self,
        tenant: &str,
        query: &str,
        window: (i64, i64, i64),
        instant: bool,
        exemplar_limit: Option<usize>,
    ) -> Result<MetricsResponseJson, BackendError> {
        let (start_ns, end_ns, step_ns) = window;
        let req = MetricsJobRequest {
            tenant: tenant.to_string(),
            query: query.to_string(),
            start_ns,
            end_ns,
            step_ns,
            instant,
            // `JobShard::Live` sends no scan restriction, so the querier scans its
            // full hot+cold union — the whole result in one job.
            shard: JobShard::Live,
        };
        let mut series = self.backend.metrics_job(&req).await?.response.series;
        metrics_merge::limit_exemplars(&mut series, exemplar_limit);
        Ok(MetricsResponseJson { series })
    }
}
