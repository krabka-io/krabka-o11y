use super::{
    Arc, AssignedJob, BackendError, BlockCatalog, FrontendConfig, JobShard, Membership,
    MembershipView, Metrics, MetricsJobRequest, MetricsResponseJson, QuerierBackend,
    SearchJobRequest, SearchPartial, SearchResponseJson, TagNamesJobRequest, TagNamesPartial,
    TagValuesJobRequest, TagValuesPartial, TenantId, TraceByIdJobRequest, TraceByIdResponseJson,
    TraceStatus, assign_jobs, catalog_error, job, merge, metrics_merge, pick_querier, queue,
};
use futures::StreamExt as _;
use tokio::sync::mpsc;

/// The query-frontend pipeline.
///
/// It runs plan jobs -> assign to ready queriers -> queue (bounded fan-out) ->
/// per-job search -> merge (limit/spss) -> render Tempo JSON. It sits in front
/// of a [`QuerierBackend`] transport, with a [`BlockCatalog`] for block
/// enumeration and a [`MembershipView`] for who may take work.
///
/// Every query takes **one** membership snapshot and uses it for planning,
/// assignment and collection. A fan-out that planned against one view and
/// collected against another could lose a shard with nothing left to blame it
/// on.
///
/// By-id does **not** fan per-block. The querier reassembles a trace across
/// blocks and exposes no block-scoped by-id. By-id instead queries every ready
/// querier and unions their v2 responses. That union is meaningful because
/// different queriers' live-stores hold different recent spans.
pub struct QueryFrontend<B: QuerierBackend, C: BlockCatalog> {
    pub(crate) backend: Arc<B>,
    pub(crate) catalog: Arc<C>,
    pub(crate) cfg: FrontendConfig,
    pub(crate) membership: MembershipView,
}

impl<B: QuerierBackend + 'static, C: BlockCatalog + 'static> QueryFrontend<B, C> {
    #[must_use]
    pub fn new(
        backend: Arc<B>,
        catalog: Arc<C>,
        cfg: FrontendConfig,
        membership: MembershipView,
    ) -> Self {
        Self {
            backend,
            catalog,
            cfg,
            membership,
        }
    }

    /// Test and inspection accessor for the backend, such as
    /// `MockQuerier::search_calls`.
    #[must_use]
    pub fn backend_ref(&self) -> &B {
        &self.backend
    }

    /// The querier pool this frontend fans out over.
    #[must_use]
    pub fn membership(&self) -> &MembershipView {
        &self.membership
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

    /// The membership snapshot this query will run against, or a transport
    /// error when nothing is ready.
    ///
    /// An empty pool is a failure, not an empty result. Answering `200 []`
    /// from no queriers at all is the most complete form of the silent loss
    /// this whole path is built to avoid.
    fn ready_pool(&self) -> Result<Arc<Membership>, BackendError> {
        let snapshot = self.membership.load();
        if snapshot.ready_count() == 0 {
            return Err(BackendError::Transport(format!(
                "no ready querier: {} known, none passing /ready",
                snapshot.members().len()
            )));
        }
        Ok(snapshot)
    }

    /// Warnings the client must see about queriers left out of this query.
    ///
    /// A cold block job is re-assignable: an excluded querier costs nothing,
    /// because the block is in object storage and another querier reads it.
    /// The hot tier is not. Each querier's live-store holds a different slice
    /// of the recent spans, so a querier that is out of the fan-out takes its
    /// slice with it, and no other querier can supply it. The warning is
    /// therefore raised exactly when the plan included a live shard.
    fn exclusion_warnings(snapshot: &Membership, planned_live: bool) -> Vec<String> {
        if planned_live {
            snapshot.exclusion_warnings()
        } else {
            Vec::new()
        }
    }

    /// The warning for a pool that changed while the query was in flight.
    ///
    /// Assignment is stable, so a cold job that already ran still covers its
    /// block. A live shard is different: it had to reach every holder of the
    /// hot tier, and a querier that joined after the snapshot was taken was
    /// never asked. Rather than re-running the fan-out against a pool that may
    /// change again, the answer says what it could not guarantee.
    fn generation_warning(&self, snapshot: &Membership, planned_live: bool) -> Option<String> {
        let now = self.membership.load();
        (planned_live && now.generation() != snapshot.generation()).then(|| {
            format!(
                "the querier pool changed while this query ran (generation {} -> {}); the live tier fan-out may not have covered every querier",
                snapshot.generation(),
                now.generation()
            )
        })
    }

    /// Plan the shards for a window, and place each on a ready querier.
    async fn plan_and_assign(
        &self,
        tenant: &TenantId,
        start_ns: i64,
        end_ns: i64,
        snapshot: &Membership,
    ) -> Result<(Vec<AssignedJob>, u64, bool), BackendError> {
        let blocks = self
            .catalog
            .blocks(tenant.as_str(), start_ns, end_ns)
            .await
            .map_err(|e| catalog_error(&e))?;
        let plan = job::plan_search_jobs(
            &blocks,
            end_ns,
            self.cfg.hot_frontier_ns,
            self.cfg.target_per_job,
        );
        let planned_live = plan.jobs.contains(&JobShard::Live);
        let assigned = assign_jobs(plan.jobs, &snapshot.ready_addrs());
        Ok((assigned, plan.total_blocks, planned_live))
    }

    /// Run a `TraceQL` `/api/search` through the full pipeline.
    ///
    /// Search shards **partition** the data across the live tier and disjoint
    /// cold blocks, so a failed shard means missing results. Any job error
    /// therefore propagates. An invalid query fails on every shard and must
    /// surface. It must not silently return an empty 200.
    ///
    /// A querier that is *excluded* before planning is a different case from
    /// one whose job failed. Its cold blocks go to another querier and cost
    /// nothing, but the hot tier it held cannot be recovered from anywhere, so
    /// the response carries a warning naming it.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn search(
        &self,
        tenant: &TenantId,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        spss: usize,
    ) -> Result<SearchResponseJson, BackendError> {
        let snapshot = self.ready_pool()?;
        let (assigned, total_blocks, planned_live) = self
            .plan_and_assign(tenant, start_ns, end_ns, &snapshot)
            .await?;
        let total_jobs = assigned.len() as u64;

        let backend = Arc::clone(&self.backend);
        let tenant = tenant.clone();
        let query_s = query.to_string();
        let results = queue::run_jobs(assigned, self.cfg.max_concurrency, move |job| {
            let backend = Arc::clone(&backend);
            let req = SearchJobRequest {
                tenant: tenant.clone(),
                query: query_s.clone(),
                start_ns,
                end_ns,
                limit,
                spss,
                shard: job.shard,
                querier: job.querier,
            };
            async move { backend.search_job(&req).await }
        })
        .await;
        let partials: Vec<SearchPartial> = results.into_iter().collect::<Result<_, _>>()?;

        let mut resp = merge::merge_search(partials, limit, spss);
        // Seed plan-derived totals (per-job metrics carry completed/bytes).
        resp.metrics.total_jobs = total_jobs;
        resp.metrics.total_blocks = total_blocks;
        resp.warnings = Self::exclusion_warnings(&snapshot, planned_live);
        resp.warnings
            .extend(self.generation_warning(&snapshot, planned_live));
        Ok(resp)
    }

    /// Stream one cumulative search response whenever a shard completes.
    pub async fn search_stream(
        &self,
        tenant: &TenantId,
        query: &str,
        start_ns: i64,
        end_ns: i64,
        limit: usize,
        spss: usize,
    ) -> Result<mpsc::Receiver<Result<SearchResponseJson, BackendError>>, BackendError> {
        let snapshot = self.ready_pool()?;
        let (assigned, total_blocks, planned_live) = self
            .plan_and_assign(tenant, start_ns, end_ns, &snapshot)
            .await?;
        let total_jobs = assigned.len() as u64;
        let backend = Arc::clone(&self.backend);
        let tenant = tenant.clone();
        let query = query.to_string();
        let concurrency = self.cfg.max_concurrency.max(1);
        let warnings = Self::exclusion_warnings(&snapshot, planned_live);
        let generation_warning = self.generation_warning(&snapshot, planned_live);
        let (tx, rx) = mpsc::channel(concurrency);
        tokio::spawn(async move {
            let mut jobs = futures::stream::iter(assigned)
                .map(|job| {
                    let backend = Arc::clone(&backend);
                    let request = SearchJobRequest {
                        tenant: tenant.clone(),
                        query: query.clone(),
                        start_ns,
                        end_ns,
                        limit,
                        spss,
                        shard: job.shard,
                        querier: job.querier,
                    };
                    async move { backend.search_job(&request).await }
                })
                .buffer_unordered(concurrency);
            let mut partials = Vec::new();
            while let Some(result) = jobs.next().await {
                match result {
                    Ok(partial) => partials.push(partial),
                    Err(error) => {
                        let _ = tx.send(Err(error)).await;
                        return;
                    }
                }
                let mut response = merge::merge_search(partials.clone(), limit, spss);
                response.metrics.total_jobs = total_jobs;
                response.metrics.total_blocks = total_blocks;
                response.warnings = warnings.clone();
                response.warnings.extend(generation_warning.clone());
                if tx.send(Ok(response)).await.is_err() {
                    return;
                }
            }
        });
        Ok(rx)
    }

    /// Run a `/api/v2/traces/{id}` by-id lookup, with one job per ready
    /// querier.
    ///
    /// By-id queriers are **redundant** for a trace's cold half. Each one
    /// reassembles it from object storage, and their live-stores differ only
    /// in recent spans. This method therefore tolerates per-querier failures.
    /// It assembles the trace from any successes, and an error propagates only
    /// when *every* querier failed.
    ///
    /// A querier missing from the fan-out still costs the recent spans only it
    /// held, so the returned status is `PARTIAL` and the warnings say which
    /// querier and why. Tempo's v2 envelope already carries both.
    ///
    /// # Errors
    /// Returns an error when every querier lookup fails.
    pub async fn trace_by_id(
        &self,
        tenant: &TenantId,
        trace_id: [u8; 16],
        start_ns: i64,
        end_ns: i64,
    ) -> Result<
        (
            Option<TraceByIdResponseJson>,
            Metrics,
            TraceStatus,
            Vec<String>,
        ),
        BackendError,
    > {
        let snapshot = self.ready_pool()?;
        let targets: Vec<String> = snapshot
            .ready_addrs()
            .into_iter()
            .map(ToString::to_string)
            .collect();
        let total_jobs = targets.len() as u64;

        let backend = Arc::clone(&self.backend);
        let tenant = tenant.clone();
        let results = queue::run_jobs(targets, self.cfg.max_concurrency, move |querier| {
            let backend = Arc::clone(&backend);
            let req = TraceByIdJobRequest {
                tenant: tenant.clone(),
                trace_id,
                start_ns,
                end_ns,
                querier,
            };
            async move { backend.trace_by_id_job(&req).await }
        })
        .await;

        let mut partials = Vec::new();
        let mut first_err = None;
        let mut failed = Vec::new();
        for r in results {
            match r {
                Ok(p) => partials.push(p),
                Err(e) => {
                    failed.push(e.to_string());
                    first_err.get_or_insert(e);
                }
            }
        }
        if partials.is_empty()
            && let Some(e) = first_err
        {
            return Err(e);
        }

        let (trace, mut metrics, mut status) = merge::assemble_trace(partials, self.cfg.max_trace);
        metrics.total_jobs = total_jobs;

        let mut warnings = Vec::new();
        if matches!(status, TraceStatus::Partial) {
            warnings.push("trace exceeds max size; returned partially".to_string());
        }
        // A by-id fan-out always reaches every ready querier, so the hot tier
        // is covered unless a querier is out of the pool or its job failed.
        warnings.extend(snapshot.exclusion_warnings());
        warnings.extend(
            failed
                .into_iter()
                .map(|e| format!("a querier lookup failed ({e}); spans only it held are missing")),
        );
        if !warnings.is_empty() {
            status = TraceStatus::Partial;
        }
        Ok((trace, metrics, status, warnings))
    }

    /// Run `/api/v2/search/tags`: fan over the assigned shards, then union and
    /// dedupe.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_names(
        &self,
        tenant: &TenantId,
        scope: Option<krabka_traceql::TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<krabka_traceql::ScopedTag>, Metrics, Vec<String>), BackendError> {
        let snapshot = self.ready_pool()?;
        let (assigned, total_blocks, planned_live) = self
            .plan_and_assign(tenant, start_ns, end_ns, &snapshot)
            .await?;
        let total_jobs = assigned.len() as u64;

        let backend = Arc::clone(&self.backend);
        let tenant = tenant.clone();
        let results = queue::run_jobs(assigned, self.cfg.max_concurrency, move |job| {
            let backend = Arc::clone(&backend);
            let req = TagNamesJobRequest {
                tenant: tenant.clone(),
                scope,
                start_ns,
                end_ns,
                shard: job.shard,
                querier: job.querier,
            };
            async move { backend.tag_names_job(&req).await }
        })
        .await;
        let partials: Vec<TagNamesPartial> = results.into_iter().collect::<Result<_, _>>()?;

        let (tags, mut metrics) = merge::merge_tag_names(partials);
        metrics.total_jobs = total_jobs;
        metrics.total_blocks = total_blocks;
        let mut warnings = Self::exclusion_warnings(&snapshot, planned_live);
        warnings.extend(self.generation_warning(&snapshot, planned_live));
        Ok((tags, metrics, warnings))
    }

    /// Run `/api/v2/search/tag/{tag}/values`: fan over assigned shards, then
    /// union and dedupe.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn tag_values(
        &self,
        tenant: &TenantId,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> Result<(Vec<krabka_traceql::TypedValue>, Metrics, Vec<String>), BackendError> {
        let snapshot = self.ready_pool()?;
        let (assigned, total_blocks, planned_live) = self
            .plan_and_assign(tenant, start_ns, end_ns, &snapshot)
            .await?;
        let total_jobs = assigned.len() as u64;

        let backend = Arc::clone(&self.backend);
        let tenant = tenant.clone();
        let tag_s = tag.to_string();
        let results = queue::run_jobs(assigned, self.cfg.max_concurrency, move |job| {
            let backend = Arc::clone(&backend);
            let req = TagValuesJobRequest {
                tenant: tenant.clone(),
                tag: tag_s.clone(),
                start_ns,
                end_ns,
                shard: job.shard,
                querier: job.querier,
            };
            async move { backend.tag_values_job(&req).await }
        })
        .await;
        let partials: Vec<TagValuesPartial> = results.into_iter().collect::<Result<_, _>>()?;

        let (values, mut metrics) = merge::merge_tag_values(partials);
        metrics.total_jobs = total_jobs;
        metrics.total_blocks = total_blocks;
        let mut warnings = Self::exclusion_warnings(&snapshot, planned_live);
        warnings.extend(self.generation_warning(&snapshot, planned_live));
        Ok((values, metrics, warnings))
    }

    /// Run a `TraceQL`-metrics query as a **single unsharded job** against one
    /// ready querier.
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
    /// The one querier is chosen by ownership of the query text, so repeated
    /// evaluations of the same query land on the same querier while the pool
    /// holds, and it is always one the membership has just seen ready.
    ///
    /// # Errors
    /// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
    pub async fn metrics_query(
        &self,
        tenant: &TenantId,
        query: &str,
        window: (i64, i64, i64),
        instant: bool,
        exemplar_limit: Option<usize>,
    ) -> Result<MetricsResponseJson, BackendError> {
        let snapshot = self.ready_pool()?;
        let querier = pick_querier(&snapshot.ready_addrs(), query)
            .ok_or_else(|| BackendError::Transport("no ready querier".to_string()))?
            .to_string();
        let (start_ns, end_ns, step_ns) = window;
        let req = MetricsJobRequest {
            tenant: tenant.clone(),
            query: query.to_string(),
            start_ns,
            end_ns,
            step_ns,
            instant,
            // `JobShard::Live` sends no scan restriction, so the querier scans its
            // full hot+cold union — the whole result in one job.
            shard: JobShard::Live,
            querier,
        };
        let mut series = self.backend.metrics_job(&req).await?.response.series;
        metrics_merge::limit_exemplars(&mut series, exemplar_limit);
        Ok(MetricsResponseJson { series })
    }
}
