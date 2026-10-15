use std::sync::Arc;

use axum::Router;
use krabka_blockstore::{
    ConditionalUpdateRequirement, MeteredObjectStore, ObjectStoreAccess, ObjectStoreMetrics,
    verify_object_store_access,
};
use krabka_observability::RoleReadiness;
use krabka_traces::frontend::{
    BlockCatalog, FrontendConfig, MembershipView, QuerierBackend, QueryFrontend,
};

use super::*;
use crate::live_span_batches::LiveStoreState;

pub(crate) fn build_live_store_router(
    cli: &Cli,
    live_store: Arc<RwLock<LiveStore>>,
    readiness: RoleReadiness,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    // This role serves the Tempo read API beside its internal span-batch
    // route, so its read gates resolve a tenant through the same provider the
    // querier builds.
    let overrides = load_traces_limits_overrides_config(
        cli.traces_limits_overrides_config.as_deref(),
        cli.traces_api_overrides_file.as_deref(),
        limits_from_cli(cli),
    )?;
    let trace_index: SharedTraceIndex = Arc::new(ArcSwap::from_pointee(TraceIndex::new()));
    let blocks = Arc::new(BlockStore::new(
        Arc::new(object_store::memory::InMemory::new()),
        Url::parse("memory:///")?,
    ));
    let live = LiveTier::new(Arc::new(IndexedLiveSource::new(
        Arc::clone(&live_store),
        Arc::clone(&trace_index),
    )));
    let store = Arc::new(KrabkaSpanStore::new_with_scan_concat_max(
        blocks,
        trace_index,
        Some(live),
        cli.scan_concat_max,
    ));
    let engine = Arc::new(TraceqlEngine::new(store, engine_opts_from_cli(cli)?));
    let cfg = HttpConfig {
        max_trace_spans: cli.max_trace_spans,
        tag_query_filter_autocomplete_limit: cli.tag_query_filter_autocomplete_limit,
        overrides,
        tenant_policy: TenantPolicy::anonymous(),
    };
    // The internal route resolves its tenant with the policy the Tempo routes
    // beside it use, so the two cannot disagree on a request without a tenant.
    let tenant_policy = cfg.tenant_policy.clone();
    let tempo_router = trace_querier::http::router_with_config(engine, cfg, readiness);
    let internal_router = axum::Router::new()
        .route(
            "/api/krabka/live/span-batches",
            axum::routing::get(live_span_batches),
        )
        .with_state(LiveStoreState {
            store: live_store,
            tenant_policy,
        });
    Ok(tempo_router.merge(internal_router))
}

/// Builds the traces object store, wrapped so every request it serves is
/// counted in `metrics`.
///
/// The wrap happens here and nowhere else. Every reader and writer in the role
/// gets its store from this function, `DataFusion` included, so one decorator
/// covers the whole role.
pub(crate) fn build_object_store(
    cli: &Cli,
    metrics: ObjectStoreMetrics,
) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
    let root = Url::parse(&cli.object_store_url)?;
    let parsed = object_store::parse_url_opts(&root, std::env::vars())?;
    let configured = ConfiguredObjectStore {
        store: MeteredObjectStore::wrap(Arc::from(parsed.0), metrics),
        root,
        prefix: parsed.1,
    };
    tracing::debug!(
        object_store_url = %configured.root,
        object_store_prefix = %configured.prefix,
        "configured traces object store"
    );
    Ok(configured)
}

#[cfg(test)]
pub(crate) async fn build_querier_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    let readiness = krabka_observability::RoleReadiness::new();
    let gates = BlockStoreGates::register(&readiness);
    let router_build = build_querier_router_with_live(
        cli,
        ServiceMetrics::new(),
        None,
        &gates,
        readiness,
        &SharedObjectStore::new(),
        &InternalClient::default(),
    )
    .await?;
    let router = router_build.router;
    Ok(
        krabka_observability::server_security::authenticate_requests(
            router,
            &ServerSecurity::default(),
        ),
    )
}

pub(crate) fn build_query_frontend_app<B, C>(
    backend: B,
    catalog: C,
    config: FrontendConfig,
    membership: MembershipView,
    readiness: krabka_observability::RoleReadiness,
) -> Router
where
    B: QuerierBackend + 'static,
    C: BlockCatalog + 'static,
{
    let frontend = Arc::new(QueryFrontend::new(
        Arc::new(backend),
        Arc::new(catalog),
        config,
        membership,
    ));
    krabka_traces::frontend::server::router_with_backend(frontend, readiness)
}

#[cfg(test)]
pub(crate) async fn build_query_frontend_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    use krabka_traces::frontend::{HttpQuerier, MembershipView};

    let addr: SocketAddr = cli.listen.parse()?;
    let cfg = frontend_config_from_cli(cli, addr)?;
    let catalog =
        build_trace_index_catalog(cli, &ServiceMetrics::new(), None, &SharedObjectStore::new())
            .await?;
    let backend = HttpQuerier::new(
        cfg.request_timeout.to_std(),
        cfg.querier_scheme,
        &InternalClient::default(),
    )?;
    // The router test double skips the probe loop: the configured endpoints
    // are the membership, all ready.
    let membership = MembershipView::fixed(cfg.querier_addrs.clone());
    Ok(
        krabka_observability::server_security::authenticate_requests(
            build_query_frontend_app(
                backend,
                catalog,
                cfg,
                membership,
                krabka_observability::RoleReadiness::new(),
            ),
            &ServerSecurity::default(),
        ),
    )
}

/// Build the production block catalog from the trace index when row-group
/// sharding is enabled, that is when `--target-bytes-per-job > 0`.
///
/// Otherwise this returns an empty catalog, which gives a whole-tier search
/// with no per-block fan-out.
///
/// `gates` is `Some` only in the sharding case, because that is the only case
/// that reads the object store or the index. A role with no gates to clear
/// reports nothing it does not do.
pub(crate) async fn build_trace_index_catalog(
    cli: &Cli,
    metrics: &ServiceMetrics,
    gates: Option<&BlockStoreGates>,
    object_store: &SharedObjectStore,
) -> Result<TraceIndexCatalog, Box<dyn std::error::Error + Send + Sync>> {
    if cli.target_bytes_per_job == ByteSize::from_bytes(0) {
        return Ok(TraceIndexCatalog::new(std::collections::BTreeMap::new()));
    }
    let configured = object_store
        .get(
            cli,
            metrics.object_store.clone(),
            ObjectStoreAccess::ReadOnly,
        )
        .await?;
    if let Some(gates) = gates {
        gates.object_store.mark_ready();
    }
    let trace_index_key = configured.object_key(&cli.trace_index_key);
    let trace_index = TraceIndex::load_latest_snapshot_or_empty_with_max_bytes(
        &configured.store,
        &trace_index_key,
        cli.index_snapshot_max,
    )
    .await?;
    if let Some(gates) = gates {
        gates.trace_index.mark_ready();
    }
    let blocks =
        BlockStore::new_with_block_read_max(configured.store, configured.root, cli.block_read_max);
    Ok(TraceIndexCatalog::from_trace_index(&blocks, &trace_index).await?)
}

#[derive(Clone)]
pub(crate) struct ConfiguredObjectStore {
    pub(crate) store: Arc<dyn ObjectStore>,
    pub(crate) root: Url,
    pub(crate) prefix: Path,
}

impl ConfiguredObjectStore {
    pub(crate) fn object_key(&self, key: &str) -> String {
        blockbuilder::prefixed_object_key(self.prefix.as_ref(), key)
    }
}

pub(crate) struct IndexedLiveSource {
    pub(crate) store: Arc<RwLock<LiveStore>>,
    pub(crate) trace_index: SharedTraceIndex,
}

impl IndexedLiveSource {
    pub(crate) fn new(store: Arc<RwLock<LiveStore>>, trace_index: SharedTraceIndex) -> Self {
        Self { store, trace_index }
    }
}

#[async_trait::async_trait]
impl LiveSource for IndexedLiveSource {
    async fn span_batches(
        &self,
        tenant: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> krabka_traces::querier::live::Result<Vec<arrow::record_batch::RecordBatch>> {
        let guard = self.store.read().await;
        guard.span_batches(tenant, start_ns, end_ns).await
    }

    async fn trace_spans(
        &self,
        tenant: &str,
        trace_id: &[u8; 16],
    ) -> krabka_traces::querier::live::Result<Option<krabka_traceql::TraceSpans>> {
        let guard = self.store.read().await;
        guard.trace_spans(tenant, trace_id).await
    }

    fn block_builder_frontier_ns(&self, tenant: &str) -> i64 {
        krabka_traces::querier::store::block_builder_frontier_ns(&self.trace_index, tenant)
    }
}

#[async_trait::async_trait]
impl TagCatalog for IndexedLiveSource {
    async fn tag_names(
        &self,
        tenant: &str,
        scope: Option<krabka_traceql::TagScope>,
        start_ns: i64,
        end_ns: i64,
    ) -> krabka_traces::querier::live::Result<Vec<krabka_traceql::ScopedTag>> {
        let guard = self.store.read().await;
        guard.tag_names(tenant, scope, start_ns, end_ns).await
    }

    async fn tag_values(
        &self,
        tenant: &str,
        tag: &str,
        start_ns: i64,
        end_ns: i64,
    ) -> krabka_traces::querier::live::Result<Vec<krabka_traceql::TypedValue>> {
        let guard = self.store.read().await;
        guard.tag_values(tenant, tag, start_ns, end_ns).await
    }
}

/// The one object store a process configures, however many roles ask for it.
///
/// Four of this binary's roles read or write blocks -- the block builder, the
/// querier, the query-frontend's catalog, and the compactor -- and each of
/// them used to call [`build_object_store`] for itself. For a single-role
/// process that is one call either way. For `--target all` it is four
/// independent stores, and with the default `--object-store-url memory:///`
/// that means four independent *heaps*: the block builder would write blocks
/// into one `InMemory` and the querier would search another that nothing ever
/// wrote to, so the process would start, pass every probe, and answer every
/// query with nothing. That failure has no symptom anywhere -- no error, no
/// warning, not even a missing object -- which is why the sharing is a type
/// rather than a convention.
///
/// The store is built on first use rather than up front. A distributor's or a
/// metrics-generator's startup does not touch object storage, and building an
/// S3 client for them would make a bad `--object-store-url`, or a credential
/// that role never needs, into a reason it will not start. Building it lazily
/// also keeps each role's readiness gates honest: the role still reaches its
/// store at the point in its own startup where it always did, and marks
/// `object-store` ready there, and only the second and later roles find the
/// work already done.
///
/// The [`ObjectStoreMetrics`] handle of whichever role asks first is the one
/// the decorator keeps. Every role in a process shares one registry, so this
/// changes no counter's value; it only means the request counts of all four
/// roles land on one set of series, which is what a single-process stack has
/// to report anyway.
#[derive(Clone)]
pub(crate) struct SharedObjectStore {
    cell: Arc<tokio::sync::OnceCell<ConfiguredObjectStore>>,
    read_verified: Arc<tokio::sync::OnceCell<()>>,
    read_write_verified: Arc<tokio::sync::OnceCell<()>>,
}

impl SharedObjectStore {
    /// A handle that has not built its store yet.
    pub(crate) fn new() -> Self {
        Self {
            cell: Arc::new(tokio::sync::OnceCell::new()),
            read_verified: Arc::new(tokio::sync::OnceCell::new()),
            read_write_verified: Arc::new(tokio::sync::OnceCell::new()),
        }
    }

    /// The process's object store, building it on the first call.
    ///
    /// The store is probed once for each `access` below the configured
    /// prefix, before the role that asked accepts any data. The block builder
    /// and the compactor ask for [`ObjectStoreAccess::ReadWrite`] and get
    /// [`verify_object_store_access`]'s full probe. The querier and the
    /// query-frontend ask for [`ObjectStoreAccess::ReadOnly`], which writes
    /// nothing, so they can run with a read-only credential. A process whose
    /// store already passed the full probe does not run the read probe.
    ///
    /// # Errors
    /// Returns whatever [`build_object_store`] returns: a `--object-store-url`
    /// that does not parse, or a backend that rejects the configuration it was
    /// given. Returns the probe's error when the store fails it. A failed build
    /// or probe is not remembered, so the next role to ask tries again rather
    /// than inheriting a poisoned cell.
    pub(crate) async fn get(
        &self,
        cli: &Cli,
        metrics: ObjectStoreMetrics,
        access: ObjectStoreAccess,
    ) -> Result<ConfiguredObjectStore, Box<dyn std::error::Error + Send + Sync>> {
        let configured = self
            .cell
            .get_or_try_init(|| async { build_object_store(cli, metrics) })
            .await?;
        let verified = match access {
            ObjectStoreAccess::ReadWrite => &self.read_write_verified,
            ObjectStoreAccess::ReadOnly if self.read_write_verified.initialized() => {
                return Ok(configured.clone());
            }
            ObjectStoreAccess::ReadOnly => &self.read_verified,
        };
        verified
            .get_or_try_init(|| {
                verify_object_store_access(
                    configured.store.as_ref(),
                    &configured.prefix,
                    access,
                    ConditionalUpdateRequirement::for_object_store_url(&cli.object_store_url),
                )
            })
            .await?;
        Ok(configured.clone())
    }
}
