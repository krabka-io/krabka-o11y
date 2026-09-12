use krabka_blockstore::TenantPolicy;
use krabka_observability::RoleReadiness;

use super::{
    Arc, CancellationToken, Cli, ClientSecurity, DistributorState, KafkaSink, Mutex,
    OverridesProvider, Producer, RelabelConfig, ServiceMetrics, client_resource_policy,
};

/// Builds the distributor's shared state, connecting its WAL producer on the
/// way.
///
/// `None` is a stop, not a failure: `krabka-client-producer` retries an
/// unreachable bootstrap address rather than reporting it, so the build is
/// raced against `shutdown`. Without that race a distributor pointed at a
/// broker that is not there would be a process that no signal can end, which
/// an orchestrator can only resolve by killing it.
///
/// `overrides` is the one provider the process resolves every per-tenant limit
/// through, so the caller loads the overrides file and every role shares what it
/// read.
///
/// `wal_security` is the TLS and SASL of the producer. `None` connects in
/// plain text.
///
/// # Errors
/// Returns an error when the producer refuses the configured bootstrap address.
pub(crate) async fn build_distributor_state(
    cli: &Cli,
    metrics: &ServiceMetrics,
    readiness: &RoleReadiness,
    shutdown: &CancellationToken,
    overrides: OverridesProvider,
    wal_security: Option<ClientSecurity>,
) -> Result<Option<Arc<DistributorState>>, Box<dyn std::error::Error>> {
    let (client_dispatch_queue_capacity, client_frame_max) = client_resource_policy(cli);
    // Nowhere to put a push until the WAL producer has a broker.
    let wal_broker = readiness.gate("wal-broker");
    let producer = tokio::select! {
        biased;
        () = shutdown.cancelled() => return Ok(None),
        built = Producer::builder()
            .bootstrap(&cli.bootstrap)
            .maybe_security(wal_security)
            .dispatch_queue_capacity(client_dispatch_queue_capacity.get())
            .frame_max(client_frame_max.size())
            .build() => built?,
    };
    wal_broker.mark_ready();
    Ok(Some(Arc::new(DistributorState {
        sink: Arc::new(KafkaSink::with_topic(
            Arc::new(producer),
            cli.wal_topic.clone(),
        )),
        overrides,
        // Pyroscope keeps multi-tenancy off by default, so a push without a
        // tenant goes to `anonymous`.
        tenant_policy: TenantPolicy::anonymous(),
        active_series: Mutex::default(),
        cumulative_profiles: tokio::sync::Mutex::default(),
        ingestion_buckets: Mutex::default(),
        relabel: Vec::<RelabelConfig>::new(),
        max_decompressed: cli.distributor_request_max,
        max_tracked_tenants: cli.distributor_max_tracked_tenants,
        legacy_decode_limits: krabka_profiles::ingest::LegacyDecodeLimits {
            max_nodes: cli.legacy_max_nodes,
            max_path_bytes: cli.legacy_max_path_bytes,
            max_trie_depth: cli.legacy_max_trie_depth,
        },
        metrics: metrics.clone(),
    })))
}
