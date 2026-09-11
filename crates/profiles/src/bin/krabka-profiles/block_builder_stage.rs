use super::{
    AllStage, Arc, Cli, ClientSecurity, ObjectStore, ServiceMetrics, block_builder_config,
};

/// The block builder, as `--target all` runs it.
///
/// It is given the process's one object store rather than building its own, so
/// that the blocks it writes are the blocks the read path reads. With an
/// in-memory `--object-store-url` a second store would be a second universe:
/// the builder would report blocks written, the querier would report no
/// profiles matched, and neither would be wrong.
pub(crate) fn block_builder_stage(
    cli: &Arc<Cli>,
    store: &Arc<dyn ObjectStore>,
    index_key: &str,
    metrics: &ServiceMetrics,
    wal_security: Option<ClientSecurity>,
) -> AllStage {
    let cli = Arc::clone(cli);
    let store = Arc::clone(store);
    let index_key = index_key.to_owned();
    let metrics = metrics.clone();
    Box::new(move |token| {
        Box::pin(async move {
            let config = block_builder_config(&cli, store, index_key, metrics, wal_security);
            if let Err(error) = krabka_profiles::blockbuilder::run_with_config(config, token).await
            {
                tracing::error!(%error, "profiles block builder stopped");
            }
        })
    })
}
