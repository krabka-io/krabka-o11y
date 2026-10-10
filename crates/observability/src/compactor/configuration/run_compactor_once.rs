use super::{
    BlockDescriptor, CompactorBatches, CompactorRun, ObjectStore, ServiceConfig,
    ServiceDependencies, ServiceRuntimeError,
};

/// # Errors
/// Returns an error when telemetry input is malformed, a query cannot be evaluated, or the configured storage or export backend fails.
pub async fn run_compactor_once(
    config: &ServiceConfig,
    dependencies: ServiceDependencies,
    object_store: Option<&dyn ObjectStore>,
) -> Result<Option<BlockDescriptor>, ServiceRuntimeError> {
    Ok(CompactorRun {
        config,
        dependencies,
        object_store,
    }
    .run(CompactorBatches::Next)
    .await?
    .into_iter()
    .next())
}
