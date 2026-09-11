#[cfg(test)]
use super::*;

#[cfg(test)]
pub(crate) async fn build_querier_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    let readiness = krabka_observability::RoleReadiness::new();
    let gates = BlockStoreGates::register(&readiness);
    let (router, ..) = build_querier_router_with_live(
        cli,
        ServiceMetrics::new(),
        None,
        &gates,
        readiness,
        &SharedObjectStore::new(),
    )
    .await?;
    Ok(router)
}
