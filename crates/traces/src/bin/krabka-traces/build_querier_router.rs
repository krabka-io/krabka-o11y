
#[cfg(test)]
pub(crate) async fn build_querier_router(
    cli: &Cli,
) -> Result<axum::Router, Box<dyn std::error::Error + Send + Sync>> {
    let (router, ..) = build_querier_router_with_live(cli, ServiceMetrics::new(), None).await?;
    Ok(router)
}
