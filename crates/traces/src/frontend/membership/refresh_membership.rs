use super::{QuerierMember, ReadinessProbe, resolve_endpoints};

/// Resolve `endpoints` and probe every address behind them, concurrently.
///
/// The result is the member list for the next generation: every querier the
/// names point at, each carrying what its probe said.
pub async fn refresh_membership(
    endpoints: &[String],
    probe: &dyn ReadinessProbe,
) -> Vec<QuerierMember> {
    let addrs = resolve_endpoints(endpoints).await;
    futures::future::join_all(addrs.into_iter().map(|addr| async move {
        let health = probe.probe(&addr).await;
        QuerierMember { addr, health }
    }))
    .await
}
