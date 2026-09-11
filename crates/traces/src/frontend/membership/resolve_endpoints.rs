use super::BTreeSet;

/// Re-resolve the configured querier endpoints into the addresses behind them
/// right now.
///
/// A flag holds names, not instances. `querier.traces.svc:3200` is one
/// headless Service in front of however many querier pods exist this minute,
/// and resolving it once at startup is what makes a scaled-up querier
/// unreachable until someone restarts the frontend. Resolving it every refresh
/// is what makes the new pod a member.
///
/// An endpoint that does not resolve is kept as itself rather than dropped.
/// Dropping it would remove it from the membership, and a querier that is
/// absent from the membership earns no warning -- exactly the silence this
/// exists to remove. Kept, it fails its probe and says so.
pub async fn resolve_endpoints(endpoints: &[String]) -> Vec<String> {
    let mut resolved = BTreeSet::new();
    for endpoint in endpoints {
        match tokio::net::lookup_host(endpoint.as_str()).await {
            Ok(addrs) => {
                let before = resolved.len();
                for addr in addrs {
                    resolved.insert(addr.to_string());
                }
                if resolved.len() == before {
                    resolved.insert(endpoint.clone());
                }
            }
            Err(error) => {
                tracing::warn!(%endpoint, %error, "querier endpoint did not resolve");
                resolved.insert(endpoint.clone());
            }
        }
    }
    resolved.into_iter().collect()
}
