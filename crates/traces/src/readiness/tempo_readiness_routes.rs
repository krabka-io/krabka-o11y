use super::{Extension, RoleReadiness, Router, get};

/// The `/ready` and `/status` routes for a role's data port.
///
/// Merge the result into a Tempo-compatible router. `/status` is Tempo's alias
/// of `/ready`, and both paths answer with the same handler, so the two can
/// never disagree about what the role is still waiting for.
pub(crate) fn tempo_readiness_routes<S>(readiness: RoleReadiness) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/ready", get(krabka_observability::ready))
        .route("/status", get(krabka_observability::ready))
        .layer(Extension(readiness))
}
