//! The readiness routes a Tempo-compatible data port serves.
//!
//! `krabka_observability::readiness_router` mounts `/ready` alone, which is
//! what a role's admin port needs. Tempo also answers `/status` on its query
//! port, and Grafana's Tempo datasource health check reads it, so a Krabka
//! query port has to answer both paths with the same body.
//!
//! Both routes here call `krabka_observability::ready`. The `not ready: <name>`
//! body therefore has one producer in the workspace, and
//! [`HttpReadinessProbe`](crate::frontend::HttpReadinessProbe) has one format
//! to parse.

use axum::{Extension, Router, routing::get};
use krabka_observability::RoleReadiness;

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
