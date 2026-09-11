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

mod tempo_readiness_routes;

pub(crate) use tempo_readiness_routes::tempo_readiness_routes;
