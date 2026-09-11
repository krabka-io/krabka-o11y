//! What a role must have done before it can serve correct answers.
//!
//! A readiness probe that passes as soon as the listener binds tells the
//! orchestrator to route queries into a process that has not loaded its index
//! and has not caught up on the WAL. Such a process answers with silent gaps
//! rather than with an error, so the rollout looks healthy while the data is
//! wrong. [`RoleReadiness`] is the shared seam that lets each role say what it
//! is still waiting for.

use crate::{
    Arc, AtomicBool, AtomicOrdering, Extension, IntoResponse, PanicSafeShared, Response, RoleKind,
    Router, StatusCode, get,
};

mod draining_gate;
mod readiness_gate;
mod readiness_router;
mod ready;
mod role_readiness;
#[cfg(test)]
mod tests;

pub use draining_gate::DRAINING_GATE;
pub use readiness_gate::ReadinessGate;
pub use readiness_router::readiness_router;
pub use ready::ready;
pub use role_readiness::RoleReadiness;
