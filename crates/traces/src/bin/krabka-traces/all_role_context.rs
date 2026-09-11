use krabka_observability::RoleReadiness;

use super::{Cli, ProcessSecurity, ServiceMetrics, SharedObjectStore};

/// The five things every role of `--target all` needs, and one of each.
///
/// Each field is shared rather than per-role, and each for its own reason:
///
/// * `cli` is the process's configuration. A role that wants a different value
///   -- the querier's `--querier-live-store-url`, which only the composition
///   knows -- clones this and edits its own copy, so the edit is visible where
///   it is made instead of being a flag the operator has to set.
/// * `metrics` is one registry, because the admin port exports one `/metrics`.
/// * `readiness` is one gate list, so `/ready` is one answer for the process.
///   Roles take [`RoleReadiness::for_role`] views of it, which stamp the role's
///   name onto the gates they register: the probe then says
///   `not ready: block-builder/wal-consumer` rather than a bare `wal-consumer`
///   that four of the roles could have registered.
/// * `object_store` is one store. See [`SharedObjectStore`] for what four of
///   them would do to a `memory:///` deployment.
/// * `security` is one loaded posture, so every listener serves alike and one
///   audit trail records what all of them report. See
///   [`run_all`](super::run_all::run_all) for what that means for the
///   loopback ports.
#[derive(Clone)]
pub(crate) struct AllRoleContext {
    pub(crate) cli: Cli,
    pub(crate) metrics: ServiceMetrics,
    pub(crate) readiness: RoleReadiness,
    pub(crate) object_store: SharedObjectStore,
    pub(crate) security: ProcessSecurity,
}
