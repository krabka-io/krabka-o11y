use super::{Arc, PanicSafeShared, ReadinessGate, RoleKind};

/// The preconditions a role must meet before an orchestrator routes traffic
/// to it.
///
/// A role registers one gate per precondition it cannot serve correct answers
/// without -- the WAL tail is attached, the block index snapshot is loaded,
/// the object store answered -- and hands each gate to the component that
/// satisfies it. `/ready` is then a report of real startup state instead of a
/// constant.
///
/// A role with no gates is ready from construction. That is the honest answer
/// for a role whose whole startup runs before its listener binds: there is no
/// window in which it is listening and not yet able to serve.
///
/// # One process, several roles
///
/// An all-in-one process is ready when every role it runs is ready, and not
/// before: a `--target all` whose probe passed while its block builder was
/// still reaching the broker would take queries it cannot answer from a port
/// that looks healthy. [`for_role`](Self::for_role) is how that is arranged.
/// It hands a role a view onto the *same* gate list, so one `/ready` covers
/// the whole process, and stamps the role's name in front of every gate that
/// view registers. The probe then reads `not ready: block-builder/wal-consumer`
/// and names the role that is holding the process back, rather than reporting
/// a bare `wal-consumer` that three of the roles could have registered.
///
/// The gate list sits behind a [`PanicSafeShared`] rather than a plain
/// `Mutex`, because `/ready` is the one route every role serves and the one an
/// orchestrator believes. A panic anywhere else in the process that happened
/// to be holding a plain lock here would poison it, and the probe would then
/// fail for the life of the pod with no way back -- a role marked permanently
/// unready by a fault that had nothing to do with its readiness.
#[derive(Clone, Default)]
pub struct RoleReadiness {
    gates: Arc<PanicSafeShared<Vec<ReadinessGate>>>,
    role: Option<RoleKind>,
}

impl RoleReadiness {
    /// A role with no preconditions registered yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A view onto the same preconditions that names `role` in front of every
    /// gate it registers.
    ///
    /// The gates are shared, not copied: readiness stays one answer for the
    /// process, which is what a single `/ready` port can honestly report.
    ///
    /// Sharing has one consequence worth knowing before it is met. A role that
    /// probes another role's `/ready` -- the traces query-frontend probing its
    /// queriers is the one that does -- reads the whole process's answer, not
    /// that querier's. In one process that closes a loop: the frontend's own
    /// `querier-membership` gate is unmet, so the querier's port answers 503,
    /// so the membership never fills, so the gate stays unmet. `krabka-traces`
    /// breaks the loop by giving its all-in-one frontend a fixed membership
    /// and no probe loop. The durable answer is for a role's data port to
    /// report that role's gates while the admin port reports the process's,
    /// which this type does not yet separate.
    #[must_use]
    pub fn for_role(&self, role: RoleKind) -> Self {
        Self {
            gates: Arc::clone(&self.gates),
            role: Some(role),
        }
    }

    /// Registers a precondition, which starts unmet, and returns the handle
    /// that the component satisfying it holds.
    #[must_use]
    pub fn gate(&self, name: &str) -> ReadinessGate {
        let gate = match self.role {
            Some(role) => ReadinessGate::unmet(format!("{role}/{name}")),
            None => ReadinessGate::unmet(name),
        };
        self.gates.update(|gates| gates.push(gate.clone()));
        gate
    }

    /// Whether every registered precondition currently holds.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.pending().is_empty()
    }

    /// Whether a precondition called `name` is registered and still unmet,
    /// whichever role registered it.
    ///
    /// A role-scoped gate is reported as `<role>/<name>`, so a caller asking
    /// about one precondition by name -- the drain gate, above all -- has to
    /// ask about the last segment rather than the whole string. This is where
    /// that is done, once.
    #[must_use]
    pub fn is_pending(&self, name: &str) -> bool {
        self.pending()
            .iter()
            .any(|pending| pending.rsplit('/').next() == Some(name))
    }

    /// The names of the preconditions still unmet, in registration order.
    #[must_use]
    pub fn pending(&self) -> Vec<String> {
        self.gates.read(|gates| {
            gates
                .iter()
                .filter(|gate| !gate.is_ready())
                .map(|gate| gate.name().to_string())
                .collect()
        })
    }
}
