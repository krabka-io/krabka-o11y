use super::{Arc, AtomicBool, AtomicOrdering};

/// One precondition of a role's readiness, held by whatever satisfies it.
///
/// A gate starts unmet. The component that owns the precondition -- the WAL
/// tail task, the index loader, the compaction-frontier refresher -- marks it
/// ready when it has done its work, and marks it unready again if it loses
/// what it had. A background task that dies or falls behind therefore takes
/// its role out of rotation without stopping the process, which is the
/// difference between readiness and liveness.
///
/// The handle is cheap to clone and every clone names the same precondition.
///
/// The name is an [`Arc<str>`] rather than a `&'static str` because an
/// all-in-one process registers the same precondition once per role, and
/// `not ready: object-store, object-store` names neither of them. A gate taken
/// from a role-scoped [`RoleReadiness`](super::RoleReadiness) carries the
/// role in front of the name instead: `block-builder/object-store`.
#[derive(Clone)]
pub struct ReadinessGate {
    name: Arc<str>,
    ready: Arc<AtomicBool>,
}

impl ReadinessGate {
    pub(crate) fn unmet(name: impl Into<Arc<str>>) -> Self {
        Self {
            name: name.into(),
            ready: Arc::new(AtomicBool::new(false)),
        }
    }

    /// The precondition's name, as `/ready` reports it while it is unmet.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Records that the precondition now holds.
    pub fn mark_ready(&self) {
        self.ready.store(true, AtomicOrdering::SeqCst);
    }

    /// Records that the precondition no longer holds.
    pub fn mark_unready(&self) {
        self.ready.store(false, AtomicOrdering::SeqCst);
    }

    /// Whether the precondition currently holds.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.ready.load(AtomicOrdering::SeqCst)
    }

    /// The gate's flag, for an existing API that already takes a shared bool
    /// and flips it as its connection comes and goes.
    #[must_use]
    pub fn shared_flag(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.ready)
    }
}
