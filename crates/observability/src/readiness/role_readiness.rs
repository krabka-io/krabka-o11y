use super::{Arc, PanicSafeShared, ReadinessGate};

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
/// The gate list sits behind a [`PanicSafeShared`] rather than a plain
/// `Mutex`, because `/ready` is the one route every role serves and the one an
/// orchestrator believes. A panic anywhere else in the process that happened
/// to be holding a plain lock here would poison it, and the probe would then
/// fail for the life of the pod with no way back -- a role marked permanently
/// unready by a fault that had nothing to do with its readiness.
#[derive(Clone, Default)]
pub struct RoleReadiness {
    gates: Arc<PanicSafeShared<Vec<ReadinessGate>>>,
}

impl RoleReadiness {
    /// A role with no preconditions registered yet.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a precondition, which starts unmet, and returns the handle
    /// that the component satisfying it holds.
    #[must_use]
    pub fn gate(&self, name: &'static str) -> ReadinessGate {
        let gate = ReadinessGate::unmet(name);
        self.gates.update(|gates| gates.push(gate.clone()));
        gate
    }

    /// Whether every registered precondition currently holds.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.pending().is_empty()
    }

    /// The names of the preconditions still unmet, in registration order.
    #[must_use]
    pub fn pending(&self) -> Vec<&'static str> {
        self.gates.read(|gates| {
            gates
                .iter()
                .filter(|gate| !gate.is_ready())
                .map(ReadinessGate::name)
                .collect()
        })
    }
}
