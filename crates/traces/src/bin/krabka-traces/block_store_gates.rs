use krabka_observability::{ReadinessGate, RoleReadiness};

/// The two preconditions every role that reads blocks has to clear.
///
/// Both are registered before the work starts, so the admin port reports them
/// for the whole window between its own bind and the end of the start. A role
/// that registered a gate only after satisfying it would answer `ready` during
/// exactly the window the gate exists to cover.
pub(crate) struct BlockStoreGates {
    /// Met when the object store is configured.
    pub(crate) object_store: ReadinessGate,
    /// Met when the trace index snapshot is loaded.
    ///
    /// A querier that answers before this holds returns an empty result rather
    /// than an error, and the query-frontend in front of it cannot tell the two
    /// apart.
    pub(crate) trace_index: ReadinessGate,
}

impl BlockStoreGates {
    /// Registers both gates, unmet, in the order the start meets them.
    pub(crate) fn register(readiness: &RoleReadiness) -> Self {
        Self {
            object_store: readiness.gate("object-store"),
            trace_index: readiness.gate("trace-index"),
        }
    }
}
