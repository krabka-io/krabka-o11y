use super::QuerierHealth;

/// One querier the frontend knows about, and what its last probe said.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct QuerierMember {
    /// The `host:port` the frontend dials, after DNS re-resolution.
    pub addr: String,
    /// The result of the last readiness probe.
    pub health: QuerierHealth,
}

impl QuerierMember {
    /// A member whose last probe succeeded.
    #[must_use]
    pub fn ready(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            health: QuerierHealth::Ready,
        }
    }

    /// Whether this member may take a job.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.health.is_ready()
    }

    /// The response warning this member's exclusion earns, or `None` when it
    /// is in the fan-out.
    ///
    /// The wording names the address and the reason, because an operator
    /// reading it from a Grafana panel has neither.
    #[must_use]
    pub fn warning(&self) -> Option<String> {
        self.health.reason().map(|reason| {
            format!(
                "querier {} was excluded from the fan-out ({reason}); data only it holds is missing from this result",
                self.addr
            )
        })
    }
}
