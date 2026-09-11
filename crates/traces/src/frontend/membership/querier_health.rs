/// What a querier's last readiness probe said.
///
/// The three cases are distinct on purpose. A querier that answers `/ready`
/// with a 503 naming its unmet gates is starting up or has fallen behind, and
/// will come back. One that does not answer at all is gone, or the network to
/// it is. Both are out of the fan-out, and both say why in the response
/// warnings, which is the difference between an ejection and a silent loss.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum QuerierHealth {
    /// `/ready` answered 2xx: every gate the role registered is met.
    Ready,
    /// `/ready` answered 503 and named the gates still unmet, as
    /// `krabka_observability::RoleReadiness` renders them.
    NotReady {
        /// The comma-separated gate names the querier reported, or
        /// `unnamed gate` when it reported none.
        pending: String,
    },
    /// The probe did not complete, or answered something other than 2xx/503.
    Unreachable {
        /// The transport or status detail, for the operator reading a warning.
        error: String,
    },
}

impl QuerierHealth {
    /// Whether this querier may take a job.
    #[must_use]
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }

    /// Why this querier is out of the fan-out, or `None` when it is in it.
    #[must_use]
    pub fn reason(&self) -> Option<String> {
        match self {
            Self::Ready => None,
            Self::NotReady { pending } => Some(format!("not ready: {pending}")),
            Self::Unreachable { error } => Some(format!("unreachable: {error}")),
        }
    }
}
