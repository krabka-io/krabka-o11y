use derive_more::Display;

/// What one call to the consumer's poll returned.
///
/// The three outcomes are the three states an operator has to tell apart, and
/// two of them look identical without this counter: a consumer that is caught
/// up and a consumer whose task has stopped both have a
/// `last_consumed_offset` that does not move.
#[derive(Debug, Clone, Copy, Display, PartialEq, Eq, Hash)]
pub enum WalPollOutcome {
    /// The poll returned at least one record.
    #[display("records")]
    Records,
    /// The poll returned nothing before its timeout. The consumer is caught
    /// up, and it is still running.
    #[display("empty")]
    Empty,
    /// The poll failed.
    #[display("error")]
    Error,
}

impl WalPollOutcome {
    /// The label value for this outcome.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::Empty => "empty",
            Self::Error => "error",
        }
    }
}
