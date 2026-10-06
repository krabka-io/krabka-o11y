use krabka_domain_macros::EnumName;

/// What one call to the consumer's poll returned.
///
/// The three outcomes are the three states an operator has to tell apart, and
/// two of them look identical without this counter: a consumer that is caught
/// up and a consumer whose task has stopped both have a
/// `last_consumed_offset` that does not move.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, EnumName)]
pub enum WalPollOutcome {
    /// The poll returned at least one record.
    #[name(value = "records")]
    Records,
    /// The poll returned nothing before its timeout. The consumer is caught
    /// up, and it is still running.
    #[name(value = "empty")]
    Empty,
    /// The poll failed.
    #[name(value = "error")]
    Error,
}
