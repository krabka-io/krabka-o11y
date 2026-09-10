use super::ResetHint;

/// Which counter-reset hints an aggregation group has taken in.
///
/// A group that has seen a histogram stating a counter reset AND one stating
/// there was none is summing samples that disagree about their own history, and
/// Prometheus warns about the result.
#[derive(Debug, Default)]
pub(crate) struct CounterResetHints {
    reset_seen: bool,
    not_reset_seen: bool,
}

impl CounterResetHints {
    /// Records one sample's stated hint. An unknown or gauge hint states
    /// nothing and cannot collide.
    pub(crate) fn observe(&mut self, hint: ResetHint) {
        match hint {
            ResetHint::Yes => self.reset_seen = true,
            ResetHint::No => self.not_reset_seen = true,
            ResetHint::Unknown | ResetHint::Gauge => {}
        }
    }

    /// Whether the group has taken in both a stated reset and a stated non-reset.
    pub(crate) fn collide(&self) -> bool {
        self.reset_seen && self.not_reset_seen
    }
}
