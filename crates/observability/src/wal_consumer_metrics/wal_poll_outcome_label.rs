use super::{EncodeLabelSet, WalPollOutcome};

/// The `outcome` label the poll counter carries.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct WalPollOutcomeLabel {
    pub outcome: &'static str,
}

impl From<WalPollOutcome> for WalPollOutcomeLabel {
    fn from(outcome: WalPollOutcome) -> Self {
        Self {
            outcome: outcome.as_str(),
        }
    }
}
