use super::*;

/// Prometheus 3.x range/vector selector modifier accepted after selectors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExtendedSelectorModifier {
    Anchored,
    Smoothed,
}

impl ExtendedSelectorModifier {
    #[must_use]
    pub fn keyword(self) -> &'static str {
        match self {
            Self::Anchored => "anchored",
            Self::Smoothed => "smoothed",
        }
    }
}
