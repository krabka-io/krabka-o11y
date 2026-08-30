use super::*;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum CompareGroup {
    Baseline,
    Selection,
}

impl CompareGroup {
    pub(crate) fn meta_type(self) -> &'static str {
        match self {
            Self::Baseline => "baseline",
            Self::Selection => "selection",
        }
    }

    pub(crate) fn total_meta_type(self) -> &'static str {
        match self {
            Self::Baseline => "baseline_total",
            Self::Selection => "selection_total",
        }
    }
}
