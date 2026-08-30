use super::{Serialize, Deserialize};

/// Counter-reset semantics carried with each histogram sample.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResetHint {
    Unknown,
    Yes,
    No,
    Gauge,
}

impl ResetHint {
    #[must_use]
    pub fn as_i8(self) -> i8 {
        match self {
            Self::Unknown => 0,
            Self::Yes => 1,
            Self::No => 2,
            Self::Gauge => 3,
        }
    }

    #[must_use]
    pub fn from_i8(value: i8) -> Self {
        match value {
            1 => Self::Yes,
            2 => Self::No,
            3 => Self::Gauge,
            _ => Self::Unknown,
        }
    }
}
