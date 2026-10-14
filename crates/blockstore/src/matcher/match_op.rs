use super::{Deserialize, Serialize};

/// Matcher operator.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MatchOp {
    /// `name="value"`
    Eq,
    /// `name!="value"`
    Neq,
    /// `name=~"regex"`
    Re,
    /// `name!~"regex"`
    Nre,
}

impl TryFrom<i32> for MatchOp {
    type Error = i32;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Eq),
            1 => Ok(Self::Neq),
            2 => Ok(Self::Re),
            3 => Ok(Self::Nre),
            other => Err(other),
        }
    }
}
