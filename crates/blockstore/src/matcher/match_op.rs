use super::*;

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
