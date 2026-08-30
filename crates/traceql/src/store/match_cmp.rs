use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MatchCmp {
    Eq,
    Neq,
    Lt,
    Lte,
    Gt,
    Gte,
    Re,
    Nre,
}
