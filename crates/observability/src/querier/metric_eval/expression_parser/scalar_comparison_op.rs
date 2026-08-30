use super::*;

#[derive(Clone, Copy)]
pub(crate) enum ScalarComparisonOp {
    Equal,
    NotEqual,
    Greater,
    GreaterOrEqual,
    Less,
    LessOrEqual,
}
