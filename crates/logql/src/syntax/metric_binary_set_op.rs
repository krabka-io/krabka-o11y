use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetricBinarySetOp {
    And,
    Or,
    Unless,
}
