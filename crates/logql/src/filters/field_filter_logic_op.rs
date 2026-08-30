use super::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FieldFilterLogicOp {
    And,
    Or,
}
