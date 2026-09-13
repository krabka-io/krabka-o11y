#[derive(Clone, Copy, Debug, Eq, PartialEq)]
/// A Boolean operator between field-filter expressions.
pub enum FieldFilterLogicOp {
    And,
    Or,
}
