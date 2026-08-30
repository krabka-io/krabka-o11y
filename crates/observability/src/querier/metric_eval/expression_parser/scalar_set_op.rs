#[derive(Clone, Copy)]
pub(crate) enum ScalarSetOp {
    And,
    Or,
    Unless,
}
