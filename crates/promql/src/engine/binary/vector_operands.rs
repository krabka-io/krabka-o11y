use super::{BinModifier, BinaryOp, InstantSample, InstantValue};

/// The two instant vectors of a vector-to-vector binary operation.
pub(crate) struct VectorOperands {
    /// The left-hand vector.
    pub(crate) left: Vec<InstantSample>,
    /// The right-hand vector.
    pub(crate) right: Vec<InstantSample>,
}

/// The operator of a vector-to-vector binary operation and how it matches.
#[derive(Clone, Copy)]
pub(crate) struct VectorMatching<'a> {
    /// The binary operator.
    pub(crate) op: BinaryOp,
    /// The `on`/`ignoring`, grouping, `bool`, and fill modifiers, if any.
    pub(crate) modifier: Option<&'a BinModifier>,
}

/// The two evaluated operands of an instant binary expression.
pub(crate) struct InstantOperands {
    /// The left-hand operand.
    pub(crate) lhs: InstantValue,
    /// The right-hand operand.
    pub(crate) rhs: InstantValue,
}
