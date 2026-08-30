use super::*;

/// Returns `true` when the operator path can carry one binary operand.
///
/// The planner folds a scalar operand through the pure scalar path of the
/// interpreter, which is always plannable. A vector operand must itself be
/// structurally plannable. A matrix or string operand is never plannable.
pub(crate) fn binary_operand_is_plannable(operand: &Expr) -> bool {
    match operand.value_type() {
        ValueType::Scalar => true,
        ValueType::Vector => instant_expr_is_plannable(operand),
        ValueType::Matrix | ValueType::String => false,
    }
}
