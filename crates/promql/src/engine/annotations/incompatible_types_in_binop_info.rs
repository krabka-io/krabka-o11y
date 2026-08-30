use super::*;

/// Exact Prometheus `IncompatibleTypesInBinOpInfo` text for incompatible operands.
///
/// An operator gets incompatible operand sample types, for example a histogram
/// and a float.
pub(crate) fn incompatible_types_in_binop_info(
    lhs_type: &str,
    operator: &str,
    rhs_type: &str,
) -> String {
    format!(
        "PromQL info: incompatible sample types encountered for binary operator {operator:?}: {lhs_type} {operator} {rhs_type}"
    )
}
