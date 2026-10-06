use super::{ComparisonOp, Intrinsic, Result, ScalarExpr, Scope, TraceqlError, Value};

#[derive(Clone, Copy, PartialEq, Eq)]
enum ScalarType {
    Number,
    String,
    Bool,
    Status,
    Kind,
    Unknown,
    Nil,
}

fn scalar_type(expr: &ScalarExpr) -> Result<ScalarType> {
    Ok(match expr {
        ScalarExpr::Field(field) => match field.scope {
            Scope::Intrinsic(
                Intrinsic::Duration
                | Intrinsic::TraceDuration
                | Intrinsic::ChildCount
                | Intrinsic::NestedSetLeft
                | Intrinsic::NestedSetRight
                | Intrinsic::NestedSetParent
                | Intrinsic::EventTimeSinceStart,
            ) => ScalarType::Number,
            Scope::Intrinsic(Intrinsic::Status) => ScalarType::Status,
            Scope::Intrinsic(Intrinsic::Kind) => ScalarType::Kind,
            Scope::Intrinsic(_) => ScalarType::String,
            _ => ScalarType::Unknown,
        },
        ScalarExpr::Literal(value) => match value {
            Value::Int(_) | Value::Float(_) | Value::Duration(_) => ScalarType::Number,
            Value::Str(_) => ScalarType::String,
            Value::Bool(_) => ScalarType::Bool,
            Value::Nil => ScalarType::Nil,
        },
        ScalarExpr::Negate(inner) => {
            let kind = scalar_type(inner)?;
            if !matches!(kind, ScalarType::Number | ScalarType::Unknown) {
                return Err(TraceqlError::Plan(
                    "arithmetic requires numeric operands".into(),
                ));
            }
            kind
        }
        ScalarExpr::Binary { lhs, rhs, .. } => {
            let lhs = scalar_type(lhs)?;
            let rhs = scalar_type(rhs)?;
            if !matches!(lhs, ScalarType::Number | ScalarType::Unknown)
                || !matches!(rhs, ScalarType::Number | ScalarType::Unknown)
            {
                return Err(TraceqlError::Plan(
                    "arithmetic requires numeric operands".into(),
                ));
            }
            if lhs == ScalarType::Unknown { rhs } else { lhs }
        }
    })
}

pub(crate) fn validate_scalar_comparison(
    lhs: &ScalarExpr,
    op: ComparisonOp,
    rhs: &ScalarExpr,
) -> Result<()> {
    // Preserve the existing simple intrinsic enum literal representation: its
    // string token is resolved by the intrinsic planner. Arithmetic is typed.
    if matches!(lhs, ScalarExpr::Field(_)) && matches!(rhs, ScalarExpr::Literal(_)) {
        return Ok(());
    }
    let lhs = scalar_type(lhs)?;
    let rhs = scalar_type(rhs)?;
    if lhs != rhs
        && !matches!(lhs, ScalarType::Unknown | ScalarType::Nil)
        && !matches!(rhs, ScalarType::Unknown | ScalarType::Nil)
    {
        return Err(TraceqlError::Plan(
            "comparison operands have incompatible types".into(),
        ));
    }
    if !matches!(op, ComparisonOp::Eq | ComparisonOp::Neq)
        && (matches!(
            lhs,
            ScalarType::Bool | ScalarType::Status | ScalarType::Kind | ScalarType::Nil
        ) || matches!(
            rhs,
            ScalarType::Bool | ScalarType::Status | ScalarType::Kind | ScalarType::Nil
        ))
    {
        return Err(TraceqlError::Plan(
            "comparison operator is invalid for operand type".into(),
        ));
    }
    Ok(())
}
