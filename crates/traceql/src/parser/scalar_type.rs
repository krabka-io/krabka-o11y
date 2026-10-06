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
        ScalarExpr::Predicate(_) => ScalarType::Bool,
        ScalarExpr::Field(field) => match field.scope {
            Scope::Parent => {
                return Err(TraceqlError::Unsupported("parent field expressions".into()));
            }
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
            Value::Array(_) => ScalarType::Unknown,
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
    // Tempo rewrites equality with nil to a not-exists operation. These fields
    // cannot be nil even when a particular span has no event or link.
    if op == ComparisonOp::Eq
        && matches!(rhs, ScalarExpr::Literal(Value::Nil))
        && let ScalarExpr::Field(field) = lhs
        && (matches!(field.scope, Scope::Intrinsic(_))
            || matches!(field.scope, Scope::Resource) && field.key == "service.name")
    {
        return Err(TraceqlError::Plan(format!("{} cannot be nil", field.key)));
    }
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

pub(crate) fn validate_boolean_field(field: &super::Field) -> Result<()> {
    if scalar_type(&ScalarExpr::Field(field.clone()))? != ScalarType::Unknown {
        return Err(TraceqlError::Plan(
            "span filter field expressions must resolve to a boolean".into(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_span_expression(expr: &ScalarExpr, numeric: bool) -> Result<()> {
    let kind = scalar_type(expr)?;
    if numeric && !matches!(kind, ScalarType::Number | ScalarType::Unknown) {
        return Err(TraceqlError::Plan(
            "aggregate field expressions must resolve to a number type".into(),
        ));
    }
    let mut fields = Vec::new();
    expr.collect_fields(&mut fields);
    if fields
        .iter()
        .any(|field| matches!(field.scope, Scope::Parent))
    {
        return Err(TraceqlError::Unsupported("parent field expressions".into()));
    }
    if fields.is_empty() {
        return Err(TraceqlError::Plan(
            "field expressions must reference the span".into(),
        ));
    }
    Ok(())
}
