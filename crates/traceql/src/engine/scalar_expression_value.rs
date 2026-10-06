use super::{ArithmeticOp, CompareRow, Result, ScalarExpr, TraceqlError, Value, f64_from_i64};

pub(crate) fn scalar_expression_value(expr: &ScalarExpr, row: &CompareRow) -> Result<Value> {
    match expr {
        ScalarExpr::Literal(value) => Ok(value.clone()),
        ScalarExpr::Field(field) => {
            let values = super::field_comparison::field_values(field, row);
            if values.len() > 1 {
                return Err(TraceqlError::Unsupported(
                    "scalar arithmetic on array attributes".into(),
                ));
            }
            Ok(values.into_iter().next().unwrap_or(Value::Nil))
        }
        ScalarExpr::Negate(inner) => match scalar_expression_value(inner, row)? {
            Value::Int(value) => Ok(Value::Int(value.wrapping_neg())),
            Value::Duration(value) => Ok(Value::Duration(value.wrapping_neg())),
            Value::Float(value) => Ok(Value::Float(-value)),
            _ => Err(TraceqlError::Exec(
                "unary minus requires a numeric value".into(),
            )),
        },
        ScalarExpr::Binary { lhs, op, rhs } => {
            let lhs = scalar_expression_value(lhs, row)?;
            let rhs = scalar_expression_value(rhs, row)?;
            scalar_arithmetic(lhs, *op, rhs)
        }
    }
}

fn scalar_arithmetic(lhs: Value, op: ArithmeticOp, rhs: Value) -> Result<Value> {
    if let (Value::Int(lhs), Value::Int(rhs)) = (&lhs, &rhs) {
        return Ok(Value::Int(match op {
            ArithmeticOp::Add => lhs.wrapping_add(*rhs),
            ArithmeticOp::Sub => lhs.wrapping_sub(*rhs),
            ArithmeticOp::Mul => lhs.wrapping_mul(*rhs),
            ArithmeticOp::Div if *rhs != 0 => lhs.wrapping_div(*rhs),
            ArithmeticOp::Mod if *rhs != 0 => lhs.wrapping_rem(*rhs),
            ArithmeticOp::Div => return Err(TraceqlError::Exec("division by zero".into())),
            ArithmeticOp::Mod => return Err(TraceqlError::Exec("modulo by zero".into())),
            // Pinned Tempo 3.0.3 calls intPow(rhs, lhs), whose parameters are
            // base then exponent. Preserve that version's operand order.
            ArithmeticOp::Pow => {
                let value = f64_from_i64(*rhs).powf(f64_from_i64(*lhs));
                if !value.is_finite() {
                    return Err(TraceqlError::Unsupported("nonfinite integer power".into()));
                }
                value
                    .trunc()
                    .to_string()
                    .parse()
                    .map_err(|_| TraceqlError::Unsupported("integer power outside i64".into()))?
            }
        }));
    }
    let number = |value| match value {
        Value::Int(value) | Value::Duration(value) => Some(f64_from_i64(value)),
        Value::Float(value) => Some(value),
        _ => None,
    };
    let (Some(lhs), Some(rhs)) = (number(lhs), number(rhs)) else {
        // Tempo resolves incompatible runtime operands to false. The enclosing
        // numeric comparison then rejects this value without string coercion.
        return Ok(Value::Bool(false));
    };
    Ok(Value::Float(match op {
        ArithmeticOp::Add => lhs + rhs,
        ArithmeticOp::Sub => lhs - rhs,
        ArithmeticOp::Mul => lhs * rhs,
        ArithmeticOp::Div => lhs / rhs,
        ArithmeticOp::Mod => lhs % rhs,
        ArithmeticOp::Pow => lhs.powf(rhs),
    }))
}

pub(crate) fn evaluate_typed_predicate(
    expr: &super::FieldExpr,
    row: &CompareRow,
    regexes: &super::CompareRegexCache,
) -> Result<bool> {
    use super::FieldExpr;
    Ok(match expr {
        FieldExpr::ExpressionComparison { lhs, op, rhs } => {
            super::field_comparison::scalar_matches(
                &scalar_expression_value(lhs, row)?,
                *op,
                &scalar_expression_value(rhs, row)?,
            )
        }
        FieldExpr::And(lhs, rhs) => {
            evaluate_typed_predicate(lhs, row, regexes)?
                && evaluate_typed_predicate(rhs, row, regexes)?
        }
        FieldExpr::Or(lhs, rhs) => {
            evaluate_typed_predicate(lhs, row, regexes)?
                || evaluate_typed_predicate(rhs, row, regexes)?
        }
        FieldExpr::Not(inner) => !evaluate_typed_predicate(inner, row, regexes)?,
        _ => super::field_expr_matches_row(expr, row, regexes),
    })
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    #[test]
    fn integer_arithmetic_wraps_but_zero_division_is_an_error_and_mixed_numbers_are_float() {
        assert!(
            scalar_arithmetic(Value::Int(i64::MAX), ArithmeticOp::Add, Value::Int(1)).unwrap()
                == Value::Int(i64::MIN)
        );
        assert!(
            scalar_arithmetic(Value::Int(7), ArithmeticOp::Div, Value::Int(2)).unwrap()
                == Value::Int(3)
        );
        assert!(
            scalar_arithmetic(Value::Int(-7), ArithmeticOp::Mod, Value::Int(2)).unwrap()
                == Value::Int(-1)
        );
        assert!(
            scalar_arithmetic(Value::Int(2), ArithmeticOp::Pow, Value::Int(3)).unwrap()
                == Value::Int(9)
        );
        assert!(scalar_arithmetic(Value::Int(1), ArithmeticOp::Div, Value::Int(0)).is_err());
        assert!(scalar_arithmetic(Value::Int(1), ArithmeticOp::Mod, Value::Int(0)).is_err());
        assert!(scalar_arithmetic(Value::Int(1_000), ArithmeticOp::Pow, Value::Int(10)).is_err());
        let Value::Float(infinity) =
            scalar_arithmetic(Value::Float(1.0), ArithmeticOp::Div, Value::Float(0.0)).unwrap()
        else {
            panic!("floating division retains its type")
        };
        assert!(infinity.is_infinite() && infinity.is_sign_positive());
        let Value::Float(nan) =
            scalar_arithmetic(Value::Float(0.0), ArithmeticOp::Div, Value::Float(0.0)).unwrap()
        else {
            panic!("floating division retains its type")
        };
        assert!(nan.is_nan());
        assert!(
            scalar_arithmetic(
                Value::Duration(1_000_000),
                ArithmeticOp::Div,
                Value::Duration(2_000_000)
            )
            .unwrap()
                == Value::Float(0.5)
        );
        assert!(
            scalar_arithmetic(Value::Int(2), ArithmeticOp::Add, Value::Float(0.5)).unwrap()
                == Value::Float(2.5)
        );
        assert!(
            scalar_arithmetic(Value::Str("2".into()), ArithmeticOp::Add, Value::Int(1)).unwrap()
                == Value::Bool(false)
        );
    }
}
