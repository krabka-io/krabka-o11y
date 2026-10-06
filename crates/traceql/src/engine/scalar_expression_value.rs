use super::{ArithmeticOp, CompareRow, Result, ScalarExpr, TraceqlError, Value, f64_from_i64};

pub(crate) fn scalar_expression_value(expr: &ScalarExpr, row: &CompareRow) -> Result<Value> {
    match expr {
        ScalarExpr::Predicate(expr) => {
            let mut regexes = super::CompareRegexCache::new();
            super::collect_field_expr_regexes(expr, &mut regexes);
            let values = predicate_values(expr, row, &regexes)?;
            if values.len() > 1 || values.iter().any(|value| matches!(value, Value::Array(_))) {
                return Err(TraceqlError::Unsupported(
                    "scalar expression on array attributes".into(),
                ));
            }
            Ok(values.into_iter().next().unwrap_or(Value::Nil))
        }
        ScalarExpr::Literal(value) => Ok(value.clone()),
        ScalarExpr::Field(field) => {
            let values = super::field_comparison::field_values(field, row);
            if values.len() > 1 || values.iter().any(|value| matches!(value, Value::Array(_))) {
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
                // Tempo's supported amd64 image converts an out-of-range or
                // nonfinite float to the minimum signed integer (CVTTSD2SI).
                format!("{:.0}", value.trunc()).parse().unwrap_or(i64::MIN)
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
    Ok(matches!(
        predicate_values(expr, row, regexes)?.as_slice(),
        [Value::Bool(true)]
    ))
}

pub(crate) fn predicate_values(
    expr: &super::FieldExpr,
    row: &CompareRow,
    regexes: &super::CompareRegexCache,
) -> Result<Vec<Value>> {
    use super::FieldExpr;
    let value = match expr {
        FieldExpr::Field(field) => return Ok(super::field_comparison::field_values(field, row)),
        FieldExpr::FieldComparison { lhs, op, rhs } => {
            let left = super::field_comparison::field_values(lhs, row);
            let right = super::field_comparison::field_values(rhs, row);
            let same_type = left.first().zip(right.first()).is_some_and(|(lhs, rhs)| {
                std::mem::discriminant(lhs) == std::mem::discriminant(rhs)
                    || matches!(lhs, Value::Int(_) | Value::Float(_) | Value::Duration(_))
                        && matches!(rhs, Value::Int(_) | Value::Float(_) | Value::Duration(_))
            });
            if (left.len() > 1 || left.iter().any(|value| matches!(value, Value::Array(_))))
                && (right.len() > 1 || right.iter().any(|value| matches!(value, Value::Array(_))))
                && same_type
            {
                return Err(TraceqlError::Exec(
                    "array operators must consist of a scalar and an array operand".into(),
                ));
            }
            super::field_comparison::field_comparison_matches(lhs, *op, rhs, row)
        }
        FieldExpr::ExpressionComparison { lhs, op, rhs } => {
            super::field_comparison::scalar_matches(
                &scalar_expression_value(lhs, row)?,
                *op,
                &scalar_expression_value(rhs, row)?,
            )
        }
        FieldExpr::And(lhs, rhs) | FieldExpr::Or(lhs, rhs) => {
            let and = matches!(expr, FieldExpr::And(_, _));
            let left = predicate_values(lhs, row, regexes)?;
            // Pinned BinaryOperation avoids evaluating an unnecessary rhs only
            // when the lhs is a scalar boolean of the appropriate value.
            if matches!(left.as_slice(), [Value::Bool(value)] if *value != and) {
                !and
            } else {
                let right = predicate_values(rhs, row, regexes)?;
                boolean_operation(&left, &right, and)?
            }
        }
        FieldExpr::Not(inner) => {
            let values = predicate_values(inner, row, regexes)?;
            let [Value::Bool(value)] = values.as_slice() else {
                return Err(TraceqlError::Exec("expression expected a boolean".into()));
            };
            !value
        }
        _ => super::field_expr_matches_row(expr, row, regexes),
    };
    Ok(vec![Value::Bool(value)])
}

fn boolean_operation(left: &[Value], right: &[Value], and: bool) -> Result<bool> {
    let unwrap = |values: &[Value]| match values {
        [Value::Array(values)] => (true, values.clone()),
        values => (values.len() > 1, values.to_vec()),
    };
    let (left_array, left) = unwrap(left);
    let (right_array, right) = unwrap(right);
    if left_array && right_array {
        return Err(TraceqlError::Exec(
            "array operators must consist of a scalar and an array operand".into(),
        ));
    }
    if left
        .iter()
        .chain(&right)
        .any(|value| !matches!(value, Value::Bool(_)))
    {
        return Ok(false);
    }
    Ok(left.iter().any(|lhs| {
        right.iter().any(|rhs| {
            let (Value::Bool(lhs), Value::Bool(rhs)) = (lhs, rhs) else {
                return false;
            };
            if and { *lhs && *rhs } else { *lhs || *rhs }
        })
    }))
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
        for (exponent, base, expected) in [
            (1_000, 10, i64::MIN),
            (63, 2, i64::MIN),
            (62, 2, 4_611_686_018_427_387_904),
            (-1, 0, i64::MIN),
            (-1, 2, 0),
            (0, 0, 1),
        ] {
            assert!(
                scalar_arithmetic(Value::Int(exponent), ArithmeticOp::Pow, Value::Int(base))
                    .unwrap()
                    == Value::Int(expected)
            );
        }
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
