use super::{
    AttrValue, CompareFieldClass, CompareRow, ComparisonOp, Field, FieldExpr, Intrinsic,
    RecordBatch, Result, Value, compare_field_class, compare_row, compare_row_attr_values,
    float_cmp, num_cmp,
};

pub(crate) fn field_comparison_column_values(
    expr: &FieldExpr,
    batch: &RecordBatch,
) -> Result<Vec<bool>> {
    super::validate_compare_field_expr(expr)?;
    let mut regexes = super::CompareRegexCache::new();
    super::collect_field_expr_regexes(expr, &mut regexes);
    (0..batch.num_rows())
        .map(|index| {
            let row = compare_row(batch, index, super::UnixNano(0))?;
            super::evaluate_typed_predicate(expr, &row, &regexes)
        })
        .collect()
}

pub(crate) fn field_values(field: &Field, row: &CompareRow) -> Vec<Value> {
    match compare_field_class(field) {
        Ok(CompareFieldClass::Attr { scope, key }) => compare_row_attr_values(row, &scope, &key)
            .into_iter()
            .map(attribute_scalar)
            .collect(),
        Ok(CompareFieldClass::Intrinsic(intrinsic)) => {
            let value = match intrinsic {
                Intrinsic::Name => row.name.clone().map(Value::Str),
                Intrinsic::StatusMessage => row.status_message.clone().map(Value::Str),
                Intrinsic::Duration => row.duration.map(Value::Duration),
                Intrinsic::Status => row.status_code.map(|value| Value::Int(i64::from(value))),
                Intrinsic::Kind => row.kind.map(|value| Value::Int(i64::from(value))),
                _ => super::metric_field_column(field)
                    .ok()
                    .and_then(|column| row.columns.get(&column).cloned())
                    .map(|value| {
                        if matches!(
                            intrinsic,
                            Intrinsic::TraceDuration | Intrinsic::EventTimeSinceStart
                        ) && let Value::Int(value) = value
                        {
                            return Value::Duration(value);
                        }
                        value
                    }),
            };
            value.into_iter().collect()
        }
        Err(_) => Vec::new(),
    }
}

fn attribute_scalar(value: &AttrValue) -> Value {
    match value {
        AttrValue::Str(value) => Value::Str(value.clone()),
        AttrValue::Int(value) => Value::Int(*value),
        AttrValue::Float(value) => Value::Float(*value),
        AttrValue::Bool(value) => Value::Bool(*value),
        AttrValue::Unsupported(_) => Value::Nil,
        AttrValue::Array(values) => Value::Array(values.iter().map(attribute_scalar).collect()),
    }
}

pub(crate) fn field_comparison_matches(
    lhs: &Field,
    op: ComparisonOp,
    rhs: &Field,
    row: &CompareRow,
) -> bool {
    let enum_type = |field: &Field| match field.scope {
        super::Scope::Intrinsic(Intrinsic::Status) => Some(Intrinsic::Status),
        super::Scope::Intrinsic(Intrinsic::Kind) => Some(Intrinsic::Kind),
        _ => None,
    };
    let lhs_enum = enum_type(lhs);
    let rhs_enum = enum_type(rhs);
    if (lhs_enum.is_some() || rhs_enum.is_some())
        && (lhs_enum != rhs_enum || !matches!(op, ComparisonOp::Eq | ComparisonOp::Neq))
    {
        return false;
    }
    let lhs = field_values(lhs, row);
    let rhs = field_values(rhs, row);
    // Tempo Static.Equals and Static.NotEquals both reject nil operands.
    if lhs.is_empty() || rhs.is_empty() {
        return false;
    }
    // Array inequality requires every element to differ. Equality and ordered
    // predicates need one matching element, as in Tempo's BinaryOperation.
    let predicates = lhs
        .iter()
        .flat_map(|lhs| rhs.iter().map(move |rhs| scalar_matches(lhs, op, rhs)));
    if matches!(op, ComparisonOp::Neq | ComparisonOp::Nre) {
        predicates.into_iter().all(|value| value)
    } else {
        predicates.into_iter().any(|value| value)
    }
}

pub(crate) fn scalar_matches(lhs: &Value, op: ComparisonOp, rhs: &Value) -> bool {
    match (lhs, rhs) {
        (Value::Array(_), Value::Array(_)) => false,
        (Value::Array(values), scalar) => {
            if matches!(op, ComparisonOp::Neq | ComparisonOp::Nre) {
                values.iter().all(|value| scalar_matches(value, op, scalar))
            } else {
                values.iter().any(|value| scalar_matches(value, op, scalar))
            }
        }
        (scalar, Value::Array(values)) => {
            if matches!(op, ComparisonOp::Neq | ComparisonOp::Nre) {
                values.iter().all(|value| scalar_matches(scalar, op, value))
            } else {
                values.iter().any(|value| scalar_matches(scalar, op, value))
            }
        }
        (Value::Str(lhs), Value::Str(rhs)) => match op {
            ComparisonOp::Eq => lhs == rhs,
            ComparisonOp::Neq => lhs != rhs,
            ComparisonOp::Lt => lhs < rhs,
            ComparisonOp::Lte => lhs <= rhs,
            ComparisonOp::Gt => lhs > rhs,
            ComparisonOp::Gte => lhs >= rhs,
            ComparisonOp::Re | ComparisonOp::Nre => regex::Regex::new(&format!("^(?:{rhs})$"))
                .is_ok_and(|regex| regex.is_match(lhs) == (op == ComparisonOp::Re)),
        },
        (Value::Int(lhs) | Value::Duration(lhs), Value::Int(rhs) | Value::Duration(rhs)) => {
            num_cmp(*lhs, op, *rhs)
        }
        (Value::Float(lhs), Value::Float(rhs)) => float_cmp(*lhs, op, *rhs),
        (Value::Float(lhs), Value::Int(rhs) | Value::Duration(rhs)) => {
            float_cmp(*lhs, op, super::f64_from_i64(*rhs))
        }
        (Value::Int(lhs) | Value::Duration(lhs), Value::Float(rhs)) => {
            float_cmp(super::f64_from_i64(*lhs), op, *rhs)
        }
        (Value::Bool(lhs), Value::Bool(rhs)) => super::bool_cmp(*lhs, op, *rhs),
        // No string coercion: an integer attribute never equals a string with
        // the same spelling, and incompatible operands produce false for != too.
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;

    #[test]
    fn enum_fields_do_not_become_numeric_or_interchangeable() {
        let row = CompareRow {
            ts: super::super::UnixNano(0),
            attrs: Vec::new(),
            raw_span_attrs: vec![("number".into(), AttrValue::Int(2))],
            raw_resource_attrs: Vec::new(),
            name: None,
            status_code: Some(2),
            status_message: None,
            kind: Some(2),
            duration: None,
            columns: std::collections::BTreeMap::new(),
        };
        let status = Field {
            scope: super::super::Scope::Intrinsic(Intrinsic::Status),
            key: "status".into(),
        };
        let kind = Field {
            scope: super::super::Scope::Intrinsic(Intrinsic::Kind),
            key: "kind".into(),
        };
        let number = Field {
            scope: super::super::Scope::Span,
            key: "number".into(),
        };
        assert!(field_comparison_matches(
            &status,
            ComparisonOp::Eq,
            &status,
            &row
        ));
        assert!(!field_comparison_matches(
            &status,
            ComparisonOp::Eq,
            &kind,
            &row
        ));
        assert!(!field_comparison_matches(
            &kind,
            ComparisonOp::Eq,
            &number,
            &row
        ));
        assert!(!field_comparison_matches(
            &kind,
            ComparisonOp::Neq,
            &number,
            &row
        ));
        assert!(!field_comparison_matches(
            &kind,
            ComparisonOp::Gte,
            &kind,
            &row
        ));
    }

    #[test]
    fn dynamic_comparisons_preserve_types_ordering_and_regex_operands() {
        for (lhs, op, rhs, expected) in [
            (Value::Int(3), ComparisonOp::Eq, Value::Int(3), true),
            (
                Value::Int(3),
                ComparisonOp::Eq,
                Value::Str("3".into()),
                false,
            ),
            (
                Value::Int(3),
                ComparisonOp::Neq,
                Value::Str("4".into()),
                false,
            ),
            (Value::Float(3.5), ComparisonOp::Gt, Value::Float(3.0), true),
            (
                Value::Bool(true),
                ComparisonOp::Eq,
                Value::Bool(false),
                false,
            ),
            (
                Value::Str("a".into()),
                ComparisonOp::Lt,
                Value::Str("b".into()),
                true,
            ),
            (
                Value::Str("cart".into()),
                ComparisonOp::Re,
                Value::Str("ca.*".into()),
                true,
            ),
            (
                Value::Str("cart".into()),
                ComparisonOp::Re,
                Value::Str("ar".into()),
                false,
            ),
        ] {
            assert!(scalar_matches(&lhs, op, &rhs) == expected);
        }
    }
}
