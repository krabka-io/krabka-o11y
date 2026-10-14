use super::{
    AttrValue, CompareRegexCache, ComparisonOp, Value, bool_cmp, float_cmp, num_cmp, string_cmp,
};

struct ComparedValues<'a> {
    actual: &'a AttrValue,
    expected: &'a Value,
}

pub(crate) fn compare_value_match(
    value: &AttrValue,
    op: ComparisonOp,
    rhs: &Value,
    regexes: &CompareRegexCache,
) -> bool {
    match (ComparedValues {
        actual: value,
        expected: rhs,
    }) {
        ComparedValues {
            actual: AttrValue::Array(values),
            expected: rhs,
        } => {
            if matches!(op, ComparisonOp::Neq | ComparisonOp::Nre) {
                values
                    .iter()
                    .all(|value| compare_value_match(value, op, rhs, regexes))
            } else {
                values
                    .iter()
                    .any(|value| compare_value_match(value, op, rhs, regexes))
            }
        }
        ComparedValues {
            actual: AttrValue::Str(value),
            expected: Value::Str(rhs),
        } => string_cmp(value, op, rhs, regexes),
        ComparedValues {
            actual: AttrValue::Int(value),
            expected: Value::Int(rhs) | Value::Duration(rhs),
        } => num_cmp(*value, op, *rhs),
        ComparedValues {
            actual: AttrValue::Float(value),
            expected: Value::Float(rhs),
        } => float_cmp(*value, op, *rhs),
        ComparedValues {
            actual: AttrValue::Bool(value),
            expected: Value::Bool(rhs),
        } => bool_cmp(*value, op, *rhs),
        _ => false,
    }
}
