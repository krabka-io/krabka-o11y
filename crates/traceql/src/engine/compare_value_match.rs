use super::{
    AttrValue, CompareRegexCache, ComparisonOp, Value, bool_cmp, float_cmp, num_cmp, string_cmp,
};

pub(crate) fn compare_value_match(
    value: &AttrValue,
    op: ComparisonOp,
    rhs: &Value,
    regexes: &CompareRegexCache,
) -> bool {
    match (value, rhs) {
        (AttrValue::Str(value), Value::Str(rhs)) => string_cmp(value, op, rhs, regexes),
        (AttrValue::Int(value), Value::Int(rhs) | Value::Duration(rhs)) => {
            num_cmp(*value, op, *rhs)
        }
        (AttrValue::Float(value), Value::Float(rhs)) => float_cmp(*value, op, *rhs),
        (AttrValue::Bool(value), Value::Bool(rhs)) => bool_cmp(*value, op, *rhs),
        _ => false,
    }
}
