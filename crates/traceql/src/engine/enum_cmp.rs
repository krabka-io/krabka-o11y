use super::*;

pub(crate) fn enum_cmp(
    value: i32,
    op: ComparisonOp,
    rhs: &Value,
    enum_value: fn(&str) -> Option<i32>,
) -> bool {
    let expected = match rhs {
        Value::Str(name) => enum_value(&name.to_ascii_lowercase()),
        Value::Int(value) => i32::try_from(*value).ok(),
        _ => None,
    };
    expected.is_some_and(|expected| num_cmp(i64::from(value), op, i64::from(expected)))
}
