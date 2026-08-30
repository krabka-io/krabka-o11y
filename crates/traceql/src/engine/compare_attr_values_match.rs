use super::{AttrValue, CompareRegexCache, ComparisonOp, Value, compare_value_match};

/// Applies Tempo array-attribute semantics to one attribute.
///
/// Equality, `<`, `>`, and regex match if ANY value satisfies the predicate.
/// `!=` and `!~` match only if ALL values satisfy the predicate. An absent
/// attribute matches ONLY `= nil`. This mirrors the planner SQL, where
/// `NULL != v` excludes the row, and the nil rules of the in-memory store. A
/// `!= <concrete>` or `!~` predicate does NOT pull a span without the
/// attribute into the selection.
pub(crate) fn compare_attr_values_match(
    values: &[&AttrValue],
    op: ComparisonOp,
    rhs: &Value,
    regexes: &CompareRegexCache,
) -> bool {
    if values.is_empty() {
        return matches!((op, rhs), (ComparisonOp::Eq, Value::Nil));
    }
    if matches!(rhs, Value::Nil) {
        // present value: `= nil` is false, `!= nil` is true.
        return matches!(op, ComparisonOp::Neq);
    }
    match op {
        ComparisonOp::Neq | ComparisonOp::Nre => values
            .iter()
            .all(|value| compare_value_match(value, op, rhs, regexes)),
        _ => values
            .iter()
            .any(|value| compare_value_match(value, op, rhs, regexes)),
    }
}
