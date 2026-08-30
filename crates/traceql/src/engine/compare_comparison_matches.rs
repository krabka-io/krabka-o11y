use super::*;

pub(crate) fn compare_comparison_matches(
    field: &Field,
    op: ComparisonOp,
    rhs: &Value,
    row: &CompareRow,
    regexes: &CompareRegexCache,
) -> bool {
    match compare_field_class(field) {
        Ok(CompareFieldClass::Attr { scope, key }) => {
            let values = compare_row_attr_values(row, &scope, &key);
            compare_attr_values_match(&values, op, rhs, regexes)
        }
        Ok(CompareFieldClass::Intrinsic(intrinsic)) => {
            compare_intrinsic_matches(row, &intrinsic, op, rhs, regexes)
        }
        Err(_) => false,
    }
}
