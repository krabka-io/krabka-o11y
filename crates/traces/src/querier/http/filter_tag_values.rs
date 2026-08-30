use super::*;

pub(crate) fn filter_tag_values(values: Vec<TypedValue>, expected: &TypedValue) -> Vec<TypedValue> {
    values
        .into_iter()
        .filter(|value| value == expected)
        .collect()
}
