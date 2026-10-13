pub(crate) fn min_option(left: Option<i64>, right: Option<i64>) -> Option<i64> {
    match left {
        Some(left_value) => match right {
            Some(right_value) => Some(left_value.min(right_value)),
            None => Some(left_value),
        },
        None => right,
    }
}
