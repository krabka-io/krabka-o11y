
pub(crate) fn apply_limit<T>(values: &mut Vec<T>, limit: Option<usize>) {
    if let Some(limit) = limit.filter(|limit| *limit > 0) {
        values.truncate(limit);
    }
}
