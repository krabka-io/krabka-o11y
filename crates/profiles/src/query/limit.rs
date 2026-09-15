pub(crate) fn limit(limit: Option<i64>) -> usize {
    limit
        .and_then(|limit| usize::try_from(limit).ok())
        .filter(|limit| *limit > 0)
        .unwrap_or(usize::MAX)
}
