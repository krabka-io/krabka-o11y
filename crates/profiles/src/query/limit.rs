pub(crate) fn limit(limit: i64) -> usize {
    usize::try_from(limit)
        .ok()
        .filter(|limit| *limit > 0)
        .unwrap_or(usize::MAX)
}
