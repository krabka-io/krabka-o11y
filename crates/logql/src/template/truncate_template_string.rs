
pub(crate) fn truncate_template_string(value: &str, count: i64) -> String {
    if count >= 0 {
        let count = usize::try_from(count).unwrap_or(usize::MAX);
        return value.chars().take(count).collect();
    }
    let count = usize::try_from(count.unsigned_abs()).unwrap_or(usize::MAX);
    let len = value.chars().count();
    value.chars().skip(len.saturating_sub(count)).collect()
}
