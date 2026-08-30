
pub(crate) fn substring_template_string(value: &str, start: i64, end: i64) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    let len = chars.len();
    let start = usize::try_from(start.max(0)).unwrap_or(usize::MAX).min(len);
    let end = usize::try_from(end).ok().map_or(len, |end| end.min(len));
    if end <= start {
        return String::new();
    }
    chars[start..end].iter().collect()
}
