pub(crate) fn align_left_template_string(width: usize, value: &str) -> String {
    let mut chars = value.chars().take(width).collect::<String>();
    let padding = width.saturating_sub(chars.chars().count());
    chars.extend(std::iter::repeat_n(' ', padding));
    chars
}
