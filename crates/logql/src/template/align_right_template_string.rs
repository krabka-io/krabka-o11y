pub(crate) fn align_right_template_string(width: usize, value: &str) -> String {
    let chars = value.chars().collect::<Vec<_>>();
    if chars.len() >= width {
        return chars[chars.len() - width..].iter().collect();
    }
    let mut aligned = " ".repeat(width - chars.len());
    aligned.extend(chars);
    aligned
}
