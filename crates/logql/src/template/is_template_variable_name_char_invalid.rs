
pub(crate) fn is_template_variable_name_char_invalid(ch: char) -> bool {
    match ch {
        '.' => true,
        _ => ch.is_whitespace(),
    }
}
