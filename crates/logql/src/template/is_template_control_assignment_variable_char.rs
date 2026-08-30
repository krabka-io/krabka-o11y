pub(crate) fn is_template_control_assignment_variable_char(ch: char) -> bool {
    match ch {
        '|' => true,
        _ => ch.is_whitespace(),
    }
}
