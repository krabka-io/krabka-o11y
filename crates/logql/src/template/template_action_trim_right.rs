
pub(crate) fn template_action_trim_right(template: &str, expression_start: usize, close: usize) -> bool {
    if close <= expression_start || !template[..close].ends_with('-') {
        return false;
    }
    template[expression_start..close - 1]
        .chars()
        .next_back()
        .is_some_and(char::is_whitespace)
}
