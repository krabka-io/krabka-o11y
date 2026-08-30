use super::*;

pub(crate) fn skip_leading_template_whitespace(template: &str, mut pos: usize) -> usize {
    let Some(rest) = template.get(pos..) else {
        return template.len();
    };
    let trimmed = rest.trim_start_matches(char::is_whitespace);
    pos = template
        .len()
        .checked_sub(trimmed.len())
        .expect("trimmed suffix cannot be longer than template");
    pos
}
