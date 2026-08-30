use super::*;

pub(crate) fn trim_template_body_end(template: &str, start: usize, end: usize) -> usize {
    template[..end]
        .trim_end_matches(char::is_whitespace)
        .len()
        .max(start)
}
