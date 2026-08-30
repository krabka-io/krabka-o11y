use super::*;

pub(crate) fn sanitize_logfmt_field_name(name: &str) -> String {
    let mut sanitized = String::new();
    let mut last_was_underscore = false;
    for ch in name.chars() {
        if ch == '_' || ch == ':' || ch.is_ascii_alphanumeric() {
            sanitized.push(ch);
            last_was_underscore = false;
        } else if !last_was_underscore {
            sanitized.push('_');
            last_was_underscore = true;
        }
    }
    if sanitized.is_empty() {
        sanitized.push('_');
    }
    if sanitized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        sanitized.insert(0, '_');
    }
    sanitized
}
