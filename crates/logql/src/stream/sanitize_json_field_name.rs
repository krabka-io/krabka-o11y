use super::*;

pub(crate) fn sanitize_json_field_name(name: &str) -> String {
    let mut sanitized = name
        .chars()
        .map(|ch| {
            if ch == '_' || ch == ':' || ch.is_ascii_alphanumeric() {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>();
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
