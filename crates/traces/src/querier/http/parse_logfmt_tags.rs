use super::parse_logfmt_value;

pub(crate) fn parse_logfmt_tags(tags: &str) -> Option<Vec<(String, String)>> {
    let mut out = Vec::new();
    let mut rest = tags.trim_start();
    while !rest.is_empty() {
        let key_end = rest.find('=')?;
        let key = &rest[..key_end];
        if key.is_empty() || key.chars().any(char::is_whitespace) {
            return None;
        }
        rest = &rest[key_end + 1..];
        let (value, consumed) = parse_logfmt_value(rest)?;
        out.push((key.to_string(), value));
        rest = rest[consumed..].trim_start();
    }
    Some(out)
}
