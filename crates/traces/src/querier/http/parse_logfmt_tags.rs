use super::parse_logfmt_value;

pub(crate) struct LogfmtTag {
    pub(crate) key: String,
    pub(crate) value: String,
}

pub(crate) fn parse_logfmt_tags(tags: &str) -> Option<Vec<LogfmtTag>> {
    let mut out = Vec::new();
    let mut rest = tags.trim_start();
    while !rest.is_empty() {
        let key_end = rest.find('=')?;
        let key = &rest[..key_end];
        if key.is_empty() || key.chars().any(char::is_whitespace) {
            return None;
        }
        rest = &rest[key_end + 1..];
        let parsed = parse_logfmt_value(rest)?;
        out.push(LogfmtTag {
            key: key.to_string(),
            value: parsed.value,
        });
        rest = rest[parsed.consumed..].trim_start();
    }
    Some(out)
}
