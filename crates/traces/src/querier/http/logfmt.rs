#[derive(Debug, PartialEq, Eq)]
pub(crate) struct ParsedLogfmtValue {
    pub(crate) value: String,
    pub(crate) consumed: usize,
}

pub(crate) fn parse_logfmt_value(input: &str) -> Option<ParsedLogfmtValue> {
    if let Some(input) = input.strip_prefix('"') {
        let mut value = String::new();
        let mut escaped = false;
        for indexed_char in input.char_indices() {
            let idx = indexed_char.0;
            let ch = indexed_char.1;
            if escaped {
                value.push(match ch {
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    other => other,
                });
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                return Some(ParsedLogfmtValue {
                    value,
                    consumed: idx + 2,
                });
            } else {
                value.push(ch);
            }
        }
        return None;
    }

    let end = input
        .char_indices()
        .find_map(|indexed_char| indexed_char.1.is_whitespace().then_some(indexed_char.0))
        .unwrap_or(input.len());
    Some(ParsedLogfmtValue {
        value: input[..end].to_string(),
        consumed: end,
    })
}

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
