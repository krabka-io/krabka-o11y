use super::*;

pub(crate) fn parse_logfmt_value(input: &str) -> Option<(String, usize)> {
    if let Some(input) = input.strip_prefix('"') {
        let mut value = String::new();
        let mut escaped = false;
        for (idx, ch) in input.char_indices() {
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
                return Some((value, idx + 2));
            } else {
                value.push(ch);
            }
        }
        return None;
    }
    let end = input
        .char_indices()
        .find_map(|(idx, ch)| ch.is_whitespace().then_some(idx))
        .unwrap_or(input.len());
    Some((input[..end].to_string(), end))
}
