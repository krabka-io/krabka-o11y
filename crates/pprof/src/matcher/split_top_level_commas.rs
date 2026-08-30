use super::ProfileError;

pub(crate) fn split_top_level_commas(input: &str) -> Result<Vec<&str>, ProfileError> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut in_quote = false;
    let mut escaped = false;
    for (idx, ch) in input.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quote => escaped = true,
            '"' => in_quote = !in_quote,
            ',' if !in_quote => {
                // Tolerate empty parts from stray/leading/trailing/double commas
                // (e.g. `{,service_name="x"}`). Grafana's pyroscope app builds its
                // selector by concatenating an often-empty base filter with the
                // service name, yielding a leading comma; Prometheus/Pyroscope
                // accept these, so skip the empty part rather than erroring.
                let part = input[start..idx].trim();
                if !part.is_empty() {
                    parts.push(part);
                }
                start = idx + ch.len_utf8();
            }
            _ => {}
        }
    }
    if in_quote {
        return Err(ProfileError::Plan(
            "unterminated quoted matcher value".to_string(),
        ));
    }
    let tail = input[start..].trim();
    if !tail.is_empty() {
        parts.push(tail);
    }
    Ok(parts)
}
