use super::{parse_logfmt_tags, key_is_safe_attribute};

pub(crate) fn tags_to_traceql(tags: &str) -> Option<String> {
    let parts: Vec<String> = parse_logfmt_tags(tags)?
        .into_iter()
        .map(|(key, value)| {
            // The key is interpolated unquoted as a TraceQL attribute reference,
            // so a key carrying TraceQL-significant characters would inject query
            // structure (the value is already quoted+escaped). Reject such keys.
            key_is_safe_attribute(&key).then(|| {
                let field = if key.contains(':') {
                    key
                } else {
                    format!(".{}", key.strip_prefix('.').unwrap_or(&key))
                };
                let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
                format!("{field} = \"{escaped}\"")
            })
        })
        .collect::<Option<Vec<String>>>()?;
    (!parts.is_empty()).then(|| format!("{{ {} }}", parts.join(" && ")))
}
