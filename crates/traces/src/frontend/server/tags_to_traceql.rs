use super::{key_is_safe_attribute, parse_logfmt_tags};

pub(crate) fn tags_to_traceql(tags: &str) -> Option<String> {
    let parts: Vec<String> = parse_logfmt_tags(tags)?
        .into_iter()
        .map(|tag| {
            // The key is interpolated unquoted as a TraceQL attribute reference,
            // so a key carrying TraceQL-significant characters would inject query
            // structure (the value is already quoted+escaped). Reject such keys.
            key_is_safe_attribute(&tag.key).then(|| {
                let field = if tag.key.contains(':') {
                    tag.key
                } else {
                    format!(".{}", tag.key.strip_prefix('.').unwrap_or(&tag.key))
                };
                let escaped = tag.value.replace('\\', "\\\\").replace('"', "\\\"");
                format!("{field} = \"{escaped}\"")
            })
        })
        .collect::<Option<Vec<String>>>()?;
    (!parts.is_empty()).then(|| format!("{{ {} }}", parts.join(" && ")))
}
