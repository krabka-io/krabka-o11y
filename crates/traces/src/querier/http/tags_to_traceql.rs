use super::{key_is_safe_attribute, parse_logfmt_tags, traceql_tag_field};

pub(crate) fn tags_to_traceql(tags: &str) -> Option<String> {
    let parts = parse_logfmt_tags(tags)?
        .into_iter()
        .map(|tag| {
            // The key becomes an unquoted TraceQL attribute reference, so a key
            // carrying TraceQL-significant characters would inject query
            // structure (the value is already quoted+escaped). Reject such keys
            // rather than interpolating their raw bytes.
            key_is_safe_attribute(&tag.key).then(|| {
                format!(
                    "{} = \"{}\"",
                    traceql_tag_field(&tag.key),
                    tag.value.replace('\\', "\\\\").replace('"', "\\\"")
                )
            })
        })
        .collect::<Option<Vec<_>>>()?;
    (!parts.is_empty()).then(|| format!("{{ {} }}", parts.join(" && ")))
}
