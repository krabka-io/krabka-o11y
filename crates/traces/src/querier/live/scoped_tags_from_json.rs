use super::*;

pub(crate) fn scoped_tags_from_json(json: &serde_json::Value) -> Result<Vec<ScopedTag>> {
    let scopes = json
        .get("scopes")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            TraceqlError::Plan("remote live-store tags response missing scopes".into())
        })?;
    let mut out = Vec::new();
    for scope in scopes {
        let Some(name) = scope.get("name").and_then(serde_json::Value::as_str) else {
            continue;
        };
        let Some(scope_name) = tag_scope_from_name(name) else {
            continue;
        };
        let tags = scope
            .get("tags")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|tag| tag.as_str().map(ToString::to_string))
            .collect();
        out.push(ScopedTag {
            scope: scope_name,
            tags,
        });
    }
    Ok(out)
}
