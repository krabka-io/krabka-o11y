use super::*;

pub(crate) fn search_tags_v2_json(tags: &[ScopedTag]) -> Value {
    json!({
        "scopes": tags.iter().map(|scope| {
            json!({
                "name": tag_scope_name(scope.scope),
                "tags": &scope.tags,
            })
        }).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}
