use super::{ScopedTag, Value, json};

pub(crate) fn search_tags_v2_json(tags: &[ScopedTag]) -> Value {
    json!({
        "scopes": tags.iter().map(|scope| {
            json!({
                "name": scope.scope.as_str(),
                "tags": &scope.tags,
            })
        }).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}
