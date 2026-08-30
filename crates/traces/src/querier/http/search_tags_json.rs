use super::{json, ScopedTag, Value};

pub(crate) fn search_tags_json(tags: &[ScopedTag]) -> Value {
    json!({
        "tagNames": tags.iter().flat_map(|scope| scope.tags.iter()).collect::<Vec<_>>(),
        "metrics": {
            "inspectedBytes": "0",
        },
    })
}
