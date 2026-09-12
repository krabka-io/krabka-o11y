use super::{Value, json};

/// Echoes the request's encoding flags into a `streams` response.
///
/// Loki reports them from the `streams` marshaller alone: a `matrix` or
/// `vector` answer to a request that carried the header comes back without
/// them.
///
/// The flags go in front of `result`, and the position is part of the
/// contract rather than a matter of taste. `serde_json` keeps insertion order
/// here, and Grafana's Loki datasource decodes the body in one pass: it reads
/// `encodingFlags` to learn that an entry is three elements long rather than
/// two. A flag written after `result` therefore arrives too late, and the
/// datasource fails the whole query with `ReadArray: expect [ or , or ] or n,
/// but found {`. Loki writes `resultType`, then `encodingFlags`, then
/// `result`, so this does the same.
pub(crate) fn add_loki_encoding_flags(value: &mut Value, flags: &[String]) {
    if flags.is_empty()
        || value.pointer("/data/resultType").and_then(Value::as_str) != Some("streams")
    {
        return;
    }
    let Some(data) = value.get_mut("data").and_then(Value::as_object_mut) else {
        return;
    };
    let mut ordered = serde_json::Map::new();
    for (name, member) in std::mem::take(data) {
        let is_result_type = name == "resultType";
        ordered.insert(name, member);
        if is_result_type {
            ordered.insert("encodingFlags".to_string(), json!(flags));
        }
    }
    *data = ordered;
}
