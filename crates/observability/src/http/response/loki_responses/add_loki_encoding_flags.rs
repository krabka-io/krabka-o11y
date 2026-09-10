use super::{Value, json};

/// Echoes the request's encoding flags into a `streams` response.
///
/// Loki reports them from the `streams` marshaller alone: a `matrix` or
/// `vector` answer to a request that carried the header comes back without
/// them.
pub(crate) fn add_loki_encoding_flags(value: &mut Value, flags: &[String]) {
    if flags.is_empty()
        || value.pointer("/data/resultType").and_then(Value::as_str) != Some("streams")
    {
        return;
    }
    value["data"]["encodingFlags"] = json!(flags);
}
