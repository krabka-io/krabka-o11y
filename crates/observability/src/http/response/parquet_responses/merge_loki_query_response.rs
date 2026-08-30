use super::{Value, json, merge_loki_query_stats};

pub(crate) fn merge_loki_query_response(target: &mut Value, source: &Value) {
    if let Some(source_result) = source
        .pointer("/data/result")
        .and_then(Value::as_array)
        .cloned()
        && let Some(target_result) = target
            .pointer_mut("/data/result")
            .and_then(Value::as_array_mut)
    {
        target_result.extend(source_result);
    }

    if let Some(source_stats) = source.pointer("/data/stats") {
        merge_loki_query_stats(&mut target["data"]["stats"], source_stats);
    }

    if let Some(source_warnings) = source.get("warnings").and_then(Value::as_array).cloned() {
        let warnings = target
            .as_object_mut()
            .expect("Loki response is an object")
            .entry("warnings")
            .or_insert_with(|| json!([]));
        if let Some(target_warnings) = warnings.as_array_mut() {
            target_warnings.extend(source_warnings);
        }
    }
}
