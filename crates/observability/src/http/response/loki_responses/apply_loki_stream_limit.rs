use super::*;

pub(crate) fn apply_loki_stream_limit(mut value: Value, limit: Option<usize>) -> Value {
    let Some(limit) = limit else {
        return value;
    };
    if value.pointer("/data/resultType").and_then(Value::as_str) != Some("streams") {
        return value;
    }

    let Some(streams) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return value;
    };

    let mut remaining = limit;
    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        // No `remaining == 0` short circuit: `truncate(0)` already clears.
        if values.len() > remaining {
            values.truncate(remaining);
            remaining = 0;
        } else {
            remaining -= values.len();
        }
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });

    value
}
