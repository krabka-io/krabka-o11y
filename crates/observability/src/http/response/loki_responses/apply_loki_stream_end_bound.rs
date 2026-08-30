use super::*;

pub(crate) fn apply_loki_stream_end_bound(value: &mut Value, end_exclusive: Option<i64>) {
    let Some(end_exclusive) = end_exclusive else {
        return;
    };
    let Some(streams) = value
        .pointer_mut("/data/result")
        .and_then(Value::as_array_mut)
    else {
        return;
    };

    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        values.retain(|entry| {
            entry
                .as_array()
                .and_then(|entry| entry.first())
                .and_then(Value::as_str)
                .and_then(|timestamp| timestamp.parse::<i64>().ok())
                .is_none_or(|timestamp| timestamp < end_exclusive)
        });
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });
}
