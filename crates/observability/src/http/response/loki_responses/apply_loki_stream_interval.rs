use super::*;

pub(crate) fn apply_loki_stream_interval(value: &mut Value, interval: Option<i64>) {
    let Some(interval) = interval else {
        return;
    };
    if interval == 0 {
        return;
    }
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
        let mut next_timestamp = None;
        values.retain(|entry| {
            let Some(timestamp) = entry
                .as_array()
                .and_then(|entry| entry.first())
                .and_then(Value::as_str)
                .and_then(|timestamp| timestamp.parse::<i64>().ok())
            else {
                return true;
            };
            match next_timestamp {
                Some(next) if timestamp < next => false,
                _ => {
                    next_timestamp = timestamp.checked_add(interval);
                    true
                }
            }
        });
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });
}
