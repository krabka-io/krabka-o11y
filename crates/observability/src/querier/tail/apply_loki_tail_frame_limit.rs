use super::*;

pub(crate) fn apply_loki_tail_frame_limit(mut frame: Value, limit: Option<usize>) -> Value {
    let Some(limit) = limit else {
        return frame;
    };
    let Some(streams) = frame.get_mut("streams").and_then(Value::as_array_mut) else {
        return frame;
    };

    let mut remaining = limit;
    for stream in streams.iter_mut() {
        let Some(values) = stream.get_mut("values").and_then(Value::as_array_mut) else {
            continue;
        };
        // No guards: `truncate` is a no-op when the stream is shorter than
        // what is left, and `truncate(0)` clears exactly as `clear()` would.
        values.truncate(remaining);
        remaining = remaining.saturating_sub(values.len());
    }
    streams.retain(|stream| {
        stream
            .get("values")
            .and_then(Value::as_array)
            .is_some_and(|values| !values.is_empty())
    });

    frame
}
