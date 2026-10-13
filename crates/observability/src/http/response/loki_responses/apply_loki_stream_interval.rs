use super::*;

pub(crate) fn apply_loki_stream_interval(value: &mut Value, interval: Option<i64>) {
    let Some(interval) = interval else {
        return;
    };
    if interval == 0 {
        return;
    }
    update_loki_stream_values(value, |values| {
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
    });
}
