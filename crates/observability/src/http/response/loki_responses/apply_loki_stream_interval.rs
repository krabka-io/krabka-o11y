use super::{Value, loki_stream_entry_timestamp, update_loki_stream_values};

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
            let Some(timestamp) = loki_stream_entry_timestamp(entry) else {
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
