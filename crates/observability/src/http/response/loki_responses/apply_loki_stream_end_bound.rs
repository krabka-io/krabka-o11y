use super::{Value, loki_stream_entry_timestamp, update_loki_stream_values};

pub(crate) fn apply_loki_stream_end_bound(value: &mut Value, end_exclusive: Option<i64>) {
    let Some(end_exclusive) = end_exclusive else {
        return;
    };
    update_loki_stream_values(value, |values| {
        values.retain(|entry| {
            loki_stream_entry_timestamp(entry).is_none_or(|timestamp| timestamp < end_exclusive)
        });
    });
}
