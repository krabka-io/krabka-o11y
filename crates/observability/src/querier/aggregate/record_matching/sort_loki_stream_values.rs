use super::{BTreeMap, Labels, LokiStreamEntry};

pub(crate) fn sort_loki_stream_values(streams: &mut BTreeMap<Labels, Vec<LokiStreamEntry>>) {
    for values in streams.values_mut() {
        values.sort_by_key(|entry| entry.parsed_timestamp_ns().unwrap_or(i64::MAX));
    }
}
