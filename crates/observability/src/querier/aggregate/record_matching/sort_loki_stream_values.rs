use super::*;

pub(crate) fn sort_loki_stream_values(streams: &mut BTreeMap<Labels, Vec<[String; 2]>>) {
    for values in streams.values_mut() {
        values.sort_by_key(|[timestamp, _]| timestamp.parse::<i64>().unwrap_or(i64::MAX));
    }
}
