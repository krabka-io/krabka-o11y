use super::{BTreeMap, Labels, LokiStreamEntry};

pub(crate) fn count_stream_map_lines(
    streams: &BTreeMap<Labels, Vec<LokiStreamEntry>>,
    end_exclusive: Option<i64>,
) -> usize {
    streams
        .values()
        .map(|values| {
            values
                .iter()
                .filter(|entry| {
                    end_exclusive.is_none_or(|end_exclusive| {
                        entry
                            .parsed_timestamp_ns()
                            .is_none_or(|timestamp| timestamp < end_exclusive)
                    })
                })
                .count()
        })
        .fold(0_usize, usize::saturating_add)
}
