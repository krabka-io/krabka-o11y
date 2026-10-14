use super::{BTreeMap, Labels, LokiStreamEntry, Value, json, sort_loki_stream_values};

/// Regroups folded streams into the `categorize-labels` encoding.
///
/// A stream arrives keyed by the labels the default encoding shows, which
/// already has each entry's structured metadata and parsed labels folded in.
/// Lifting those back out leaves the stream's own labels, and the streams that
/// were split only by a metadata value become one stream again -- which is what
/// Loki returns under this encoding.
pub(crate) fn categorized_loki_stream_results(
    streams: BTreeMap<Labels, Vec<LokiStreamEntry>>,
) -> Vec<Value> {
    let mut categorized: BTreeMap<Labels, Vec<LokiStreamEntry>> = BTreeMap::new();
    for stream_entry in streams {
        for entry in stream_entry.1 {
            let mut stream = stream_entry.0.clone();
            for name in entry.categorized_label_names() {
                stream.remove(name);
            }
            categorized.entry(stream).or_default().push(entry);
        }
    }
    // Merging two folded streams interleaves their entries, so the order the
    // scan established has to be re-established over the merged run.
    sort_loki_stream_values(&mut categorized);

    categorized
        .into_iter()
        .map(|categorized_entry| {
            json!({
                "stream": categorized_entry.0,
                "values": categorized_entry.1
                    .iter()
                    .map(LokiStreamEntry::categorized_value)
                    .collect::<Vec<_>>(),
            })
        })
        .collect()
}
