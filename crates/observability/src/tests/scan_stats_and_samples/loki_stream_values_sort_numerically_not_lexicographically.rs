use super::*;

/// `sort_loki_stream_values` orders each stream's entries by timestamp.
/// The timestamps are decimal strings, so a lexicographic sort would put
/// "1000" before "999" -- the fixture crosses that boundary deliberately.
/// An unparseable timestamp sorts last rather than first, so a malformed
/// entry does not claim to be the oldest line in the stream.
#[test]
pub(crate) fn loki_stream_values_sort_numerically_not_lexicographically() {
    let entry = |timestamp: &str| [timestamp.to_string(), "line".to_string()];
    let mut streams = BTreeMap::new();
    let mut labels = Labels::default();
    labels.insert("app".to_string(), "api".to_string());
    streams.insert(
        labels.clone(),
        vec![
            entry("1000"),
            entry("999"),
            entry("nonsense"),
            entry("10000"),
            entry("2"),
        ],
    );

    super::super::prelude::sort_loki_stream_values(&mut streams);

    let order = streams[&labels]
        .iter()
        .map(|[timestamp, _]| timestamp.as_str())
        .collect::<Vec<_>>();
    check!(
        order == vec!["2", "999", "1000", "10000", "nonsense"],
        "numeric order, with the unparseable entry last"
    );
}
