use super::*;

/// The patterns scan drops a row outside the query window, and the window
/// is half-open: a row exactly on the start counts, one exactly on the end
/// does not. Nothing had scanned a block through this endpoint, so the two
/// edges and the `||` joining them to the fingerprint test were all free.
#[tokio::test]
pub(crate) async fn a_patterns_scan_keeps_the_window_half_open() {
    use krabka_blockstore::{BlockKey, LogRow, TimeRange, series_fingerprint, write_log_block};

    let dir = tempfile::tempdir().expect("a temp dir");
    let mut labels = Labels::new();
    labels.insert("app".to_string(), "web".to_string());
    let fingerprint = series_fingerprint(&labels);

    let row = |timestamp_ns, line: &str| LogRow {
        series_fingerprint: fingerprint,
        timestamp_ns,
        line: line.to_string(),
        structured_metadata: BTreeMap::new(),
    };
    // Before the window, on its start, inside, on its end, and past it.
    // The rows outside carry a different line shape, so including one
    // shows up as a second pattern rather than merely a larger count --
    // swapping which rows are kept leaves the count alone.
    let key = BlockKey::new(
        "tenant-a",
        0,
        0,
        0,
        TimeRange::new(0, 100).expect("a valid range"),
    );
    let descriptor = write_log_block(
        dir.path(),
        &key,
        vec![
            row(5, "cache warmed"),
            row(10, "request served"),
            row(20, "request served"),
            row(30, "cache warmed"),
            row(40, "cache warmed"),
        ],
    )
    .expect("the block writes");

    let mut index = BlockIndex::default();
    index.insert(descriptor);
    let mut label_index = LabelIndex::default();
    label_index.insert_series("tenant-a", labels);
    let state = QuerierState::new(dir.path(), label_index, index);

    let mut headers = HeaderMap::new();
    headers.insert("X-Scope-OrgID", "tenant-a".parse().expect("a header value"));
    let value = super::super::prelude::execute_patterns_query(
        &state,
        &headers,
        Some("query=%7Bapp%3D%22web%22%7D&start=10&end=30&step=1h"),
    )
    .await
    .expect("the patterns query runs");

    // One line shape, one bucket, and only the two rows inside the window.
    let data = value["data"].as_array().expect("a data array");
    check!(data.len() == 1, "one pattern: {value}");
    let samples = data[0]["samples"].as_array().expect("a samples array");
    check!(samples.len() == 1, "one bucket: {value}");
    check!(
        samples[0][1] == 2,
        "the row on the start counts and the one on the end does not: {value}"
    );
}
