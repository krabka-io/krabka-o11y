use super::*;

/// `populate_loki_query_scan_stats` fills Loki's stats block, and the two
/// per-source sections appear only when that source contributed. An empty
/// `ingester` or `store` object would tell a client the source was
/// consulted and returned nothing, which is a different claim from not
/// having been consulted -- Grafana renders the two differently.
///
/// The summary is unconditional and sums BOTH sources, so it is checked
/// with each source alone as well as with both: with one contributing,
/// a sum that dropped the other term still reads correctly.
#[test]
pub(crate) fn loki_scan_stats_report_only_the_sources_that_contributed() {
    let fill = |store_lines, ingester_lines, chunks| {
        let mut stats = serde_json::json!({});
        super::super::prelude::populate_loki_query_scan_stats(
            &mut stats,
            krabka_units::bytes(4_096),
            store_lines,
            ingester_lines,
            chunks,
        );
        stats
    };

    // Both sources contributed.
    let both = fill(7, 3, 2);
    check!(both["ingester"]["decompressedLines"] == 3);
    check!(both["ingester"]["totalLinesSent"] == 3);
    check!(both["store"]["decompressedLines"] == 7);
    check!(both["store"]["totalChunksRef"] == 2);
    check!(both["store"]["totalChunksDownloaded"] == 2);
    check!(both["store"]["compressedBytes"] == 4_096);
    check!(both["store"]["decompressedBytes"] == 4_096);
    check!(both["summary"]["totalBytesProcessed"] == 4_096);
    check!(
        both["summary"]["totalLinesProcessed"] == 10,
        "the summary sums store and ingester"
    );

    // Only the ingester: no store section at all, not an empty one.
    let hot = fill(0, 3, 0);
    check!(hot["ingester"]["decompressedLines"] == 3);
    check!(hot.get("store").is_none(), "absent, not empty");
    check!(hot["summary"]["totalLinesProcessed"] == 3);

    // Only the store: no ingester section.
    let cold = fill(7, 0, 2);
    check!(cold["store"]["decompressedLines"] == 7);
    check!(cold.get("ingester").is_none(), "absent, not empty");
    check!(cold["summary"]["totalLinesProcessed"] == 7);

    // Neither: the summary still reports, at zero.
    let empty = fill(0, 0, 0);
    check!(empty.get("store").is_none());
    check!(empty.get("ingester").is_none());
    check!(empty["summary"]["totalLinesProcessed"] == 0);
    check!(
        empty["summary"]["totalBytesProcessed"] == 4_096,
        "bytes are unconditional"
    );

    // The store section is gated on CHUNKS, not on lines: a chunk that
    // matched no lines was still downloaded and still cost bytes.
    let scanned_nothing = fill(0, 0, 2);
    check!(scanned_nothing["store"]["totalChunksRef"] == 2);
    check!(scanned_nothing["store"]["decompressedLines"] == 0);
}
