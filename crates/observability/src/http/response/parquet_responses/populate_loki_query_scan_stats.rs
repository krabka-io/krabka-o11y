use super::{ByteSize, ByteSizeExt, Value, json};

pub(crate) fn populate_loki_query_scan_stats(
    stats: &mut Value,
    scanned: ByteSize,
    store_lines: u64,
    ingester_lines: u64,
    chunks: u64,
) {
    // Loki's stats block reports whole bytes, so the quantity is lowered once
    // here, at the JSON boundary.
    let bytes = scanned.bytes_u64();
    if ingester_lines > 0 {
        stats["ingester"]["decompressedLines"] = json!(ingester_lines);
        stats["ingester"]["totalLinesSent"] = json!(ingester_lines);
    }
    if chunks > 0 {
        stats["store"]["compressedBytes"] = json!(bytes);
        stats["store"]["decompressedBytes"] = json!(bytes);
        stats["store"]["decompressedLines"] = json!(store_lines);
        stats["store"]["totalChunksRef"] = json!(chunks);
        stats["store"]["totalChunksDownloaded"] = json!(chunks);
    }
    stats["summary"]["totalBytesProcessed"] = json!(bytes);
    stats["summary"]["totalLinesProcessed"] = json!(store_lines.saturating_add(ingester_lines));
}
