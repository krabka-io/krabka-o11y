use super::{Value, add_loki_query_stat_field};

pub(crate) fn merge_loki_query_stats(target: &mut Value, source: &Value) {
    for pointer in [
        "/ingester/compressedBytes",
        "/ingester/decompressedBytes",
        "/ingester/decompressedLines",
        "/ingester/headChunkBytes",
        "/ingester/headChunkLines",
        "/ingester/totalBatches",
        "/ingester/totalChunksMatched",
        "/ingester/totalDuplicates",
        "/ingester/totalLinesSent",
        "/ingester/totalReached",
        "/store/compressedBytes",
        "/store/decompressedBytes",
        "/store/decompressedLines",
        "/store/totalChunksRef",
        "/store/totalChunksDownloaded",
        "/store/totalDuplicates",
        "/summary/totalBytesProcessed",
        "/summary/totalLinesProcessed",
    ] {
        add_loki_query_stat_field(target, source, pointer);
    }
    let Some(source_queue) = source.pointer("/summary/queueTime").and_then(Value::as_f64) else {
        return;
    };
    let Some(target_queue) = target.pointer_mut("/summary/queueTime") else {
        return;
    };
    if source_queue > target_queue.as_f64().unwrap_or_default() {
        *target_queue = serde_json::json!(source_queue);
    }
}
