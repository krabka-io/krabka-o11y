use std::time::Duration;

use crate::{Value, json};

pub(crate) fn loki_query_stats() -> Value {
    json!({
        "ingester": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": 0,
            "headChunkBytes": 0,
            "headChunkLines": 0,
            "totalBatches": 0,
            "totalChunksMatched": 0,
            "totalDuplicates": 0,
            "totalLinesSent": 0,
            "totalReached": 0
        },
        "store": {
            "compressedBytes": 0,
            "decompressedBytes": 0,
            "decompressedLines": 0,
            "chunksDownloadTime": 0.0,
            "totalChunksRef": 0,
            "totalChunksDownloaded": 0,
            "totalDuplicates": 0
        },
        "summary": {
            "bytesProcessedPerSecond": 0,
            "execTime": 0.0,
            "linesProcessedPerSecond": 0,
            "queueTime": 0.0,
            "totalBytesProcessed": 0,
            "totalLinesProcessed": 0
        }
    })
}

pub(crate) fn populate_loki_query_execution_stats(
    response: &mut Value,
    elapsed: Duration,
    queue_time: Duration,
) {
    let Some(summary) = response.pointer_mut("/data/stats/summary") else {
        return;
    };
    let seconds = elapsed.as_secs_f64();
    let bytes = summary["totalBytesProcessed"].as_u64().unwrap_or(0);
    let lines = summary["totalLinesProcessed"].as_u64().unwrap_or(0);
    summary["execTime"] = json!(seconds);
    summary["queueTime"] = json!(queue_time.as_secs_f64());
    if seconds > 0.0 {
        summary["bytesProcessedPerSecond"] = json!((bytes as f64 / seconds) as u64);
        summary["linesProcessedPerSecond"] = json!((lines as f64 / seconds) as u64);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execution_stats_derive_rates_from_scan_totals() {
        let mut response = json!({ "data": { "stats": loki_query_stats() } });
        response["data"]["stats"]["summary"]["totalBytesProcessed"] = json!(10);
        response["data"]["stats"]["summary"]["totalLinesProcessed"] = json!(4);
        populate_loki_query_execution_stats(
            &mut response,
            Duration::from_secs(2),
            Duration::from_millis(25),
        );
        assert_eq!(
            response.pointer("/data/stats/summary/bytesProcessedPerSecond"),
            Some(&json!(5))
        );
        assert_eq!(
            response.pointer("/data/stats/summary/linesProcessedPerSecond"),
            Some(&json!(2))
        );
        assert_eq!(
            response.pointer("/data/stats/summary/execTime"),
            Some(&json!(2.0))
        );
        assert_eq!(
            response.pointer("/data/stats/summary/queueTime"),
            Some(&json!(0.025))
        );
    }
}
