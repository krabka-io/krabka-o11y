use super::{json, TsdbStats, Value, named_tsdb_stats_json};

pub(crate) fn tsdb_status_json(stats: TsdbStats, limit: Option<usize>) -> Value {
    json!({
        "headStats": {
            "numSeries": stats.head_stats.num_series,
            "chunkCount": stats.head_stats.num_chunks,
            "numSamples": stats.head_stats.num_samples,
            "minTime": stats.head_stats.min_time,
            "maxTime": stats.head_stats.max_time,
        },
        "seriesCountByMetricName": named_tsdb_stats_json(stats.series_count_by_metric_name, limit),
        "labelValueCountByLabelName": named_tsdb_stats_json(stats.label_value_count_by_label_name, limit),
        "memoryInBytesByLabelName": named_tsdb_stats_json(stats.memory_in_bytes_by_label_name, limit),
        "seriesCountByLabelValuePair": named_tsdb_stats_json(stats.series_count_by_label_value_pair, limit),
    })
}
