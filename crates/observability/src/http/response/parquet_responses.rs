use krabka_units::convert::ByteSizeExt;

use crate::{
    ActiveLogDeleteFilter, ArrowWriter, BTreeMap, BlockDescriptor, ByteSize, CompactionFrontier,
    HttpQueryError, LOKI_PARQUET_CONTENT_TYPE, Labels, MapArray, MapBuilder, MetricQuery,
    MetricWindow, RangeAggregation, RecordBatch, Response, StatusCode, StreamPlan, StringBuilder,
    TimeRange, Value, WalLogRecord, append_matching_hot_metric_record, eval_times,
    format_metric_samples, json, json_response, loki_query_stats, matching_loki_stream_entry,
    unix_ns_string_to_loki_seconds,
};

pub(crate) fn add_loki_query_stat_field(target: &mut Value, source: &Value, pointer: &str) {
    let Some(addend) = source.pointer(pointer).and_then(Value::as_u64) else {
        return;
    };
    let Some(current) = target.pointer_mut(pointer) else {
        return;
    };
    let total = current.as_u64().unwrap_or_default().saturating_add(addend);
    *current = json!(total);
}

pub(crate) fn add_loki_query_stats(mut value: Value) -> Value {
    if value
        .pointer("/data/stats")
        .and_then(Value::as_object)
        .is_none()
    {
        value["data"]["stats"] = loki_query_stats();
    }
    value
}

pub(crate) fn count_loki_metric_result_scan_lines(value: &Value, query: &MetricQuery) -> u64 {
    if matches!(query.aggregation, RangeAggregation::AbsentOverTime) {
        return 0;
    }
    count_loki_metric_result_samples(value)
}

pub(crate) fn loki_metric_sample_timestamp_key(sample: &Value) -> Option<String> {
    sample
        .as_array()
        .and_then(|sample| sample.first())
        .map(Value::to_string)
}

pub(crate) fn loki_sparse_success() -> Response {
    json_response(StatusCode::OK, &json!({ "status": "success" }))
}

pub(crate) fn loki_success(data: impl serde::Serialize) -> Response {
    json_response(StatusCode::OK, &loki_success_value(data))
}

pub(crate) fn loki_success_value(data: impl serde::Serialize) -> Value {
    json!({
        "status": "success",
        "data": data,
    })
}

pub(crate) fn planned_block_bytes(plan: &StreamPlan) -> ByteSize {
    planned_block_bytes_for_blocks(&plan.blocks)
}

pub(crate) fn planned_block_bytes_for_blocks(blocks: &[BlockDescriptor]) -> ByteSize {
    blocks.iter().map(|block| block.size).sum()
}

pub(crate) fn add_loki_query_stats_for_metric_plan(
    mut value: Value,
    plan: &StreamPlan,
    query: &MetricQuery,
) -> Value {
    let bytes = planned_block_bytes(plan);
    let chunks = u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX);
    let samples = count_loki_metric_result_scan_lines(&value, query);
    let mut stats = loki_query_stats();
    let (store_lines, ingester_lines) = if chunks == 0 {
        (0, samples)
    } else {
        (samples, 0)
    };
    populate_loki_query_scan_stats(&mut stats, bytes, store_lines, ingester_lines, chunks);
    value["data"]["stats"] = stats;
    value
}

pub(crate) fn add_loki_query_stats_for_stream_plan(mut value: Value, plan: &StreamPlan) -> Value {
    let bytes = planned_block_bytes(plan);
    let chunks = u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX);
    let lines = count_loki_stream_result_lines(&value);
    let mut stats = loki_query_stats();
    let (store_lines, ingester_lines) = if chunks == 0 { (0, lines) } else { (lines, 0) };
    populate_loki_query_scan_stats(&mut stats, bytes, store_lines, ingester_lines, chunks);
    value["data"]["stats"] = stats;
    value
}

pub(crate) fn consume_hot_metric_sample(
    hot_counts: &mut BTreeMap<HotSampleKey, u64>,
    labels: &Labels,
    sample: &Value,
) -> bool {
    let Some(timestamp_key) = loki_metric_sample_timestamp_key(sample) else {
        return false;
    };
    let key = HotSampleKey {
        labels: labels.clone(),
        timestamp_key,
    };
    let Some(count) = hot_counts.get_mut(&key) else {
        return false;
    };
    if *count == 0 {
        return false;
    }
    *count -= 1;
    true
}

pub(crate) fn count_loki_metric_result_samples(value: &Value) -> u64 {
    let Some(results) = value.pointer("/data/result").and_then(Value::as_array) else {
        return 0;
    };
    results
        .iter()
        .map(|result| {
            if let Some(values) = result.get("values").and_then(Value::as_array) {
                u64::try_from(values.len()).unwrap_or(u64::MAX)
            } else {
                u64::from(result.get("value").is_some())
            }
        })
        .fold(0_u64, u64::saturating_add)
}

pub(crate) fn count_loki_stream_result_lines(value: &Value) -> u64 {
    value
        .pointer("/data/result")
        .and_then(Value::as_array)
        .map_or(0, |streams| {
            streams
                .iter()
                .filter_map(|stream| stream.get("values").and_then(Value::as_array))
                .map(|values| u64::try_from(values.len()).unwrap_or(u64::MAX))
                .fold(0_u64, u64::saturating_add)
        })
}

pub(crate) fn json_object_to_labels(value: &Value) -> Option<Labels> {
    value.as_object().map(|object| {
        let mut labels = Labels::new();
        for entry in object {
            if let Some(value) = entry.1.as_str() {
                labels.insert(entry.0.clone(), value.to_string());
            }
        }
        labels
    })
}

pub(crate) fn loki_parquet_batch_response(batch: &RecordBatch) -> Result<Response, HttpQueryError> {
    let mut body = Vec::new();
    {
        let mut writer = ArrowWriter::try_new(&mut body, batch.schema(), None)?;
        writer.write(batch)?;
        writer.close()?;
    }
    Ok(Response::builder()
        .status(StatusCode::OK)
        .header("content-type", LOKI_PARQUET_CONTENT_TYPE)
        .body(body.into())
        .expect("Parquet response headers are valid"))
}

pub(crate) fn loki_parquet_label_array(
    label_sets: &[Vec<krabka_blockstore::LabelPair>],
) -> Result<MapArray, HttpQueryError> {
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for labels in label_sets {
        for label in labels {
            builder.keys().append_value(&label.name);
            builder.values().append_value(&label.value);
        }
        builder.append(true)?;
    }
    Ok(builder.finish())
}

pub(crate) fn merge_loki_query_response(target: &mut Value, source: &Value) {
    if let Some(source_result) = source
        .pointer("/data/result")
        .and_then(Value::as_array)
        .cloned()
        && let Some(target_result) = target
            .pointer_mut("/data/result")
            .and_then(Value::as_array_mut)
    {
        target_result.extend(source_result);
    }

    if let Some(source_stats) = source.pointer("/data/stats") {
        merge_loki_query_stats(&mut target["data"]["stats"], source_stats);
    }

    if let Some(source_warnings) = source.get("warnings").and_then(Value::as_array).cloned() {
        let warnings = target
            .as_object_mut()
            .expect("Loki response is an object")
            .entry("warnings")
            .or_insert_with(|| json!([]));
        if let Some(target_warnings) = warnings.as_array_mut() {
            target_warnings.extend(source_warnings);
        }
    }
}

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

#[derive(Clone, Copy)]
pub(crate) struct MetricEvaluationWindow {
    pub(crate) range: TimeRange,
    pub(crate) step_ns: i64,
}

pub(crate) fn add_loki_query_stats_for_metric_plan_with_hot_tail<R: Borrow<WalLogRecord> + Sync>(
    mut value: Value,
    plan: &StreamPlan,
    query: &MetricQuery,
    hot_tail: &[R],
    frontier: &CompactionFrontier,
    evaluation: MetricEvaluationWindow,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Value {
    let bytes = planned_block_bytes(plan);
    let chunks = u64::try_from(plan.blocks.len()).unwrap_or(u64::MAX);
    let samples = count_loki_metric_result_scan_lines(&value, query);
    let ingester_samples = count_loki_metric_result_hot_tail_samples(
        &value,
        plan,
        query,
        hot_tail,
        frontier,
        evaluation,
        delete_filters,
    );
    let store_samples = samples.saturating_sub(ingester_samples);
    let mut stats = loki_query_stats();
    populate_loki_query_scan_stats(&mut stats, bytes, store_samples, ingester_samples, chunks);
    value["data"]["stats"] = stats;
    value
}

pub(crate) fn add_loki_query_stats_for_stream_blocks_with_hot_tail<
    R: Borrow<WalLogRecord> + Sync,
>(
    mut value: Value,
    blocks: &[BlockDescriptor],
    plan: &StreamPlan,
    hot_tail: &[R],
    frontier: &CompactionFrontier,
) -> Value {
    let bytes = planned_block_bytes_for_blocks(blocks);
    let chunks = u64::try_from(blocks.len()).unwrap_or(u64::MAX);
    let lines = count_loki_stream_result_lines(&value);
    let ingester_lines = count_loki_stream_result_hot_tail_lines(&value, plan, hot_tail, frontier);
    let store_lines = lines.saturating_sub(ingester_lines);
    let mut stats = loki_query_stats();
    populate_loki_query_scan_stats(&mut stats, bytes, store_lines, ingester_lines, chunks);
    value["data"]["stats"] = stats;
    value
}

pub(crate) fn add_loki_query_stats_for_stream_plan_with_hot_tail<R: Borrow<WalLogRecord> + Sync>(
    value: Value,
    plan: &StreamPlan,
    hot_tail: &[R],
    frontier: &CompactionFrontier,
) -> Value {
    add_loki_query_stats_for_stream_blocks_with_hot_tail(
        value,
        &plan.blocks,
        plan,
        hot_tail,
        frontier,
    )
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct HotSampleKey {
    pub(crate) labels: Labels,
    pub(crate) timestamp_key: String,
}

pub(crate) fn count_loki_metric_result_hot_tail_samples<R: Borrow<WalLogRecord> + Sync>(
    value: &Value,
    plan: &StreamPlan,
    query: &MetricQuery,
    hot_tail: &[R],
    frontier: &CompactionFrontier,
    evaluation: MetricEvaluationWindow,
    delete_filters: &[ActiveLogDeleteFilter],
) -> u64 {
    if matches!(query.aggregation, RangeAggregation::AbsentOverTime) {
        return 0;
    }

    let eval_times = eval_times(evaluation.range, evaluation.step_ns);
    let mut hot_samples = BTreeMap::new();
    for record in hot_tail {
        let record: &WalLogRecord = record.borrow();
        append_matching_hot_metric_record(
            &mut hot_samples,
            plan,
            record,
            frontier,
            MetricWindow {
                query,
                eval_times: &eval_times,
                range_ns: query.range_ns.0,
                delete_filters,
            },
        )
        .ok();
    }

    let mut hot_counts: BTreeMap<HotSampleKey, u64> = BTreeMap::new();
    for series in format_metric_samples(hot_samples, query) {
        for [timestamp_ns, _] in series.values {
            let key = HotSampleKey {
                labels: series.labels.clone(),
                timestamp_key: unix_ns_string_to_loki_seconds(&timestamp_ns).to_string(),
            };
            hot_counts
                .entry(key)
                .and_modify(|count| *count = count.saturating_add(1))
                .or_insert(1);
        }
    }

    let Some(results) = value.pointer("/data/result").and_then(Value::as_array) else {
        return 0;
    };
    let mut matched = 0_u64;
    for result in results {
        let Some(labels) = result.get("metric").and_then(json_object_to_labels) else {
            continue;
        };
        if let Some(values) = result.get("values").and_then(Value::as_array) {
            for sample in values {
                if consume_hot_metric_sample(&mut hot_counts, &labels, sample) {
                    matched = matched.saturating_add(1);
                }
            }
        } else if let Some(sample) = result.get("value")
            && consume_hot_metric_sample(&mut hot_counts, &labels, sample)
        {
            matched = matched.saturating_add(1);
        }
    }
    matched
}

use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
};

struct MatchedLine<'a> {
    labels: &'a Labels,
    line: &'a str,
}

pub(crate) fn count_loki_stream_result_hot_tail_lines<R: Borrow<WalLogRecord> + Sync>(
    value: &Value,
    plan: &StreamPlan,
    hot_tail: &[R],
    frontier: &CompactionFrontier,
) -> u64 {
    let Some(streams) = value.pointer("/data/result").and_then(Value::as_array) else {
        return 0;
    };
    // Count only the returned multiset. Borrow its lines instead of cloning
    // every hot record's labels, timestamp and text into another ordered map.
    let mut wanted: HashMap<Labels, HashMap<i64, HashMap<&str, u64>>> = HashMap::new();
    let mut returned_timestamps = HashSet::new();
    for stream in streams {
        let Some(labels) = stream.get("stream").and_then(json_object_to_labels) else {
            continue;
        };
        let Some(values) = stream.get("values").and_then(Value::as_array) else {
            continue;
        };
        let timestamps = wanted.entry(labels).or_default();
        for value in values {
            let Some(timestamp) = value.get(0).and_then(Value::as_str) else {
                continue;
            };
            let Some(line) = value.get(1).and_then(Value::as_str) else {
                continue;
            };
            let Ok(ts_ns) = timestamp.parse::<i64>() else {
                continue;
            };
            // Hot entries render canonical decimal timestamps. Preserve the
            // string match: "010" and "+10" cannot account for their "10".
            if timestamp != ts_ns.to_string() {
                continue;
            }
            returned_timestamps.insert(ts_ns);
            let count = timestamps
                .entry(ts_ns)
                .or_default()
                .entry(line)
                .or_default();
            *count = count.saturating_add(1);
        }
    }

    let mut labels_cache: HashMap<&Labels, Option<Labels>> = HashMap::new();
    let mut matched = 0_u64;
    for record in hot_tail {
        let record: &WalLogRecord = record.borrow();
        if record.tenant != plan.tenant
            || !returned_timestamps.contains(&record.timestamp_ns)
            || frontier.is_compacted(record)
            || record.timestamp_ns < plan.time_range.start_ns
            || record.timestamp_ns > plan.time_range.end_ns
        {
            continue;
        }
        let evaluated;
        let matched_line =
            if plan.query.pipeline.is_empty() && record.structured_metadata.is_empty() {
                // Without a pipeline or per-record metadata, output labels depend
                // only on the source labels. Keep the normal evaluator's level
                // discovery, but evaluate each distinct label set just once.
                let labels = labels_cache.entry(&record.labels).or_insert_with(|| {
                    matching_loki_stream_entry(
                        &plan.query,
                        &record.labels,
                        &record.line,
                        &record.structured_metadata,
                        record.timestamp_ns,
                        false,
                    )
                    .map(|entry| entry.labels)
                });
                let Some(labels) = labels.as_ref() else {
                    continue;
                };
                Some(MatchedLine {
                    labels,
                    line: record.line.as_str(),
                })
            } else {
                evaluated = matching_loki_stream_entry(
                    &plan.query,
                    &record.labels,
                    &record.line,
                    &record.structured_metadata,
                    record.timestamp_ns,
                    false,
                );
                let Some(evaluated_entry) = evaluated.as_ref() else {
                    continue;
                };
                Some(MatchedLine {
                    labels: &evaluated_entry.labels,
                    line: evaluated_entry.entry.line.as_str(),
                })
            };
        let Some(matched_line) = matched_line else {
            continue;
        };
        let Some(count) = wanted
            .get_mut(matched_line.labels)
            .and_then(|timestamps| timestamps.get_mut(&record.timestamp_ns))
            .and_then(|lines| lines.get_mut(matched_line.line))
        else {
            continue;
        };
        if *count > 0 {
            *count -= 1;
            matched = matched.saturating_add(1);
        }
    }
    matched
}

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
