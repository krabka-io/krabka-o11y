use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
};

use super::{
    CompactionFrontier, Labels, StreamPlan, Value, WalLogRecord, json_object_to_labels,
    matching_loki_stream_entry,
};

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
            let (Some(timestamp), Some(line)) = (
                value.get(0).and_then(Value::as_str),
                value.get(1).and_then(Value::as_str),
            ) else {
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
        let (labels, line) =
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
                    .map(|(labels, _)| labels)
                });
                let Some(labels) = labels.as_ref() else {
                    continue;
                };
                (labels, record.line.as_str())
            } else {
                evaluated = matching_loki_stream_entry(
                    &plan.query,
                    &record.labels,
                    &record.line,
                    &record.structured_metadata,
                    record.timestamp_ns,
                    false,
                );
                let Some((labels, entry)) = evaluated.as_ref() else {
                    continue;
                };
                (labels, entry.line.as_str())
            };
        let Some(count) = wanted
            .get_mut(labels)
            .and_then(|timestamps| timestamps.get_mut(&record.timestamp_ns))
            .and_then(|lines| lines.get_mut(line))
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
