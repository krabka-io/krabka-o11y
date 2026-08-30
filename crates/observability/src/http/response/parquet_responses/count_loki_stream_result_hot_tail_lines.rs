use super::*;

pub(crate) fn count_loki_stream_result_hot_tail_lines(
    value: &Value,
    plan: &StreamPlan,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
) -> u64 {
    let mut hot_counts: BTreeMap<(Labels, String, String), u64> = BTreeMap::new();
    for record in hot_tail {
        if record.tenant != plan.tenant
            || frontier.is_compacted(record)
            || record.timestamp_ns < plan.time_range.start_ns
            || record.timestamp_ns > plan.time_range.end_ns
        {
            continue;
        }
        let Some((stream_labels, current_line)) = matching_loki_stream_entry(
            &plan.query,
            &record.labels,
            &record.line,
            &record.structured_metadata,
            record.timestamp_ns,
        ) else {
            continue;
        };
        let key = (stream_labels, record.timestamp_ns.to_string(), current_line);
        hot_counts
            .entry(key)
            .and_modify(|count| *count = count.saturating_add(1))
            .or_insert(1);
    }

    let Some(streams) = value.pointer("/data/result").and_then(Value::as_array) else {
        return 0;
    };
    let mut matched = 0_u64;
    for stream in streams {
        let Some(labels) = stream.get("stream").and_then(json_object_to_labels) else {
            continue;
        };
        let Some(values) = stream.get("values").and_then(Value::as_array) else {
            continue;
        };
        for value in values {
            let Some(pair) = value.as_array() else {
                continue;
            };
            let (Some(timestamp), Some(line)) = (
                pair.first().and_then(Value::as_str),
                pair.get(1).and_then(Value::as_str),
            ) else {
                continue;
            };
            let key = (labels.clone(), timestamp.to_string(), line.to_string());
            let Some(count) = hot_counts.get_mut(&key) else {
                continue;
            };
            if *count == 0 {
                continue;
            }
            *count -= 1;
            matched = matched.saturating_add(1);
        }
    }
    matched
}
