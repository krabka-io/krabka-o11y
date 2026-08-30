use super::*;

pub(crate) fn count_loki_metric_result_hot_tail_samples(
    value: &Value,
    plan: &StreamPlan,
    query: &MetricQuery,
    hot_tail: &[WalLogRecord],
    frontier: &CompactionFrontier,
    evaluation: (TimeRange, i64),
    delete_filters: &[ActiveLogDeleteFilter],
) -> u64 {
    if matches!(query.aggregation, RangeAggregation::AbsentOverTime) {
        return 0;
    }

    let (eval_range, step_ns) = evaluation;
    let eval_times = eval_times(eval_range, step_ns);
    let mut hot_samples = BTreeMap::new();
    for record in hot_tail {
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

    let mut hot_counts: BTreeMap<(Labels, String), u64> = BTreeMap::new();
    for (labels, values) in format_metric_samples(hot_samples, query) {
        for [timestamp_ns, _] in values {
            let key = (
                labels.clone(),
                unix_ns_string_to_loki_seconds(&timestamp_ns).to_string(),
            );
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
