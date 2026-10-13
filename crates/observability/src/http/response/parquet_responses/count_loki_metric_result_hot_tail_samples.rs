use std::borrow::Borrow;

use super::{
    ActiveLogDeleteFilter, BTreeMap, CompactionFrontier, Labels, MetricEvaluationWindow,
    MetricQuery, MetricWindow, RangeAggregation, StreamPlan, Value, WalLogRecord,
    append_matching_hot_metric_record, consume_hot_metric_sample, eval_times,
    format_metric_samples, json_object_to_labels, unix_ns_string_to_loki_seconds,
};

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
