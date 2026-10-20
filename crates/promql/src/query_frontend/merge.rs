use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use krabka_blockstore::SeriesFingerprint;
use krabka_metrics::NativeHistogram;
use promql_parser::parser::LabelModifier;

use super::{MomentReduction, QueryShardReducer, RankReduction};
use crate::{
    PromqlError, PromqlLabels as Labels, QueryResult, RangeSeries, RangeSeriesSample, SampleValue,
    engine::add_compatible_native_histogram,
};
pub(crate) fn label_sort_key(labels: &Labels) -> String {
    labels.iter().fold(String::new(), |mut out, pair| {
        let _ = writeln!(out, "{}={}", pair.name, pair.value);
        out
    })
}

/// Merges range-matrix subquery results back into one Prometheus matrix.
///
/// This function is the query-frontend counterpart to
/// [`crate::plan_range_query`]. It joins the time-split subqueries for the same
/// series. Sharded subqueries contribute distinct series.
/// # Errors
/// Returns an error when metric input is malformed, a limit is exceeded, or the backing WAL, block store, or remote endpoint fails.
pub fn merge_range_query_results(results: Vec<QueryResult>) -> Result<QueryResult, PromqlError> {
    merge_range_query_results_with_reducer(results, QueryShardReducer::Sum)
}

/// The labels a sharded aggregation groups a series by.
///
/// This is the engine's grouping, except that a `by` clause never keeps
/// `__name__` and keeps each value as its UTF-8 text.
pub(crate) fn aggregate_labels(input: &Labels, modifier: Option<&LabelModifier>) -> Labels {
    let grouped = crate::engine::aggregate_labels(input, modifier);
    if !matches!(modifier, Some(LabelModifier::Include(_))) {
        return grouped;
    }
    let mut labels = Labels::new();
    for pair in grouped.iter() {
        if pair.name != "__name__" {
            labels.insert(pair.name, pair.value.as_str());
        }
    }
    labels
}

pub(crate) fn float_samples_by_fingerprint(
    series: Vec<RangeSeries>,
) -> BTreeMap<SeriesFingerprint, BTreeMap<i64, f64>> {
    let mut samples_by_fingerprint = BTreeMap::new();
    for series in series {
        let mut samples_by_timestamp = BTreeMap::new();
        for sample in series.samples {
            if let SampleValue::Float(value) = sample.value {
                samples_by_timestamp.insert(sample.timestamp_ms, value);
            }
        }
        samples_by_fingerprint.insert(series.labels.fingerprint(), samples_by_timestamp);
    }
    samples_by_fingerprint
}

pub(crate) fn scaled_native_histogram(histogram: &NativeHistogram, factor: f64) -> NativeHistogram {
    let mut out = histogram.clone();
    out.zero_count *= factor;
    out.count *= factor;
    out.sum *= factor;
    for count in &mut out.positive_counts {
        *count *= factor;
    }
    for count in &mut out.negative_counts {
        *count *= factor;
    }
    out
}

#[derive(Clone)]
pub(crate) struct RankCandidate {
    pub(crate) fingerprint: SeriesFingerprint,
    pub(crate) labels_key: String,
    pub(crate) sample_index: usize,
    pub(crate) series_index: usize,
    pub(crate) value: f64,
}

/// Orders two rank candidates for a sharded `topk`/`bottomk` range query.
///
/// The first key is the sample value. `Top` puts the highest value first, and
/// `Bottom` puts the lowest value first. Both put a `NaN` last, because
/// Prometheus counts a `NaN` as neither a top value nor a bottom value.
///
/// `compare_k_aggregate_samples` applies the same rule on the unsharded path,
/// and the two keep one rule between them. A `topk` answered from shards and
/// the same `topk` answered whole would otherwise rank a `NaN` differently.
///
/// `total_cmp` alone does not give that result. It ranks a positive `NaN`
/// above every finite value, so `Top` would keep the `NaN` and drop a number.
/// So this function decides the `NaN` cases first, and `total_cmp` compares
/// two numbers only.
///
/// The remaining keys make the reduction deterministic whatever order the
/// shards return the candidates in. They are the canonical label key, then the
/// series position and the sample position of the candidate.
pub(crate) fn compare_rank_candidates(
    kind: RankReduction,
    left: &RankCandidate,
    right: &RankCandidate,
) -> Ordering {
    let left_is_nan = left.value.is_nan();
    let right_is_nan = right.value.is_nan();
    let by_value = if left_is_nan {
        if right_is_nan {
            Ordering::Equal
        } else {
            Ordering::Greater
        }
    } else if right_is_nan {
        Ordering::Less
    } else {
        match kind {
            RankReduction::Top => right.value.total_cmp(&left.value),
            RankReduction::Bottom => left.value.total_cmp(&right.value),
        }
    };
    by_value
        .then_with(|| left.labels_key.cmp(&right.labels_key))
        .then_with(|| left.series_index.cmp(&right.series_index))
        .then_with(|| left.sample_index.cmp(&right.sample_index))
}

pub(crate) fn divide_range_query_results(
    sums: QueryResult,
    counts: QueryResult,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(sum_series) = sums else {
        return Err(PromqlError::Plan(
            "avg query-frontend sum merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(count_series) = counts else {
        return Err(PromqlError::Plan(
            "avg query-frontend count merge requires range matrix results".into(),
        ));
    };
    let mut counts_by_fp = BTreeMap::new();
    for series in count_series {
        counts_by_fp.insert(series.labels.fingerprint(), series);
    }
    let mut avg_series = Vec::new();

    for series in sum_series {
        let Some(count_series) = counts_by_fp.get(&series.labels.fingerprint()) else {
            continue;
        };
        let mut counts_by_ts = BTreeMap::new();
        for sample in &count_series.samples {
            if let SampleValue::Float(value) = &sample.value {
                counts_by_ts.insert(sample.timestamp_ms, *value);
            }
        }
        let samples = series
            .samples
            .into_iter()
            .filter_map(|sample| {
                let ts_ms = sample.timestamp_ms;
                let value = sample.value;
                let count = *counts_by_ts.get(&ts_ms)?;
                if count == 0.0 {
                    return None;
                }
                Some(RangeSeriesSample::new(
                    ts_ms,
                    match value {
                        SampleValue::Float(value) => SampleValue::Float(value / count),
                        SampleValue::Histogram(histogram) => {
                            SampleValue::Histogram(scaled_native_histogram(&histogram, 1.0 / count))
                        }
                    },
                ))
            })
            .collect::<Vec<_>>();
        if !samples.is_empty() {
            avg_series.push(RangeSeries {
                drop_name: false,
                start_timestamps_ms: std::collections::BTreeMap::new(),
                labels: series.labels,
                samples,
            });
        }
    }

    Ok(QueryResult::RangeMatrix(avg_series))
}

pub(crate) fn merge_range_query_results_with_reducer(
    results: Vec<QueryResult>,
    reducer: QueryShardReducer,
) -> Result<QueryResult, PromqlError> {
    let mut by_fp = BTreeMap::<SeriesFingerprint, RangeSeries>::new();

    for result in results {
        let QueryResult::RangeMatrix(series) = result else {
            return Err(PromqlError::Plan(
                "query-frontend range merge requires range matrix subquery results".into(),
            ));
        };
        for mut series in series {
            by_fp
                .entry(series.labels.fingerprint())
                .and_modify(|existing| existing.samples.append(&mut series.samples))
                .or_insert(series);
        }
    }

    let mut series = by_fp.into_values().collect::<Vec<_>>();
    series.sort_by_key(|series| label_sort_key(&series.labels));
    for series in &mut series {
        series.samples.sort_by_key(|sample| sample.timestamp_ms);
        reduce_duplicate_step_samples(&mut series.samples, reducer)?;
    }
    Ok(QueryResult::RangeMatrix(series))
}

pub(crate) fn reduce_duplicate_step_samples(
    samples: &mut Vec<RangeSeriesSample>,
    reducer: QueryShardReducer,
) -> Result<(), PromqlError> {
    let mut merged_samples = Vec::<RangeSeriesSample>::with_capacity(samples.len());
    for sample in samples.drain(..) {
        let ts_ms = sample.timestamp_ms;
        let value = sample.value;
        match merged_samples.last_mut() {
            Some(last)
                if last.timestamp_ms == ts_ms && matches!(&last.value, SampleValue::Float(_)) =>
            {
                let SampleValue::Float(last_value) = &mut last.value else {
                    unreachable!()
                };
                if let SampleValue::Float(value) = value {
                    *last_value = match reducer {
                        QueryShardReducer::First => *last_value,
                        QueryShardReducer::Sum => *last_value + value,
                        QueryShardReducer::Min => last_value.min(value),
                        QueryShardReducer::Max => last_value.max(value),
                    };
                }
            }
            Some(last)
                if last.timestamp_ms == ts_ms
                    && reducer == QueryShardReducer::Sum
                    && matches!(&last.value, SampleValue::Histogram(_)) =>
            {
                let SampleValue::Histogram(last_value) = &mut last.value else {
                    unreachable!()
                };
                if let SampleValue::Histogram(value) = value {
                    add_compatible_native_histogram(last_value, &value)?;
                }
            }
            Some(last) if last.timestamp_ms == ts_ms => {}
            _ => merged_samples.push(RangeSeriesSample::new(ts_ms, value)),
        }
    }
    *samples = merged_samples;
    Ok(())
}

pub(crate) fn reduce_moment_range_query_results(
    sums: QueryResult,
    counts: QueryResult,
    sum_squares: QueryResult,
    kind: MomentReduction,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(sum_series) = sums else {
        return Err(PromqlError::Plan(
            "moment query-frontend sum merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(count_series) = counts else {
        return Err(PromqlError::Plan(
            "moment query-frontend count merge requires range matrix results".into(),
        ));
    };
    let QueryResult::RangeMatrix(sum_squares_series) = sum_squares else {
        return Err(PromqlError::Plan(
            "moment query-frontend sum-squares merge requires range matrix results".into(),
        ));
    };
    let counts_by_fp = float_samples_by_fingerprint(count_series);
    let sum_squares_by_fp = float_samples_by_fingerprint(sum_squares_series);
    let mut out_series = Vec::new();

    for series in sum_series {
        let fingerprint = series.labels.fingerprint();
        let Some(counts_by_ts) = counts_by_fp.get(&fingerprint) else {
            continue;
        };
        let Some(sum_squares_by_ts) = sum_squares_by_fp.get(&fingerprint) else {
            continue;
        };
        let samples = series
            .samples
            .into_iter()
            .filter_map(|sample| {
                let ts_ms = sample.timestamp_ms;
                let value = sample.value;
                let SampleValue::Float(sum) = value else {
                    return None;
                };
                let count = *counts_by_ts.get(&ts_ms)?;
                let sum_squares = *sum_squares_by_ts.get(&ts_ms)?;
                if count == 0.0 {
                    return None;
                }
                let mean = sum / count;
                let variance = ((sum_squares / count) - (mean * mean)).max(0.0);
                let value = match kind {
                    MomentReduction::Stddev => variance.sqrt(),
                    MomentReduction::Stdvar => variance,
                };
                Some(RangeSeriesSample::new(ts_ms, SampleValue::Float(value)))
            })
            .collect::<Vec<_>>();
        if !samples.is_empty() {
            out_series.push(RangeSeries {
                drop_name: false,
                start_timestamps_ms: std::collections::BTreeMap::new(),
                labels: series.labels,
                samples,
            });
        }
    }

    Ok(QueryResult::RangeMatrix(out_series))
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct RankedSampleKey {
    fingerprint: SeriesFingerprint,
    sample_index: usize,
}

#[derive(Ord, PartialOrd, Eq, PartialEq)]
struct RankStepGroup {
    timestamp_ms: i64,
    group: String,
}

pub(crate) fn reduce_rank_range_query_results(
    result: QueryResult,
    k: usize,
    kind: RankReduction,
    modifier: Option<&LabelModifier>,
) -> Result<QueryResult, PromqlError> {
    let QueryResult::RangeMatrix(mut series) = result else {
        return Err(PromqlError::Plan(
            "rank query-frontend merge requires range matrix results".into(),
        ));
    };
    if k == 0 {
        return Ok(QueryResult::RangeMatrix(Vec::new()));
    }

    let mut keep = BTreeSet::<RankedSampleKey>::new();
    let mut candidates_by_step_and_group = BTreeMap::<RankStepGroup, Vec<RankCandidate>>::new();
    for series_index in 0..series.len() {
        let series = &series[series_index];
        let group = label_sort_key(&aggregate_labels(&series.labels, modifier));
        let labels_key = label_sort_key(&series.labels);
        let fingerprint = series.labels.fingerprint();
        for (sample_index, sample) in series.samples.iter().enumerate() {
            if let SampleValue::Float(value) = &sample.value {
                candidates_by_step_and_group
                    .entry(RankStepGroup {
                        timestamp_ms: sample.timestamp_ms,
                        group: group.clone(),
                    })
                    .or_default()
                    .push(RankCandidate {
                        fingerprint,
                        labels_key: labels_key.clone(),
                        sample_index,
                        series_index,
                        value: *value,
                    });
            }
        }
    }

    for mut candidates in candidates_by_step_and_group.into_values() {
        candidates.sort_by(|left, right| compare_rank_candidates(kind, left, right));
        candidates.truncate(k.min(candidates.len()));
        keep.extend(candidates.into_iter().map(|candidate| RankedSampleKey {
            fingerprint: candidate.fingerprint,
            sample_index: candidate.sample_index,
        }));
    }

    for series in &mut series {
        let fingerprint = series.labels.fingerprint();
        let mut sample_index = 0_usize;
        series.samples.retain(|_| {
            let keep_sample = keep.contains(&RankedSampleKey {
                fingerprint,
                sample_index,
            });
            sample_index += 1;
            keep_sample
        });
    }
    series.retain(|series| !series.samples.is_empty());
    series.sort_by_key(|series| label_sort_key(&series.labels));
    Ok(QueryResult::RangeMatrix(series))
}
