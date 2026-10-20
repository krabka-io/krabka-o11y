use std::{hint::black_box, time::Instant};

use assert2::assert;
use krabka_logql::parse_metric_query;

use super::*;
use crate::{
    Labels, MetricQuery, MetricSamples, MetricWindow, VectorAggregationState, VectorGrouping,
    merge_metric_samples,
    querier::aggregate::{
        record_matching::{MatchedEntry, record_matching_metric_sample},
        sample_windows::{group_range_samples, range_sample_value},
    },
    rate_metric_value,
};

// Keep the pre-change accumulator as an oracle and a profiling baseline.
#[derive(Clone, Debug, Default)]
struct LegacyMetricSampleState {
    /// The count, sum, sum of squares and extremes of the window's samples.
    summary: VectorAggregationState,
    first: Option<TimestampedMetricValue>,
    last: Option<TimestampedMetricValue>,
    values: Vec<MetricValue>,
    values_by_time: BTreeMap<i64, MetricValue>,
}

impl LegacyMetricSampleState {
    fn has_samples(&self) -> bool {
        self.summary.count > 0
    }

    fn record(&mut self, timestamp_ns: i64, value: MetricValue) {
        self.summary.record(value);
        let sample = TimestampedMetricValue {
            timestamp_ns,
            value,
        };
        self.first = Some(self.first.map_or(sample, |first| {
            if timestamp_ns < first.timestamp_ns {
                sample
            } else {
                first
            }
        }));
        self.last = Some(self.last.map_or(sample, |last| {
            if timestamp_ns > last.timestamp_ns {
                sample
            } else {
                last
            }
        }));
        self.values.push(value);
        self.values_by_time
            .entry(timestamp_ns)
            .and_modify(|current| *current = (*current).add(value))
            .or_insert(value);
    }

    fn merge(&mut self, other: Self) {
        self.summary.count = self.summary.count.saturating_add(other.summary.count);
        self.summary.sum = self.summary.sum.add(other.summary.sum);
        self.summary.sum_squares = self.summary.sum_squares.add(other.summary.sum_squares);
        if let Some(min) = other.summary.min {
            self.summary.min = Some(self.summary.min.map_or(min, |current| {
                if min.cmp_value(current) == Ordering::Less {
                    min
                } else {
                    current
                }
            }));
        }
        if let Some(max) = other.summary.max {
            self.summary.max = Some(self.summary.max.map_or(max, |current| {
                if max.cmp_value(current) == Ordering::Greater {
                    max
                } else {
                    current
                }
            }));
        }
        if let Some(first) = other.first {
            self.first = Some(self.first.map_or(first, |current| {
                if first.timestamp_ns < current.timestamp_ns {
                    first
                } else {
                    current
                }
            }));
        }
        if let Some(last) = other.last {
            self.last = Some(self.last.map_or(last, |current| {
                if last.timestamp_ns > current.timestamp_ns {
                    last
                } else {
                    current
                }
            }));
        }
        self.values.extend(other.values);
        for entry in other.values_by_time {
            let timestamp_ns = entry.0;
            let value = entry.1;
            self.values_by_time
                .entry(timestamp_ns)
                .and_modify(|current| *current = (*current).add(value))
                .or_insert(value);
        }
    }

    fn average(&self) -> MetricValue {
        self.summary.sum.divide_by(self.summary.count)
    }

    fn stdvar(&self) -> MetricValue {
        self.summary.stdvar()
    }

    fn stddev(&self) -> MetricValue {
        self.stdvar().sqrt()
    }

    fn quantile(&mut self, quantile: Quantile) -> MetricValue {
        if self.values.is_empty() {
            return MetricValue::zero();
        }
        self.values.sort_by(|left, right| left.cmp_value(*right));
        if self.values.len() == 1 {
            return self.values[0];
        }

        let scaled_rank =
            u128::from(quantile.numerator.0) * u128::try_from(self.values.len() - 1).unwrap();
        let denominator = u128::from(quantile.denominator.0);
        let lower_index = usize::try_from(scaled_rank / denominator).unwrap();
        let rank_remainder = scaled_rank % denominator;
        if rank_remainder == 0 {
            return self.values[lower_index];
        }

        let upper_index = lower_index + 1;
        let fraction = MetricValue::new(
            i128::try_from(rank_remainder).expect("quantile rank remainder fits in i128"),
            denominator,
        );
        self.values[lower_index].add(
            self.values[upper_index]
                .saturating_sub(self.values[lower_index])
                .multiply(fraction),
        )
    }

    fn counter_increase(&self) -> MetricValue {
        let mut values = self.values_by_time.values().copied();
        let Some(mut previous) = values.next() else {
            return MetricValue::zero();
        };
        let mut increase = MetricValue::zero();
        for value in values {
            increase = if value.cmp_value(previous) == Ordering::Less {
                increase.add(value)
            } else {
                increase.add(value.saturating_sub(previous))
            };
            previous = value;
        }
        increase
    }
}

impl LegacyMetricSampleState {
    fn value(&mut self, aggregation: RangeAggregation) -> MetricValue {
        match aggregation {
            RangeAggregation::CountOverTime
            | RangeAggregation::BytesOverTime
            | RangeAggregation::AbsentOverTime
            | RangeAggregation::SumOverTime
            | RangeAggregation::Rate
            | RangeAggregation::BytesRate => self.summary.sum,
            RangeAggregation::PresentOverTime => MetricValue::integer(1),
            RangeAggregation::RateCounter => self.counter_increase(),
            RangeAggregation::AvgOverTime => self.average(),
            RangeAggregation::StdvarOverTime => self.stdvar(),
            RangeAggregation::StddevOverTime => self.stddev(),
            RangeAggregation::QuantileOverTime(quantile) => self.quantile(quantile),
            RangeAggregation::MinOverTime => self.summary.min.unwrap_or_else(MetricValue::zero),
            RangeAggregation::MaxOverTime => self.summary.max.unwrap_or_else(MetricValue::zero),
            RangeAggregation::FirstOverTime => self
                .first
                .map_or_else(MetricValue::zero, |sample| sample.value),
            RangeAggregation::LastOverTime => self
                .last
                .map_or_else(MetricValue::zero, |sample| sample.value),
        }
    }
}

fn aggregations() -> Vec<RangeAggregation> {
    vec![
        RangeAggregation::CountOverTime,
        RangeAggregation::BytesOverTime,
        RangeAggregation::AbsentOverTime,
        RangeAggregation::SumOverTime,
        RangeAggregation::Rate,
        RangeAggregation::BytesRate,
        RangeAggregation::PresentOverTime,
        RangeAggregation::RateCounter,
        RangeAggregation::AvgOverTime,
        RangeAggregation::StdvarOverTime,
        RangeAggregation::StddevOverTime,
        RangeAggregation::MinOverTime,
        RangeAggregation::MaxOverTime,
        RangeAggregation::FirstOverTime,
        RangeAggregation::LastOverTime,
        RangeAggregation::QuantileOverTime(Quantile {
            numerator: 0.into(),
            denominator: 1.into(),
        }),
        RangeAggregation::QuantileOverTime(Quantile {
            numerator: 1.into(),
            denominator: 2.into(),
        }),
        RangeAggregation::QuantileOverTime(Quantile {
            numerator: 1.into(),
            denominator: 1.into(),
        }),
        RangeAggregation::QuantileOverTime(Quantile {
            numerator: 1.into(),
            denominator: 3.into(),
        }),
    ]
}

#[test]
fn every_accumulator_matches_legacy_for_empty_recorded_and_merged_samples() {
    let rows = [
        (30, 5, 2),
        (10, 7, 3),
        (30, 1, 3),
        (20, 2, 1),
        (10, 1, 2),
        (40, 1, 4),
        (50, -5, 2),
        (60, 0, 1),
    ];
    for aggregation in aggregations() {
        let mut state = MetricSampleState::new(aggregation);
        let mut reference = LegacyMetricSampleState::default();
        assert!(!state.has_samples());
        assert!(state.value(aggregation) == reference.value(aggregation));
        for (time, numerator, denominator) in rows {
            let value = MetricValue::new(numerator, denominator);
            state.record(time, value);
            reference.record(time, value);
            assert!(state.has_samples() == reference.has_samples());
            assert!(
                state.value(aggregation) == reference.value(aggregation),
                "{aggregation:?}"
            );
        }
        for split in 0..=rows.len() {
            let mut left = MetricSampleState::new(aggregation);
            let mut right = MetricSampleState::new(aggregation);
            let mut expected_left = LegacyMetricSampleState::default();
            let mut expected_right = LegacyMetricSampleState::default();
            for (index, (time, numerator, denominator)) in rows.iter().copied().enumerate() {
                let value = MetricValue::new(numerator, denominator);
                if index < split {
                    left.record(time, value);
                    expected_left.record(time, value);
                } else {
                    right.record(time, value);
                    expected_right.record(time, value);
                }
            }
            left.merge(right);
            expected_left.merge(expected_right);
            assert!(left.has_samples() == expected_left.has_samples());
            assert!(
                left.value(aggregation) == expected_left.value(aggregation),
                "{aggregation:?}, split {split}"
            );
        }
    }
}

type LegacyMetricSamples = BTreeMap<Labels, BTreeMap<i64, LegacyMetricSampleState>>;

fn block_samples(query: &MetricQuery, block: i128) -> (MetricSamples, LegacyMetricSamples) {
    let mut actual = MetricSamples::new();
    let mut expected = LegacyMetricSamples::new();
    let eval_times = [15_i64, 25, 35, 45];
    for instance in ["a", "b"] {
        let labels = Labels::from([
            ("app".into(), "api".into()),
            ("instance".into(), instance.into()),
        ]);
        // Duplicate times cross both blocks and streams. The values also include resets.
        for (time, numerator, denominator) in [(20, 7, 3), (10, 5, 2), (30, 1, 4), (20, 1, 2)] {
            let value = MetricValue::new(numerator + block, denominator);
            record_matching_metric_sample(
                &mut actual,
                MetricWindow {
                    query,
                    eval_times: &eval_times,
                    range_ns: 30,
                    delete_filters: &[],
                },
                MatchedEntry {
                    timestamp_ns: time,
                    metric_labels: labels.clone(),
                    line: "value=1".into(),
                    unwrap_sample: Some(value),
                },
            );
            let contribution = match query.aggregation {
                RangeAggregation::CountOverTime
                | RangeAggregation::AbsentOverTime
                | RangeAggregation::PresentOverTime => MetricValue::integer(1),
                RangeAggregation::BytesOverTime | RangeAggregation::BytesRate => {
                    MetricValue::integer(7)
                }
                _ => value,
            };
            for eval_time in eval_times {
                if time > eval_time - 30 && time <= eval_time {
                    expected
                        .entry(labels.clone())
                        .or_default()
                        .entry(eval_time)
                        .or_default()
                        .record(time, contribution);
                }
            }
        }
    }
    (actual, expected)
}

#[test]
fn grouped_cross_block_windows_match_legacy_for_every_aggregation() {
    for aggregation in aggregations() {
        let mut query =
            parse_metric_query(r#"sum_over_time({app="api"} | unwrap value [30ns])"#).unwrap();
        query.aggregation = aggregation;
        let mut actual = MetricSamples::new();
        let mut reference: BTreeMap<Labels, BTreeMap<i64, LegacyMetricSampleState>> =
            BTreeMap::new();
        for block in 0..3 {
            let (partial, expected_partial) = block_samples(&query, block);
            merge_metric_samples(&mut actual, partial);
            for (labels, windows) in expected_partial {
                for (time, state) in windows {
                    reference
                        .entry(labels.clone())
                        .or_default()
                        .entry(time)
                        .or_default()
                        .merge(state);
                }
            }
        }
        for grouping in [
            None,
            Some(VectorGrouping::By(vec!["app".into()])),
            Some(VectorGrouping::Without(vec!["instance".into()])),
        ] {
            let grouped = grouping.is_some();
            let selected = grouping.as_ref().map_or_else(
                || actual.clone(),
                |grouping| group_range_samples(actual.clone(), grouping),
            );
            let mut expected: BTreeMap<Labels, BTreeMap<i64, LegacyMetricSampleState>> =
                BTreeMap::new();
            for (labels, windows) in reference.clone() {
                let labels = if grouped {
                    Labels::from([("app".into(), "api".into())])
                } else {
                    labels
                };
                for (time, state) in windows {
                    expected
                        .entry(labels.clone())
                        .or_default()
                        .entry(time)
                        .or_default()
                        .merge(state);
                }
            }
            assert!(selected.len() == expected.len());
            for (labels, windows) in selected {
                assert!(windows.len() == expected[&labels].len());
                for (time, mut state) in windows {
                    let mut value = expected
                        .get_mut(&labels)
                        .unwrap()
                        .get_mut(&time)
                        .unwrap()
                        .value(aggregation);
                    if matches!(
                        aggregation,
                        RangeAggregation::Rate
                            | RangeAggregation::BytesRate
                            | RangeAggregation::RateCounter
                    ) {
                        value = rate_metric_value(value, query.range_ns.0);
                    }
                    assert!(
                        range_sample_value(&mut state, &query) == value,
                        "{aggregation:?}, grouped={grouped}, time={time}"
                    );
                }
            }
        }
    }
}

#[test]
fn counter_merge_sums_duplicate_timestamps_before_reset_detection() {
    let mut state = MetricSampleState::new(RangeAggregation::RateCounter);
    state.record(10, MetricValue::new(5, 2));
    state.record(20, MetricValue::new(1, 2));
    let mut other = MetricSampleState::new(RangeAggregation::RateCounter);
    other.record(10, MetricValue::new(1, 2));
    other.record(20, MetricValue::new(3, 2));
    other.record(30, MetricValue::new(7, 2));
    state.merge(other);
    assert!(state.value(RangeAggregation::RateCounter) == MetricValue::new(7, 2));
}

// Run one case per process. External allocation profilers and /usr/bin/time
// can measure the same workload with either accumulator.
#[test]
#[ignore = "profiling workload; set METRIC_PROFILE_ROWS, METRIC_PROFILE_WINDOWS and METRIC_PROFILE_STATE"]
fn profile_overlapping_windows() {
    let rows: i64 = std::env::var("METRIC_PROFILE_ROWS")
        .unwrap()
        .parse()
        .unwrap();
    let windows: usize = std::env::var("METRIC_PROFILE_WINDOWS")
        .unwrap()
        .parse()
        .unwrap();
    let mode = std::env::var("METRIC_PROFILE_STATE").unwrap();
    let aggregation = match std::env::var("METRIC_PROFILE_OP").as_deref() {
        Ok("quantile") => RangeAggregation::QuantileOverTime(Quantile {
            numerator: 1.into(),
            denominator: 2.into(),
        }),
        Ok("counter") => RangeAggregation::RateCounter,
        _ => RangeAggregation::CountOverTime,
    };
    let start = Instant::now();
    let checksum = if mode == "legacy" {
        let mut states = vec![LegacyMetricSampleState::default(); windows];
        for row in 0..rows {
            for state in &mut states {
                state.record(row, MetricValue::integer(1));
            }
        }
        black_box(
            states
                .iter_mut()
                .map(|state| state.value(aggregation).numerator)
                .sum::<i128>(),
        )
    } else {
        let mut states = vec![MetricSampleState::new(aggregation); windows];
        for row in 0..rows {
            for state in &mut states {
                state.record(row, MetricValue::integer(1));
            }
        }
        black_box(
            states
                .iter_mut()
                .map(|state| state.value(aggregation).numerator)
                .sum::<i128>(),
        )
    };
    let elapsed_us = start.elapsed().as_micros();
    let expected = match aggregation {
        RangeAggregation::CountOverTime => i128::from(rows) * i128::try_from(windows).unwrap(),
        RangeAggregation::QuantileOverTime(_) => i128::try_from(windows).unwrap(),
        _ => 0,
    };
    assert!(checksum == expected);
    eprintln!(
        "state={mode} operation={aggregation:?} rows={rows} windows={windows} elapsed_us={elapsed_us} checksum={checksum}"
    );
    eprintln!(
        "state_bytes={} legacy_state_bytes={}",
        size_of::<MetricSampleState>(),
        size_of::<LegacyMetricSampleState>()
    );
}
