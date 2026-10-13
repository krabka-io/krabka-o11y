use super::{BTreeMap, MetricValue, Ordering, Quantile, VectorAggregationState};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TimestampedMetricValue {
    pub(crate) timestamp_ns: i64,
    pub(crate) value: MetricValue,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MetricSampleState {
    /// The count, sum, sum of squares and extremes of the window's samples.
    pub(crate) summary: VectorAggregationState,
    pub(crate) first: Option<TimestampedMetricValue>,
    pub(crate) last: Option<TimestampedMetricValue>,
    pub(crate) values: Vec<MetricValue>,
    pub(crate) values_by_time: BTreeMap<i64, MetricValue>,
}

impl MetricSampleState {
    pub(crate) fn has_samples(&self) -> bool {
        self.summary.count > 0
    }

    pub(crate) fn record(&mut self, timestamp_ns: i64, value: MetricValue) {
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

    pub(crate) fn merge(&mut self, other: Self) {
        self.summary.merge(other.summary);
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

    pub(crate) fn average(&self) -> MetricValue {
        self.summary.sum.divide_by(self.summary.count)
    }

    pub(crate) fn stdvar(&self) -> MetricValue {
        self.summary.stdvar()
    }

    pub(crate) fn stddev(&self) -> MetricValue {
        self.stdvar().sqrt()
    }

    pub(crate) fn quantile(&self, quantile: Quantile) -> MetricValue {
        if self.values.is_empty() {
            return MetricValue::zero();
        }
        let mut values = self.values.clone();
        values.sort_by(|left, right| left.cmp_value(*right));
        if values.len() == 1 {
            return values[0];
        }

        let scaled_rank =
            u128::from(quantile.numerator.0) * u128::try_from(values.len() - 1).unwrap();
        let denominator = u128::from(quantile.denominator.0);
        let lower_index = usize::try_from(scaled_rank / denominator).unwrap();
        let rank_remainder = scaled_rank % denominator;
        if rank_remainder == 0 {
            return values[lower_index];
        }

        let upper_index = lower_index + 1;
        let fraction = MetricValue::new(
            i128::try_from(rank_remainder).expect("quantile rank remainder fits in i128"),
            denominator,
        );
        values[lower_index].add(
            values[upper_index]
                .saturating_sub(values[lower_index])
                .multiply(fraction),
        )
    }

    pub(crate) fn counter_increase(&self) -> MetricValue {
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
