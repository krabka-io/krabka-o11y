use super::{BTreeMap, MetricValue, Ordering, Quantile, VectorAggregationState};

#[derive(Clone, Debug, Default)]
pub(crate) struct MetricSampleState {
    /// The count, sum, sum of squares and extremes of the window's samples.
    pub(crate) summary: VectorAggregationState,
    pub(crate) first: Option<(i64, MetricValue)>,
    pub(crate) last: Option<(i64, MetricValue)>,
    pub(crate) values: Vec<MetricValue>,
    pub(crate) values_by_time: BTreeMap<i64, MetricValue>,
}

impl MetricSampleState {
    pub(crate) fn has_samples(&self) -> bool {
        self.summary.count > 0
    }

    pub(crate) fn record(&mut self, timestamp_ns: i64, value: MetricValue) {
        self.summary.record(value);
        self.first = Some(self.first.map_or((timestamp_ns, value), |first| {
            if timestamp_ns < first.0 {
                (timestamp_ns, value)
            } else {
                first
            }
        }));
        self.last = Some(self.last.map_or((timestamp_ns, value), |last| {
            if timestamp_ns > last.0 {
                (timestamp_ns, value)
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
            self.first =
                Some(self.first.map_or(
                    first,
                    |current| {
                        if first.0 < current.0 { first } else { current }
                    },
                ));
        }
        if let Some(last) = other.last {
            self.last =
                Some(self.last.map_or(
                    last,
                    |current| {
                        if last.0 > current.0 { last } else { current }
                    },
                ));
        }
        self.values.extend(other.values);
        for (timestamp_ns, value) in other.values_by_time {
            self.values_by_time
                .entry(timestamp_ns)
                .and_modify(|current| *current = (*current).add(value))
                .or_insert(value);
        }
    }

    pub(crate) fn average(self) -> MetricValue {
        self.summary.sum.divide_by(self.summary.count)
    }

    pub(crate) fn stdvar(self) -> MetricValue {
        self.summary.stdvar()
    }

    pub(crate) fn stddev(self) -> MetricValue {
        self.stdvar().sqrt()
    }

    pub(crate) fn quantile(mut self, quantile: Quantile) -> MetricValue {
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

    pub(crate) fn counter_increase(self) -> MetricValue {
        let mut values = self.values_by_time.into_values();
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
