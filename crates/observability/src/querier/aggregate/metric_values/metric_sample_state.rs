use super::{BTreeMap, MetricValue, Ordering, Quantile};
use crate::RangeAggregation;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct TimestampedMetricValue {
    pub(crate) timestamp_ns: i64,
    pub(crate) value: MetricValue,
}

/// Stores only the fields that the range aggregation consumes.
#[derive(Clone, Debug)]
pub(crate) enum MetricSampleState {
    Sum {
        sum: MetricValue,
        has_samples: bool,
    },
    Present(bool),
    Average {
        sum: MetricValue,
        count: u64,
    },
    Variance {
        sum: MetricValue,
        sum_squares: MetricValue,
        count: u64,
    },
    Min(Option<MetricValue>),
    Max(Option<MetricValue>),
    First(Option<TimestampedMetricValue>),
    Last(Option<TimestampedMetricValue>),
    Quantile(Vec<MetricValue>),
    Counter(BTreeMap<i64, MetricValue>),
}

impl MetricSampleState {
    pub(crate) fn new(aggregation: RangeAggregation) -> Self {
        match aggregation {
            RangeAggregation::CountOverTime
            | RangeAggregation::BytesOverTime
            | RangeAggregation::AbsentOverTime
            | RangeAggregation::SumOverTime
            | RangeAggregation::Rate
            | RangeAggregation::BytesRate => Self::Sum {
                sum: MetricValue::zero(),
                has_samples: false,
            },
            RangeAggregation::PresentOverTime => Self::Present(false),
            RangeAggregation::AvgOverTime => Self::Average {
                sum: MetricValue::zero(),
                count: 0,
            },
            RangeAggregation::StdvarOverTime | RangeAggregation::StddevOverTime => Self::Variance {
                sum: MetricValue::zero(),
                sum_squares: MetricValue::zero(),
                count: 0,
            },
            RangeAggregation::MinOverTime => Self::Min(None),
            RangeAggregation::MaxOverTime => Self::Max(None),
            RangeAggregation::FirstOverTime => Self::First(None),
            RangeAggregation::LastOverTime => Self::Last(None),
            RangeAggregation::QuantileOverTime(_) => Self::Quantile(Vec::new()),
            RangeAggregation::RateCounter => Self::Counter(BTreeMap::new()),
        }
    }

    pub(crate) fn has_samples(&self) -> bool {
        match self {
            Self::Sum { has_samples, .. } | Self::Present(has_samples) => *has_samples,
            Self::Average { count, .. } | Self::Variance { count, .. } => *count > 0,
            Self::Min(value) | Self::Max(value) => value.is_some(),
            Self::First(value) | Self::Last(value) => value.is_some(),
            Self::Quantile(values) => !values.is_empty(),
            Self::Counter(values) => !values.is_empty(),
        }
    }

    pub(crate) fn record(&mut self, timestamp_ns: i64, value: MetricValue) {
        match self {
            Self::Sum { sum, has_samples } => {
                *sum = sum.add(value);
                *has_samples = true;
            }
            Self::Present(has_samples) => *has_samples = true,
            Self::Average { sum, count } => {
                *sum = sum.add(value);
                *count += 1;
            }
            Self::Variance {
                sum,
                sum_squares,
                count,
            } => {
                *sum = sum.add(value);
                *sum_squares = sum_squares.add(value.multiply(value));
                *count += 1;
            }
            Self::Min(current) => {
                *current = Some(current.map_or(value, |min| value.lesser_of(min)));
            }
            Self::Max(current) => {
                *current = Some(current.map_or(value, |max| value.greater_of(max)));
            }
            Self::First(current) => {
                if current.is_none_or(|sample| timestamp_ns < sample.timestamp_ns) {
                    *current = Some(TimestampedMetricValue {
                        timestamp_ns,
                        value,
                    });
                }
            }
            Self::Last(current) => {
                if current.is_none_or(|sample| timestamp_ns > sample.timestamp_ns) {
                    *current = Some(TimestampedMetricValue {
                        timestamp_ns,
                        value,
                    });
                }
            }
            Self::Quantile(values) => values.push(value),
            Self::Counter(values) => {
                values
                    .entry(timestamp_ns)
                    .and_modify(|current| *current = current.add(value))
                    .or_insert(value);
            }
        }
    }

    pub(crate) fn merge(&mut self, other: Self) {
        match (self, other) {
            (
                Self::Sum { sum, has_samples },
                Self::Sum {
                    sum: other_sum,
                    has_samples: other_has_samples,
                },
            ) => {
                *sum = sum.add(other_sum);
                *has_samples |= other_has_samples;
            }
            (Self::Present(has_samples), Self::Present(other)) => *has_samples |= other,
            (
                Self::Average { sum, count },
                Self::Average {
                    sum: other_sum,
                    count: other_count,
                },
            ) => {
                *sum = sum.add(other_sum);
                *count = count.saturating_add(other_count);
            }
            (
                Self::Variance {
                    sum,
                    sum_squares,
                    count,
                },
                Self::Variance {
                    sum: other_sum,
                    sum_squares: other_squares,
                    count: other_count,
                },
            ) => {
                *sum = sum.add(other_sum);
                *sum_squares = sum_squares.add(other_squares);
                *count = count.saturating_add(other_count);
            }
            (Self::Min(current), Self::Min(other)) => {
                if let Some(other) = other {
                    *current = Some(current.map_or(other, |value| {
                        if other.cmp_value(value) == Ordering::Less {
                            other
                        } else {
                            value
                        }
                    }));
                }
            }
            (Self::Max(current), Self::Max(other)) => {
                if let Some(other) = other {
                    *current = Some(current.map_or(other, |value| {
                        if other.cmp_value(value) == Ordering::Greater {
                            other
                        } else {
                            value
                        }
                    }));
                }
            }
            (Self::First(current), Self::First(other)) => {
                if let Some(other) = other
                    && current.is_none_or(|value| other.timestamp_ns < value.timestamp_ns)
                {
                    *current = Some(other);
                }
            }
            (Self::Last(current), Self::Last(other)) => {
                if let Some(other) = other
                    && current.is_none_or(|value| other.timestamp_ns > value.timestamp_ns)
                {
                    *current = Some(other);
                }
            }
            (Self::Quantile(values), Self::Quantile(other)) => values.extend(other),
            (Self::Counter(values), Self::Counter(other)) => {
                for (timestamp_ns, value) in other {
                    values
                        .entry(timestamp_ns)
                        .and_modify(|current| *current = current.add(value))
                        .or_insert(value);
                }
            }
            _ => unreachable!("partial states belong to the same range aggregation"),
        }
    }

    pub(crate) fn value(&mut self, aggregation: RangeAggregation) -> MetricValue {
        match self {
            Self::Sum { sum, .. } => *sum,
            Self::Present(_) => MetricValue::integer(1),
            Self::Average { sum, count } => sum.divide_by(*count),
            Self::Variance {
                sum,
                sum_squares,
                count,
            } => {
                let mean = sum.divide_by(*count);
                let variance = sum_squares
                    .divide_by(*count)
                    .saturating_sub(mean.multiply(mean));
                if matches!(aggregation, RangeAggregation::StddevOverTime) {
                    variance.sqrt()
                } else {
                    variance
                }
            }
            Self::Min(value) | Self::Max(value) => value.unwrap_or_else(MetricValue::zero),
            Self::First(sample) | Self::Last(sample) => {
                sample.map_or_else(MetricValue::zero, |sample| sample.value)
            }
            Self::Quantile(values) => {
                let RangeAggregation::QuantileOverTime(quantile) = aggregation else {
                    unreachable!("quantile state belongs to a quantile aggregation");
                };
                Self::quantile(values, quantile)
            }
            Self::Counter(values) => Self::counter_increase(values),
        }
    }

    fn quantile(values: &mut [MetricValue], quantile: Quantile) -> MetricValue {
        if values.is_empty() {
            return MetricValue::zero();
        }
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

    fn counter_increase(values_by_time: &BTreeMap<i64, MetricValue>) -> MetricValue {
        let mut values = values_by_time.values().copied();
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

#[cfg(test)]
mod tests;
