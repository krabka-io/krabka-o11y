use super::*;

impl MetricValue {
    pub(crate) fn zero() -> Self {
        Self {
            numerator: 0,
            denominator: 1,
        }
    }

    pub(crate) fn integer(value: u64) -> Self {
        Self::new(i128::from(value), 1)
    }

    pub(crate) fn new(numerator: i128, denominator: u128) -> Self {
        if numerator == 0 || denominator == 0 {
            return Self::zero();
        }

        let divisor = gcd_signed(numerator, denominator);
        Self {
            numerator: numerator / i128::try_from(divisor).expect("gcd fits in i128"),
            denominator: denominator / divisor,
        }
    }

    pub(crate) fn add(self, other: Self) -> Self {
        Self::new(
            self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")
                + other.numerator
                    * i128::try_from(self.denominator).expect("denominator fits in i128"),
            self.denominator * other.denominator,
        )
    }

    pub(crate) fn subtract(self, other: Self) -> Self {
        Self::new(
            self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")
                - other.numerator
                    * i128::try_from(self.denominator).expect("denominator fits in i128"),
            self.denominator * other.denominator,
        )
    }

    pub(crate) fn multiply(self, other: Self) -> Self {
        Self::new(
            self.numerator * other.numerator,
            self.denominator * other.denominator,
        )
    }

    pub(crate) fn divide(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }

        let mut numerator = self
            .numerator
            .checked_mul(i128::try_from(other.denominator).ok()?)?;
        let mut denominator = i128::try_from(self.denominator)
            .ok()?
            .checked_mul(other.numerator)?;
        if denominator < 0 {
            numerator = numerator.checked_neg()?;
            denominator = denominator.checked_neg()?;
        }
        Some(Self::new(numerator, u128::try_from(denominator).ok()?))
    }

    pub(crate) fn modulo(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }
        Self::from_f64(self.to_f64()? % other.to_f64()?)
    }

    pub(crate) fn power(self, other: Self) -> Option<Self> {
        Self::from_f64(self.to_f64()?.powf(other.to_f64()?))
    }

    pub(crate) fn saturating_sub(self, other: Self) -> Self {
        if self.cmp_value(other) == Ordering::Less {
            Self::zero()
        } else {
            Self::new(
                self.numerator
                    * i128::try_from(other.denominator).expect("denominator fits in i128")
                    - other.numerator
                        * i128::try_from(self.denominator).expect("denominator fits in i128"),
                self.denominator * other.denominator,
            )
        }
    }

    pub(crate) fn divide_by(self, divisor: u64) -> Self {
        if divisor == 0 {
            Self::zero()
        } else {
            Self::new(self.numerator, self.denominator * u128::from(divisor))
        }
    }

    pub(crate) fn sqrt(self) -> Self {
        let value = self.to_f64().unwrap_or_default().sqrt();
        if !value.is_finite() || value <= 0.0 {
            return Self::zero();
        }

        let scaled = (value * METRIC_DECIMAL_SCALE.to_f64().unwrap_or_default()).floor();
        Self::new(
            i128::from_f64(scaled).unwrap_or_default(),
            METRIC_DECIMAL_SCALE,
        )
    }

    pub(crate) fn cmp_value(self, other: Self) -> Ordering {
        (self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")).cmp(
            &(other.numerator
                * i128::try_from(self.denominator).expect("denominator fits in i128")),
        )
    }

    pub(crate) fn to_f64(self) -> Option<f64> {
        let value = self.numerator.to_f64()? / self.denominator.to_f64()?;
        value.is_finite().then_some(value)
    }

    pub(crate) fn from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }

        let scaled = (value * METRIC_DECIMAL_SCALE.to_f64()?).round();
        Some(Self::new(i128::from_f64(scaled)?, METRIC_DECIMAL_SCALE))
    }
}

impl Default for MetricValue {
    fn default() -> Self {
        Self::zero()
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct MetricSampleState {
    pub(crate) count: u64,
    pub(crate) sum: MetricValue,
    pub(crate) sum_squares: MetricValue,
    pub(crate) min: Option<MetricValue>,
    pub(crate) max: Option<MetricValue>,
    pub(crate) first: Option<(i64, MetricValue)>,
    pub(crate) last: Option<(i64, MetricValue)>,
    pub(crate) values: Vec<MetricValue>,
    pub(crate) values_by_time: BTreeMap<i64, MetricValue>,
}

impl MetricSampleState {
    pub(crate) fn has_samples(&self) -> bool {
        self.count > 0
    }

    pub(crate) fn record(&mut self, timestamp_ns: i64, value: MetricValue) {
        self.count += 1;
        self.sum = self.sum.add(value);
        self.sum_squares = self.sum_squares.add(value.multiply(value));
        self.min = Some(self.min.map_or(value, |min| {
            if value.cmp_value(min) == Ordering::Less {
                value
            } else {
                min
            }
        }));
        self.max = Some(self.max.map_or(value, |max| {
            if value.cmp_value(max) == Ordering::Greater {
                value
            } else {
                max
            }
        }));
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
        self.count = self.count.saturating_add(other.count);
        self.sum = self.sum.add(other.sum);
        self.sum_squares = self.sum_squares.add(other.sum_squares);
        if let Some(min) = other.min {
            self.min = Some(self.min.map_or(min, |current| {
                if min.cmp_value(current) == Ordering::Less {
                    min
                } else {
                    current
                }
            }));
        }
        if let Some(max) = other.max {
            self.max = Some(self.max.map_or(max, |current| {
                if max.cmp_value(current) == Ordering::Greater {
                    max
                } else {
                    current
                }
            }));
        }
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
        self.sum.divide_by(self.count)
    }

    pub(crate) fn stdvar(self) -> MetricValue {
        if self.count == 0 {
            return MetricValue::zero();
        }

        let mean = self.sum.divide_by(self.count);
        self.sum_squares
            .divide_by(self.count)
            .saturating_sub(mean.multiply(mean))
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

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct VectorAggregationState {
    pub(crate) count: u64,
    pub(crate) sum: MetricValue,
    pub(crate) sum_squares: MetricValue,
    pub(crate) min: Option<MetricValue>,
    pub(crate) max: Option<MetricValue>,
}

impl VectorAggregationState {
    pub(crate) fn record(&mut self, value: MetricValue) {
        self.count += 1;
        self.sum = self.sum.add(value);
        self.sum_squares = self.sum_squares.add(value.multiply(value));
        self.min = Some(self.min.map_or(value, |min| {
            if value.cmp_value(min) == Ordering::Less {
                value
            } else {
                min
            }
        }));
        self.max = Some(self.max.map_or(value, |max| {
            if value.cmp_value(max) == Ordering::Greater {
                value
            } else {
                max
            }
        }));
    }

    pub(crate) fn finish(self, op: &VectorAggregationOp) -> MetricValue {
        match op {
            VectorAggregationOp::Sum => self.sum,
            VectorAggregationOp::Count => MetricValue::integer(self.count),
            VectorAggregationOp::Min => self.min.unwrap_or_else(MetricValue::zero),
            VectorAggregationOp::Max => self.max.unwrap_or_else(MetricValue::zero),
            VectorAggregationOp::Avg => self.sum.divide_by(self.count),
            VectorAggregationOp::Stddev => self.stdvar().sqrt(),
            VectorAggregationOp::Stdvar => self.stdvar(),
            VectorAggregationOp::TopK(_)
            | VectorAggregationOp::BottomK(_)
            | VectorAggregationOp::ApproxTopK(_)
            | VectorAggregationOp::CountValues(_)
            | VectorAggregationOp::Sort
            | VectorAggregationOp::SortDesc => {
                unreachable!("selection aggregations are handled before reduction")
            }
        }
    }

    pub(crate) fn stdvar(self) -> MetricValue {
        if self.count == 0 {
            return MetricValue::zero();
        }

        let mean = self.sum.divide_by(self.count);
        self.sum_squares
            .divide_by(self.count)
            .saturating_sub(mean.multiply(mean))
    }
}

pub(crate) fn format_metric_value(value: MetricValue) -> String {
    let negative = value.numerator < 0;
    let numerator = value.numerator.unsigned_abs();
    let whole = numerator / value.denominator;
    let mut remainder = numerator % value.denominator;
    let sign = if negative { "-" } else { "" };
    if remainder == 0 {
        return format!("{sign}{whole}");
    }

    let mut decimals = String::new();
    while remainder != 0 && decimals.len() < 9 {
        remainder *= 10;
        let digit =
            u8::try_from(remainder / value.denominator).expect("decimal digit is less than 10");
        decimals.push(char::from(b'0' + digit));
        remainder %= value.denominator;
    }
    while decimals.ends_with('0') {
        decimals.pop();
    }
    format!("{sign}{whole}.{decimals}")
}

pub(crate) fn rate_metric_value(value: MetricValue, range_ns: i64) -> MetricValue {
    let denominator = u128::from(range_ns.unsigned_abs());
    if denominator == 0 {
        return MetricValue::zero();
    }

    MetricValue::new(
        value.numerator * 1_000_000_000,
        value.denominator * denominator,
    )
}

pub(crate) fn eval_times(range: TimeRange, step_ns: i64) -> Vec<i64> {
    let mut times = Vec::new();
    let mut time = range.start_ns;
    while time <= range.end_ns {
        times.push(time);
        let Some(next) = time.checked_add(step_ns) else {
            break;
        };
        if next <= time {
            break;
        }
        time = next;
    }
    times
}

pub(crate) fn append_matching_log_row(
    streams: &mut BTreeMap<Labels, Vec<[String; 2]>>,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    row: QueryRow<'_>,
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<(), QueryError> {
    let QueryRow {
        fingerprint,
        timestamp_ns,
        line,
        structured_metadata,
    } = row;
    if timestamp_ns < plan.time_range.start_ns
        || timestamp_ns > plan.time_range.end_ns
        || !plan.fingerprints.contains(&fingerprint)
    {
        return Ok(());
    }

    let labels = label_index.labels_for(&plan.tenant, fingerprint).ok_or(
        QueryError::MissingSeriesLabels {
            tenant: plan.tenant.clone(),
            fingerprint,
        },
    )?;
    if is_deleted_log_entry(
        delete_filters,
        labels,
        line,
        structured_metadata,
        timestamp_ns,
    ) {
        return Ok(());
    }
    if let Some((stream_labels, current_line)) =
        matching_loki_stream_entry(&plan.query, labels, line, structured_metadata, timestamp_ns)
    {
        streams
            .entry(stream_labels)
            .or_default()
            .push([timestamp_ns.to_string(), current_line]);
    }

    Ok(())
}
