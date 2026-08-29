type MetricSamples = BTreeMap<Labels, BTreeMap<i64, MetricSampleState>>;
type FormattedMetricSeries = Vec<(Labels, Vec<[String; 2]>)>;

#[derive(Clone, Copy)]
struct MetricWindow<'a> {
    query: &'a MetricQuery,
    eval_times: &'a [i64],
    range_ns: i64,
    delete_filters: &'a [ActiveLogDeleteFilter],
}

fn merge_metric_samples(samples: &mut MetricSamples, block_samples: MetricSamples) {
    for (labels, values) in block_samples {
        let target = samples.entry(labels).or_default();
        for (timestamp_ns, value) in values {
            let sample = target.entry(timestamp_ns).or_default();
            sample.merge(value);
        }
    }
}

fn apply_absent_over_time(samples: &mut MetricSamples, query: &MetricQuery, eval_times: &[i64]) {
    if !matches!(query.aggregation, RangeAggregation::AbsentOverTime) {
        return;
    }

    let mut absent_values = BTreeMap::new();
    for eval_time_ns in eval_times {
        let has_sample = samples.values().any(|values| {
            values
                .get(eval_time_ns)
                .is_some_and(MetricSampleState::has_samples)
        });
        if !has_sample {
            let mut sample = MetricSampleState::default();
            sample.record(*eval_time_ns, MetricValue::integer(1));
            absent_values.insert(*eval_time_ns, sample);
        }
    }

    samples.clear();
    if !absent_values.is_empty() {
        samples.insert(absent_metric_labels(query), absent_values);
    }
}

fn absent_metric_labels(query: &MetricQuery) -> Labels {
    query
        .stream
        .matchers
        .iter()
        .filter(|matcher| matcher.op == MatchOp::Equal)
        .map(|matcher| (matcher.name.clone(), matcher.value.clone()))
        .collect::<Labels>()
}

fn metric_samples_from_batches(
    batches: &[datafusion::arrow::record_batch::RecordBatch],
    plan: &StreamPlan,
    query: &MetricQuery,
    label_index: &LabelIndex,
    eval_times: &[i64],
    delete_filters: &[ActiveLogDeleteFilter],
) -> Result<MetricSamples, QueryError> {
    let mut samples: MetricSamples = BTreeMap::new();

    for batch in batches {
        let fingerprints = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "series_fingerprint",
                expected: "UInt64",
            })?;
        let timestamps = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or(QueryError::InvalidColumn {
                column: "timestamp_ns",
                expected: "Int64",
            })?;
        let lines = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or(QueryError::InvalidColumn {
                column: "line",
                expected: "Utf8",
            })?;
        let metadata = batch.column(3).as_any().downcast_ref::<MapArray>().ok_or(
            QueryError::InvalidColumn {
                column: "structured_metadata",
                expected: "Map<Utf8, Utf8>",
            },
        )?;

        for row in 0..batch.num_rows() {
            let structured_metadata = structured_metadata_value(metadata, row)?;
            append_matching_metric_row(
                &mut samples,
                plan,
                label_index,
                QueryRow {
                    fingerprint: fingerprints.value(row),
                    timestamp_ns: timestamps.value(row),
                    line: lines.value(row),
                    structured_metadata: &structured_metadata,
                },
                MetricWindow {
                    query,
                    eval_times,
                    range_ns: query.range_ns.0,
                    delete_filters,
                },
            )?;
        }
    }

    Ok(samples)
}

fn format_metric_samples(samples: MetricSamples, query: &MetricQuery) -> FormattedMetricSeries {
    let samples = if let Some(grouping) = &query.range_grouping {
        group_range_samples(samples, grouping)
    } else {
        samples
    };

    if let Some(vector_aggregation) = &query.vector_aggregation {
        let mut series = aggregate_vector_samples(samples, query, vector_aggregation)
            .into_iter()
            .map(|(labels, values)| {
                (
                    labels,
                    values
                        .into_iter()
                        .map(|(time, value)| [time.to_string(), format_metric_value(value)])
                        .collect(),
                )
            })
            .collect::<FormattedMetricSeries>();
        sort_formatted_vector_samples(&mut series, &vector_aggregation.op);
        return series;
    }

    samples
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, value)| {
                        [
                            time.to_string(),
                            format_metric_value(range_sample_value(value, query)),
                        ]
                    })
                    .collect(),
            )
        })
        .collect()
}

fn sort_formatted_vector_samples(series: &mut FormattedMetricSeries, op: &VectorAggregationOp) {
    match op {
        VectorAggregationOp::Sort | VectorAggregationOp::SortDesc => {
            series.sort_by(|left, right| {
                let left_value = left
                    .1
                    .first()
                    .and_then(|sample| parse_metric_sample_value(&sample[1]))
                    .unwrap_or_default();
                let right_value = right
                    .1
                    .first()
                    .and_then(|sample| parse_metric_sample_value(&sample[1]))
                    .unwrap_or_default();
                let value_order = match op {
                    VectorAggregationOp::Sort => left_value.cmp_value(right_value),
                    VectorAggregationOp::SortDesc => right_value.cmp_value(left_value),
                    _ => Ordering::Equal,
                };
                value_order.then_with(|| left.0.cmp(&right.0))
            });
        }
        _ => {}
    }
}

fn group_range_samples(samples: MetricSamples, grouping: &VectorGrouping) -> MetricSamples {
    let mut grouped: MetricSamples = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, Some(grouping));
        let grouped_values = grouped.entry(grouped_labels).or_default();
        for (time, value) in values {
            grouped_values.entry(time).or_default().merge(value);
        }
    }

    grouped
}

fn aggregate_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    vector_aggregation: &VectorAggregation,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    match &vector_aggregation.op {
        VectorAggregationOp::TopK(limit) | VectorAggregationOp::ApproxTopK(limit) => {
            return select_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                *limit,
                VectorSelection::Largest,
            );
        }
        VectorAggregationOp::BottomK(limit) => {
            return select_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                *limit,
                VectorSelection::Smallest,
            );
        }
        VectorAggregationOp::CountValues(label) => {
            return count_values_vector_samples(
                samples,
                query,
                vector_aggregation.grouping.as_ref(),
                label,
            );
        }
        VectorAggregationOp::Sort | VectorAggregationOp::SortDesc => {
            return select_all_vector_samples(samples, query);
        }
        _ => {}
    }

    let mut states: BTreeMap<Labels, BTreeMap<i64, VectorAggregationState>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, vector_aggregation.grouping.as_ref());
        for (time, value) in values {
            states
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .record(range_sample_value(value, query));
        }
    }

    states
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, state)| (time, state.finish(&vector_aggregation.op)))
                    .collect(),
            )
        })
        .collect()
}

fn count_values_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    value_label: &str,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut counted: BTreeMap<Labels, BTreeMap<i64, u64>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, grouping);
        for (time, value) in values {
            let value = range_sample_value(value, query);
            let mut output_labels = grouped_labels.clone();
            output_labels.insert(value_label.to_string(), format_metric_value(value));
            *counted
                .entry(output_labels)
                .or_default()
                .entry(time)
                .or_default() += 1;
        }
    }

    counted
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, count)| (time, MetricValue::integer(count)))
                    .collect(),
            )
        })
        .collect()
}

fn select_all_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    samples
        .into_iter()
        .map(|(labels, values)| {
            (
                labels,
                values
                    .into_iter()
                    .map(|(time, value)| (time, range_sample_value(value, query)))
                    .collect(),
            )
        })
        .collect()
}

#[derive(Clone, Copy)]
enum VectorSelection {
    Largest,
    Smallest,
}

fn select_vector_samples(
    samples: MetricSamples,
    query: &MetricQuery,
    grouping: Option<&VectorGrouping>,
    limit: u64,
    selection: VectorSelection,
) -> BTreeMap<Labels, BTreeMap<i64, MetricValue>> {
    let mut groups: BTreeMap<Labels, BTreeMap<i64, Vec<(Labels, MetricValue)>>> = BTreeMap::new();

    for (labels, values) in samples {
        let grouped_labels = vector_group_labels(&labels, grouping);
        for (time, value) in values {
            groups
                .entry(grouped_labels.clone())
                .or_default()
                .entry(time)
                .or_default()
                .push((labels.clone(), range_sample_value(value, query)));
        }
    }

    let mut selected = BTreeMap::new();
    for (_grouped_labels, values) in groups {
        for (time, mut candidates) in values {
            candidates.sort_by(|left, right| {
                let value_order = match selection {
                    VectorSelection::Largest => right.1.cmp_value(left.1),
                    VectorSelection::Smallest => left.1.cmp_value(right.1),
                };
                value_order.then_with(|| left.0.cmp(&right.0))
            });
            let limit = usize::try_from(limit).unwrap_or(usize::MAX);
            for (labels, value) in candidates.into_iter().take(limit) {
                selected
                    .entry(labels)
                    .or_insert_with(BTreeMap::new)
                    .insert(time, value);
            }
        }
    }

    selected
}

fn vector_group_labels(labels: &Labels, grouping: Option<&VectorGrouping>) -> Labels {
    match grouping {
        Some(VectorGrouping::By(names)) => names
            .iter()
            .filter_map(|name| labels.get(name).map(|value| (name.clone(), value.clone())))
            .collect(),
        Some(VectorGrouping::Without(names)) => labels
            .iter()
            .filter(|(name, _)| !names.contains(name))
            .map(|(name, value)| (name.clone(), value.clone()))
            .collect(),
        None => Labels::new(),
    }
}

fn range_sample_value(value: MetricSampleState, query: &MetricQuery) -> MetricValue {
    match query.aggregation {
        RangeAggregation::CountOverTime
        | RangeAggregation::BytesOverTime
        | RangeAggregation::AbsentOverTime
        | RangeAggregation::SumOverTime => value.sum,
        RangeAggregation::PresentOverTime => MetricValue::integer(1),
        RangeAggregation::Rate | RangeAggregation::BytesRate => {
            rate_metric_value(value.sum, query.range_ns.0)
        }
        RangeAggregation::RateCounter => {
            rate_metric_value(value.counter_increase(), query.range_ns.0)
        }
        RangeAggregation::AvgOverTime => value.average(),
        RangeAggregation::StdvarOverTime => value.stdvar(),
        RangeAggregation::StddevOverTime => value.stddev(),
        RangeAggregation::QuantileOverTime(quantile) => value.quantile(quantile),
        RangeAggregation::MinOverTime => value.min.unwrap_or_else(MetricValue::zero),
        RangeAggregation::MaxOverTime => value.max.unwrap_or_else(MetricValue::zero),
        RangeAggregation::FirstOverTime => value
            .first
            .map_or_else(MetricValue::zero, |(_, value)| value),
        RangeAggregation::LastOverTime => value
            .last
            .map_or_else(MetricValue::zero, |(_, value)| value),
    }
}

fn is_unwrapped_metric_query(query: &MetricQuery) -> bool {
    query
        .stream
        .pipeline
        .iter()
        .any(|stage| matches!(stage, PipelineStage::Unwrap(_)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MetricValue {
    numerator: i128,
    denominator: u128,
}

const METRIC_DECIMAL_SCALE: u128 = 1_000_000_000;

impl MetricValue {
    fn zero() -> Self {
        Self {
            numerator: 0,
            denominator: 1,
        }
    }

    fn integer(value: u64) -> Self {
        Self::new(i128::from(value), 1)
    }

    fn new(numerator: i128, denominator: u128) -> Self {
        if numerator == 0 || denominator == 0 {
            return Self::zero();
        }

        let divisor = gcd_signed(numerator, denominator);
        Self {
            numerator: numerator / i128::try_from(divisor).expect("gcd fits in i128"),
            denominator: denominator / divisor,
        }
    }

    fn add(self, other: Self) -> Self {
        Self::new(
            self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")
                + other.numerator
                    * i128::try_from(self.denominator).expect("denominator fits in i128"),
            self.denominator * other.denominator,
        )
    }

    fn subtract(self, other: Self) -> Self {
        Self::new(
            self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")
                - other.numerator
                    * i128::try_from(self.denominator).expect("denominator fits in i128"),
            self.denominator * other.denominator,
        )
    }

    fn multiply(self, other: Self) -> Self {
        Self::new(
            self.numerator * other.numerator,
            self.denominator * other.denominator,
        )
    }

    fn divide(self, other: Self) -> Option<Self> {
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

    fn modulo(self, other: Self) -> Option<Self> {
        if other.numerator == 0 {
            return None;
        }
        Self::from_f64(self.to_f64()? % other.to_f64()?)
    }

    fn power(self, other: Self) -> Option<Self> {
        Self::from_f64(self.to_f64()?.powf(other.to_f64()?))
    }

    fn saturating_sub(self, other: Self) -> Self {
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

    fn divide_by(self, divisor: u64) -> Self {
        if divisor == 0 {
            Self::zero()
        } else {
            Self::new(self.numerator, self.denominator * u128::from(divisor))
        }
    }

    fn sqrt(self) -> Self {
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

    fn cmp_value(self, other: Self) -> Ordering {
        (self.numerator * i128::try_from(other.denominator).expect("denominator fits in i128")).cmp(
            &(other.numerator
                * i128::try_from(self.denominator).expect("denominator fits in i128")),
        )
    }

    fn to_f64(self) -> Option<f64> {
        let value = self.numerator.to_f64()? / self.denominator.to_f64()?;
        value.is_finite().then_some(value)
    }

    fn from_f64(value: f64) -> Option<Self> {
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
struct MetricSampleState {
    count: u64,
    sum: MetricValue,
    sum_squares: MetricValue,
    min: Option<MetricValue>,
    max: Option<MetricValue>,
    first: Option<(i64, MetricValue)>,
    last: Option<(i64, MetricValue)>,
    values: Vec<MetricValue>,
    values_by_time: BTreeMap<i64, MetricValue>,
}

impl MetricSampleState {
    fn has_samples(&self) -> bool {
        self.count > 0
    }

    fn record(&mut self, timestamp_ns: i64, value: MetricValue) {
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

    fn merge(&mut self, other: Self) {
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

    fn average(self) -> MetricValue {
        self.sum.divide_by(self.count)
    }

    fn stdvar(self) -> MetricValue {
        if self.count == 0 {
            return MetricValue::zero();
        }

        let mean = self.sum.divide_by(self.count);
        self.sum_squares
            .divide_by(self.count)
            .saturating_sub(mean.multiply(mean))
    }

    fn stddev(self) -> MetricValue {
        self.stdvar().sqrt()
    }

    fn quantile(mut self, quantile: Quantile) -> MetricValue {
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

    fn counter_increase(self) -> MetricValue {
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
struct VectorAggregationState {
    count: u64,
    sum: MetricValue,
    sum_squares: MetricValue,
    min: Option<MetricValue>,
    max: Option<MetricValue>,
}

impl VectorAggregationState {
    fn record(&mut self, value: MetricValue) {
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

    fn finish(self, op: &VectorAggregationOp) -> MetricValue {
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

    fn stdvar(self) -> MetricValue {
        if self.count == 0 {
            return MetricValue::zero();
        }

        let mean = self.sum.divide_by(self.count);
        self.sum_squares
            .divide_by(self.count)
            .saturating_sub(mean.multiply(mean))
    }
}

fn format_metric_value(value: MetricValue) -> String {
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

fn rate_metric_value(value: MetricValue, range_ns: i64) -> MetricValue {
    let denominator = u128::from(range_ns.unsigned_abs());
    if denominator == 0 {
        return MetricValue::zero();
    }

    MetricValue::new(
        value.numerator * 1_000_000_000,
        value.denominator * denominator,
    )
}

fn eval_times(range: TimeRange, step_ns: i64) -> Vec<i64> {
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

fn append_matching_log_row(
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

fn append_matching_hot_log_record(
    streams: &mut BTreeMap<Labels, Vec<[String; 2]>>,
    plan: &StreamPlan,
    record: &WalLogRecord,
    frontier: &CompactionFrontier,
    delete_filters: &[ActiveLogDeleteFilter],
) {
    if record.tenant != plan.tenant
        || frontier.is_compacted(record)
        || record.timestamp_ns < plan.time_range.start_ns
        || record.timestamp_ns > plan.time_range.end_ns
    {
        return;
    }

    if is_deleted_log_entry(
        delete_filters,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        return;
    }

    if let Some((stream_labels, current_line)) = matching_loki_stream_entry(
        &plan.query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        streams
            .entry(stream_labels)
            .or_default()
            .push([record.timestamp_ns.to_string(), current_line]);
    }
}

fn is_deleted_log_entry(
    delete_filters: &[ActiveLogDeleteFilter],
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> bool {
    delete_filters.iter().any(|filter| {
        timestamp_ns >= filter.time_range.start_ns
            && timestamp_ns <= filter.time_range.end_ns
            && filter
                .query
                .matches_with_fields(labels, line, structured_metadata)
    })
}

fn matching_loki_stream_entry(
    query: &StreamQuery,
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> Option<(Labels, String)> {
    let evaluation =
        query.evaluate_with_fields_at(labels, line, structured_metadata, timestamp_ns)?;
    let mut stream_labels = evaluation.fields;
    stream_labels.remove(UNWRAP_SAMPLE_VALUE_LABEL);
    if should_insert_unknown_detected_level_for_stream_query(query, &stream_labels) {
        stream_labels.insert("detected_level".to_string(), "unknown".to_string());
    }
    Some((stream_labels, evaluation.line))
}

fn matching_loki_metric_sample(
    query: &MetricQuery,
    labels: &Labels,
    line: &str,
    structured_metadata: &Labels,
    timestamp_ns: i64,
) -> Result<Option<(Labels, String, Option<MetricValue>)>, QueryError> {
    let evaluation =
        query
            .stream
            .evaluate_with_fields_at(labels, line, structured_metadata, timestamp_ns);
    let Some(evaluation) = evaluation else {
        return Ok(None);
    };
    if let Some(error) = evaluation
        .fields
        .get("__error__")
        .filter(|error| !error.is_empty())
    {
        return Err(QueryError::MetricPipelineError {
            error: error.clone(),
            details: evaluation.fields.get("__error_details__").cloned(),
        });
    }
    let mut metric_labels = evaluation.fields;
    let unwrap_sample = metric_labels
        .remove(UNWRAP_SAMPLE_VALUE_LABEL)
        .and_then(|value| parse_metric_sample_value(&value));
    for stage in &query.stream.pipeline {
        if let PipelineStage::Unwrap(unwrap) = stage {
            metric_labels.remove(unwrap.label());
        }
    }
    if should_insert_unknown_detected_level_for_stream_query(&query.stream, &metric_labels) {
        metric_labels.insert("detected_level".to_string(), "unknown".to_string());
    }
    Ok(Some((metric_labels, evaluation.line, unwrap_sample)))
}

fn parse_metric_sample_value(value: &str) -> Option<MetricValue> {
    let (numerator, denominator) = parse_decimal_sample_literal(value)?;
    Some(MetricValue::new(numerator, denominator))
}

fn parse_decimal_sample_literal(value: &str) -> Option<(i128, u128)> {
    if value.is_empty() {
        return None;
    }
    let (negative, value) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    if value.is_empty() {
        return None;
    }

    let (mantissa, exponent) = match value.find(['e', 'E']) {
        Some(index) => {
            let exponent_text = &value[index + 1..];
            if exponent_text.find(['e', 'E']).is_some() {
                return None;
            }
            (
                &value[..index],
                parse_decimal_sample_exponent(exponent_text)?,
            )
        }
        None => (value, 0),
    };
    if mantissa.is_empty() {
        return None;
    }

    let (whole, fractional) = match mantissa.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None => (mantissa, ""),
    };
    if whole.is_empty() && fractional.is_empty() {
        return None;
    }
    if !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fractional.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let mut digits = String::with_capacity(whole.len() + fractional.len());
    digits.push_str(whole);
    digits.push_str(fractional);
    if digits.is_empty() {
        return None;
    }
    let mut numerator = digits.parse::<u128>().ok()?;

    let decimal_places = i64::try_from(fractional.len())
        .ok()?
        .checked_sub(i64::from(exponent))?;
    let denominator = if decimal_places >= 0 {
        10_u128.checked_pow(u32::try_from(decimal_places).ok()?)?
    } else {
        numerator =
            numerator.checked_mul(10_u128.checked_pow(u32::try_from(-decimal_places).ok()?)?)?;
        1
    };
    let denominator = i128::try_from(denominator).ok()?;
    let numerator = i128::try_from(numerator).ok()?;
    Some((
        if negative { -numerator } else { numerator },
        u128::try_from(denominator).ok()?,
    ))
}

fn parse_decimal_sample_exponent(value: &str) -> Option<i32> {
    if value.is_empty() {
        return None;
    }
    let value = value.strip_prefix('+').unwrap_or(value);
    if value.is_empty() {
        return None;
    }
    value.parse::<i32>().ok()
}

fn should_insert_unknown_detected_level(labels: &Labels) -> bool {
    !labels.contains_key("detected_level")
        && !labels.contains_key("level")
        && !labels.contains_key("severity")
        && !labels.contains_key("severity_text")
}

fn should_insert_unknown_detected_level_for_stream_query(
    query: &StreamQuery,
    labels: &Labels,
) -> bool {
    should_insert_unknown_detected_level(labels)
        && !query
            .pipeline
            .iter()
            .any(|stage| matches!(stage, PipelineStage::KeepLabels(_)))
}

fn sort_loki_stream_values(streams: &mut BTreeMap<Labels, Vec<[String; 2]>>) {
    for values in streams.values_mut() {
        values.sort_by_key(|[timestamp, _]| timestamp.parse::<i64>().unwrap_or(i64::MAX));
    }
}

fn structured_metadata_value(metadata: &MapArray, row: usize) -> Result<Labels, QueryError> {
    let entries = metadata.value(row);
    let keys = entries
        .column_by_name("key")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(QueryError::InvalidColumn {
            column: "structured_metadata.key",
            expected: "Utf8",
        })?;
    let values = entries
        .column_by_name("value")
        .and_then(|array| array.as_any().downcast_ref::<StringArray>())
        .ok_or(QueryError::InvalidColumn {
            column: "structured_metadata.value",
            expected: "Utf8",
        })?;

    Ok((0..entries.len())
        .map(|index| {
            (
                keys.value(index).to_string(),
                values.value(index).to_string(),
            )
        })
        .collect())
}

fn append_matching_metric_row(
    samples: &mut MetricSamples,
    plan: &StreamPlan,
    label_index: &LabelIndex,
    row: QueryRow<'_>,
    window: MetricWindow<'_>,
) -> Result<(), QueryError> {
    let MetricWindow {
        query,
        eval_times,
        range_ns,
        delete_filters,
    } = window;
    if !plan.fingerprints.contains(&row.fingerprint) {
        return Ok(());
    }

    let labels = label_index
        .labels_for(&plan.tenant, row.fingerprint)
        .ok_or(QueryError::MissingSeriesLabels {
            tenant: plan.tenant.clone(),
            fingerprint: row.fingerprint,
        })?;
    if is_deleted_log_entry(
        delete_filters,
        labels,
        row.line,
        row.structured_metadata,
        row.timestamp_ns,
    ) {
        return Ok(());
    }
    if let Some((metric_labels, current_line, unwrap_sample)) = matching_loki_metric_sample(
        query,
        labels,
        row.line,
        row.structured_metadata,
        row.timestamp_ns,
    )? {
        let samples = samples.entry(metric_labels).or_default();
        let is_unwrapped = is_unwrapped_metric_query(query);
        let value = match query.aggregation {
            RangeAggregation::Rate if is_unwrapped => unwrap_sample.unwrap_or_default(),
            RangeAggregation::CountOverTime
            | RangeAggregation::Rate
            | RangeAggregation::AbsentOverTime
            | RangeAggregation::PresentOverTime => MetricValue::integer(1),
            RangeAggregation::BytesRate | RangeAggregation::BytesOverTime => {
                MetricValue::integer(current_line.len() as u64)
            }
            RangeAggregation::RateCounter
            | RangeAggregation::SumOverTime
            | RangeAggregation::AvgOverTime
            | RangeAggregation::StdvarOverTime
            | RangeAggregation::StddevOverTime
            | RangeAggregation::QuantileOverTime(_)
            | RangeAggregation::MinOverTime
            | RangeAggregation::MaxOverTime
            | RangeAggregation::FirstOverTime
            | RangeAggregation::LastOverTime => unwrap_sample.unwrap_or_default(),
        };
        for eval_time_ns in eval_times {
            let window_end_ns = eval_time_ns.saturating_sub(query.offset_ns.0);
            if row.timestamp_ns > window_end_ns.saturating_sub(range_ns)
                && row.timestamp_ns <= window_end_ns
            {
                let sample = samples.entry(*eval_time_ns).or_default();
                sample.record(row.timestamp_ns, value);
            }
        }
    }

    Ok(())
}

fn append_matching_hot_metric_record(
    samples: &mut MetricSamples,
    plan: &StreamPlan,
    record: &WalLogRecord,
    frontier: &CompactionFrontier,
    window: MetricWindow<'_>,
) -> Result<(), QueryError> {
    let MetricWindow {
        query,
        eval_times,
        range_ns,
        delete_filters,
    } = window;
    if record.tenant != plan.tenant || frontier.is_compacted(record) {
        return Ok(());
    }

    if is_deleted_log_entry(
        delete_filters,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    ) {
        return Ok(());
    }

    if let Some((metric_labels, current_line, unwrap_sample)) = matching_loki_metric_sample(
        query,
        &record.labels,
        &record.line,
        &record.structured_metadata,
        record.timestamp_ns,
    )? {
        let samples = samples.entry(metric_labels).or_default();
        let is_unwrapped = is_unwrapped_metric_query(query);
        let value = match query.aggregation {
            RangeAggregation::Rate if is_unwrapped => unwrap_sample.unwrap_or_default(),
            RangeAggregation::CountOverTime
            | RangeAggregation::Rate
            | RangeAggregation::AbsentOverTime
            | RangeAggregation::PresentOverTime => MetricValue::integer(1),
            RangeAggregation::BytesRate | RangeAggregation::BytesOverTime => {
                MetricValue::integer(current_line.len() as u64)
            }
            RangeAggregation::RateCounter
            | RangeAggregation::SumOverTime
            | RangeAggregation::AvgOverTime
            | RangeAggregation::StdvarOverTime
            | RangeAggregation::StddevOverTime
            | RangeAggregation::QuantileOverTime(_)
            | RangeAggregation::MinOverTime
            | RangeAggregation::MaxOverTime
            | RangeAggregation::FirstOverTime
            | RangeAggregation::LastOverTime => unwrap_sample.unwrap_or_default(),
        };
        for eval_time_ns in eval_times {
            let window_end_ns = eval_time_ns.saturating_sub(query.offset_ns.0);
            if record.timestamp_ns > window_end_ns.saturating_sub(range_ns)
                && record.timestamp_ns <= window_end_ns
            {
                let sample = samples.entry(*eval_time_ns).or_default();
                sample.record(record.timestamp_ns, value);
            }
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct QueryRow<'a> {
    fingerprint: SeriesFingerprint,
    timestamp_ns: i64,
    line: &'a str,
    structured_metadata: &'a Labels,
}

