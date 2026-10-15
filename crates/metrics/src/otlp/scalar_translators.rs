use super::{
    AggregationTemporality, DecodedExemplar, DecodedMetadata, DecodedSample, DecodedSeries,
    DeltaAccumulator, ExemplarPolicy, Gauge, KeyValue, Metric, NumberDataPoint, OtlpError, Sum,
    Summary, SummaryDataPoint, ToPrimitive, TranslationStrategy, exemplars_from_number_point,
    labels, metric_metadata, nanos_to_millis, number_value, sum_metadata_type,
    translated_metric_name,
};

/// What every data point of one metric shares when it fans out into series.
pub(crate) struct PointFamily<'a> {
    /// The translated metric name.
    pub(crate) name: &'a str,
    pub(crate) resource_attributes: &'a [KeyValue],
    pub(crate) metadata: Option<&'a DecodedMetadata>,
    pub(crate) strategy: TranslationStrategy,
}

/// What every scalar series an OTLP data point fans out to shares: the point's
/// attributes, its timestamps and its metadata.
///
/// A classic histogram becomes bucket, `_count` and `_sum` series, and a
/// summary quantile, `_count` and `_sum` series; each is one sample under the
/// same labels plus at most one of its own.
pub(crate) struct ScalarPointSeries<'a> {
    pub(crate) resource_attributes: &'a [KeyValue],
    pub(crate) attributes: &'a [KeyValue],
    pub(crate) strategy: TranslationStrategy,
    pub(crate) time_unix_nano: u64,
    pub(crate) start_time_unix_nano: u64,
    pub(crate) metadata: Option<DecodedMetadata>,
}

/// One series of a fanned-out point: its name, the label that sets it apart
/// from its siblings, and its one sample.
pub(crate) struct ScalarSeries<'a> {
    pub(crate) name: &'a str,
    pub(crate) extra_label: Option<ExtraLabel<'a>>,
    pub(crate) sample_value: f64,
    pub(crate) exemplars: Vec<DecodedExemplar>,
}

/// A `le` or `quantile` label.
pub(crate) struct ExtraLabel<'a> {
    pub(crate) label_name: &'a str,
    pub(crate) label_value: &'a str,
}

impl ScalarPointSeries<'_> {
    pub(crate) fn series(&self, series: ScalarSeries<'_>) -> DecodedSeries {
        let ScalarSeries {
            name,
            extra_label,
            sample_value,
            exemplars,
        } = series;
        DecodedSeries {
            labels: labels(
                name,
                self.resource_attributes,
                self.attributes,
                extra_label,
                self.strategy,
            ),
            samples: vec![DecodedSample::with_start_timestamp(
                nanos_to_millis(self.time_unix_nano),
                sample_value,
                (self.start_time_unix_nano != 0)
                    .then_some(nanos_to_millis(self.start_time_unix_nano)),
            )],
            histograms: Vec::new(),
            exemplars,
            metadata: self.metadata.clone(),
        }
    }
}

pub(crate) fn append_count_and_sum(
    out: &mut Vec<DecodedSeries>,
    point: &ScalarPointSeries<'_>,
    name: &str,
    count: f64,
    sum: Option<f64>,
) {
    out.push(point.series(ScalarSeries {
        name: &format!("{name}_count"),
        extra_label: None,
        sample_value: count,
        exemplars: Vec::new(),
    }));
    if let Some(sum) = sum {
        out.push(point.series(ScalarSeries {
            name: &format!("{name}_sum"),
            extra_label: None,
            sample_value: sum,
            exemplars: Vec::new(),
        }));
    }
}

pub(crate) fn scalar_series(
    name: &str,
    point: &NumberDataPoint,
    resource_attributes: &[KeyValue],
    metadata: Option<DecodedMetadata>,
    exemplar_policy: ExemplarPolicy,
    strategy: TranslationStrategy,
) -> Result<DecodedSeries, OtlpError> {
    let value = number_value(point)
        .ok_or_else(|| OtlpError::Invalid(name.into(), "missing number datapoint value".into()))?;
    let exemplars = match exemplar_policy {
        ExemplarPolicy::Keep => exemplars_from_number_point(point, strategy),
        ExemplarPolicy::Drop => Vec::new(),
    };
    Ok(DecodedSeries {
        labels: labels(name, resource_attributes, &point.attributes, None, strategy),
        samples: vec![DecodedSample::with_start_timestamp(
            nanos_to_millis(point.time_unix_nano),
            value,
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars,
        metadata,
    })
}

pub(crate) fn gauge_series(
    metric: &Metric,
    gauge: &Gauge,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    gauge
        .data_points
        .iter()
        .map(|point| {
            let name = translated_metric_name(metric, strategy, false);
            let metadata = metric_metadata(metric, &name, "gauge");
            scalar_series(
                &name,
                point,
                resource_attributes,
                Some(metadata),
                ExemplarPolicy::Drop,
                strategy,
            )
        })
        .collect()
}

pub(crate) fn sum_series(
    metric: &Metric,
    sum: &Sum,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
    accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let name = translated_metric_name(metric, strategy, sum.is_monotonic);

    if sum.aggregation_temporality == AggregationTemporality::Delta as i32 {
        let Some(accumulator) = accumulator else {
            return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
        };
        return sum
            .data_points
            .iter()
            .map(|point| {
                delta_sum_series(
                    &name,
                    point,
                    resource_attributes,
                    accumulator,
                    Some(metric_metadata(metric, &name, sum_metadata_type(sum))),
                    strategy,
                )
            })
            .collect();
    }

    if sum.aggregation_temporality != AggregationTemporality::Cumulative as i32
        && sum.aggregation_temporality != AggregationTemporality::Unspecified as i32
    {
        return Err(OtlpError::DeltaUnsupported(metric.name.clone()));
    }

    sum.data_points
        .iter()
        .map(|point| {
            let exemplar_policy = if sum.is_monotonic {
                ExemplarPolicy::Keep
            } else {
                ExemplarPolicy::Drop
            };
            scalar_series(
                &name,
                point,
                resource_attributes,
                Some(metric_metadata(metric, &name, sum_metadata_type(sum))),
                exemplar_policy,
                strategy,
            )
        })
        .collect()
}

pub(crate) fn delta_sum_series(
    name: &str,
    point: &NumberDataPoint,
    resource_attributes: &[KeyValue],
    accumulator: &mut DeltaAccumulator,
    metadata: Option<DecodedMetadata>,
    strategy: TranslationStrategy,
) -> Result<DecodedSeries, OtlpError> {
    let delta = number_value(point)
        .ok_or_else(|| OtlpError::Invalid(name.into(), "missing number datapoint value".into()))?;
    let labels = labels(name, resource_attributes, &point.attributes, None, strategy);
    let cumulative = accumulator.accumulate_sum(&labels, point.start_time_unix_nano, delta);
    Ok(DecodedSeries {
        labels,
        samples: vec![DecodedSample::with_start_timestamp(
            nanos_to_millis(point.time_unix_nano),
            cumulative,
            (point.start_time_unix_nano != 0)
                .then_some(nanos_to_millis(point.start_time_unix_nano)),
        )],
        histograms: Vec::new(),
        exemplars: exemplars_from_number_point(point, strategy),
        metadata,
    })
}

pub(crate) fn summary_point_series(
    point: &SummaryDataPoint,
    family: &PointFamily<'_>,
) -> Vec<DecodedSeries> {
    let PointFamily {
        name,
        resource_attributes,
        metadata,
        strategy,
    } = *family;
    let scalar = ScalarPointSeries {
        resource_attributes,
        attributes: &point.attributes,
        strategy,
        time_unix_nano: point.time_unix_nano,
        start_time_unix_nano: point.start_time_unix_nano,
        metadata: metadata.cloned(),
    };
    let mut out = Vec::new();
    for quantile in &point.quantile_values {
        let quantile_value = quantile.quantile.to_string();
        out.push(scalar.series(ScalarSeries {
            name,
            extra_label: Some(ExtraLabel {
                label_name: "quantile",
                label_value: &quantile_value,
            }),
            sample_value: quantile.value,
            exemplars: Vec::new(),
        }));
    }
    append_count_and_sum(
        &mut out,
        &scalar,
        name,
        point.count.to_f64().unwrap_or(f64::MAX),
        Some(point.sum),
    );
    out
}

pub(crate) fn summary_series(
    metric: &Metric,
    summary: &Summary,
    resource_attributes: &[KeyValue],
    strategy: TranslationStrategy,
) -> Vec<DecodedSeries> {
    let name = translated_metric_name(metric, strategy, false);
    let metadata = metric_metadata(metric, &name, "summary");
    let mut out = Vec::new();
    for point in &summary.data_points {
        out.extend(summary_point_series(
            point,
            &PointFamily {
                name: &name,
                resource_attributes,
                metadata: Some(&metadata),
                strategy,
            },
        ));
    }
    out
}
