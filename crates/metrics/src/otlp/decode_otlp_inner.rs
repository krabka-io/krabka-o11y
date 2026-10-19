use opentelemetry_proto::tonic::{common::v1::KeyValue, metrics::v1::ResourceMetrics};

use super::{
    DecodedMetadata, DecodedSample, DecodedSeries, DeltaAccumulator, MetricsData, OtlpError,
    TranslationStrategy, labels, metric, metric_attributes, metric_series,
    metric_series::metric_series_with_data, promoted_resource_attributes,
    resource_metrics_timestamp_ms,
};

pub(crate) struct PartialOtlpDecode {
    pub(crate) series: Vec<DecodedSeries>,
    pub(crate) rejected_data_points: u64,
    pub(crate) error_message: Option<String>,
}

/// How a decode translates names and handles delta temporality.
pub(crate) struct OtlpDecodeOptions<'a> {
    pub(crate) strategy: TranslationStrategy,
    /// Delta state carried across requests; `None` refuses delta points.
    pub(crate) accumulator: Option<&'a mut DeltaAccumulator>,
    /// Resource attributes promoted to labels beyond the default set.
    pub(crate) additional_resource_attributes: &'a [String],
}

pub(crate) fn decode_otlp_inner(
    metrics: &MetricsData,
    options: OtlpDecodeOptions<'_>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let OtlpDecodeOptions {
        strategy,
        mut accumulator,
        additional_resource_attributes,
    } = options;
    let mut out = Vec::new();
    for resource_metrics in &metrics.resource_metrics {
        let resource_attributes = resource_attributes(resource_metrics);
        out.extend(target_info_series(resource_metrics, strategy));

        let promoted_resource_attributes =
            promoted_resource_attributes(resource_attributes, additional_resource_attributes);
        for scope_metrics in &resource_metrics.scope_metrics {
            let metric_attributes =
                metric_attributes(&promoted_resource_attributes, scope_metrics, strategy);
            for metric in &scope_metrics.metrics {
                out.extend(metric_series(
                    metric,
                    &metric_attributes,
                    strategy,
                    accumulator.as_deref_mut(),
                )?);
            }
        }
    }
    Ok(out)
}

pub(crate) fn decode_otlp_inner_partial(
    metrics: &MetricsData,
    options: OtlpDecodeOptions<'_>,
) -> Result<PartialOtlpDecode, OtlpError> {
    let OtlpDecodeOptions {
        strategy,
        mut accumulator,
        additional_resource_attributes,
    } = options;
    let mut series = Vec::new();
    let mut accepted_data_points = 0_u64;
    let mut rejected_data_points = 0_u64;
    let mut first_error = None;

    for resource_metrics in &metrics.resource_metrics {
        let resource_attributes = resource_attributes(resource_metrics);
        series.extend(target_info_series(resource_metrics, strategy));

        let promoted =
            promoted_resource_attributes(resource_attributes, additional_resource_attributes);
        for scope_metrics in &resource_metrics.scope_metrics {
            let attributes = metric_attributes(&promoted, scope_metrics, strategy);
            for metric in &scope_metrics.metrics {
                for data in metric.data.iter().flat_map(one_point_data) {
                    match metric_series_with_data(
                        metric,
                        &data,
                        &attributes,
                        strategy,
                        accumulator.as_deref_mut(),
                    ) {
                        Ok(decoded) => {
                            accepted_data_points += 1;
                            series.extend(decoded);
                        }
                        Err(error) => {
                            rejected_data_points += 1;
                            first_error.get_or_insert(error);
                        }
                    }
                }
            }
        }
    }

    if accepted_data_points == 0
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(PartialOtlpDecode {
        series,
        rejected_data_points,
        error_message: first_error.as_ref().map(ToString::to_string),
    })
}

/// Copy only the current point, never its siblings or shared metric metadata.
/// Streaming keeps temporary point storage bounded even for large HTTP batches.
fn one_point_data(data: &metric::Data) -> Box<dyn Iterator<Item = metric::Data> + '_> {
    match data {
        metric::Data::Gauge(points) => Box::new(points.data_points.iter().map(move |point| {
            metric::Data::Gauge(super::Gauge {
                data_points: vec![point.clone()],
            })
        })),
        metric::Data::Sum(points) => Box::new(points.data_points.iter().map(move |point| {
            metric::Data::Sum(super::Sum {
                data_points: vec![point.clone()],
                aggregation_temporality: points.aggregation_temporality,
                is_monotonic: points.is_monotonic,
            })
        })),
        metric::Data::Histogram(points) => Box::new(points.data_points.iter().map(move |point| {
            metric::Data::Histogram(super::Histogram {
                data_points: vec![point.clone()],
                aggregation_temporality: points.aggregation_temporality,
            })
        })),
        metric::Data::ExponentialHistogram(points) => {
            Box::new(points.data_points.iter().map(move |point| {
                metric::Data::ExponentialHistogram(super::ExponentialHistogram {
                    data_points: vec![point.clone()],
                    aggregation_temporality: points.aggregation_temporality,
                })
            }))
        }
        metric::Data::Summary(points) => Box::new(points.data_points.iter().map(move |point| {
            metric::Data::Summary(super::Summary {
                data_points: vec![point.clone()],
            })
        })),
    }
}

fn resource_attributes(resource_metrics: &ResourceMetrics) -> &[KeyValue] {
    resource_metrics
        .resource
        .as_ref()
        .map_or(&[][..], |resource| resource.attributes.as_slice())
}

/// The `target_info` series a resource with attributes contributes, stamped
/// with the resource's latest data point.
fn target_info_series(
    resource_metrics: &ResourceMetrics,
    strategy: TranslationStrategy,
) -> Option<DecodedSeries> {
    let resource_attributes = resource_attributes(resource_metrics);
    if resource_attributes.is_empty() {
        return None;
    }
    let timestamp_ms = resource_metrics_timestamp_ms(resource_metrics)?;
    Some(DecodedSeries {
        labels: labels("target_info", resource_attributes, &[], None, strategy),
        samples: vec![DecodedSample::new(timestamp_ms, 1.0)],
        histograms: Vec::new(),
        exemplars: Vec::new(),
        metadata: Some(DecodedMetadata {
            metric_family_name: "target_info".into(),
            metric_type: "gauge".into(),
            help: "Target metadata.".into(),
            unit: String::new(),
        }),
    })
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use opentelemetry_proto::tonic::metrics::v1::{
        AggregationTemporality, ExponentialHistogram, ExponentialHistogramDataPoint, Gauge,
        Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
        Summary, SummaryDataPoint, number_data_point,
    };

    use super::*;

    fn metric_data(data: Option<metric::Data>) -> MetricsData {
        MetricsData {
            resource_metrics: vec![ResourceMetrics {
                scope_metrics: vec![ScopeMetrics {
                    metrics: vec![Metric {
                        name: "test.metric".into(),
                        description: "Shared metadata".into(),
                        unit: "s".into(),
                        data,
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn kinds(temporality: AggregationTemporality) -> Vec<metric::Data> {
        let number = NumberDataPoint {
            time_unix_nano: 2_000_000,
            start_time_unix_nano: 1_000_000,
            value: Some(number_data_point::Value::AsDouble(2.0)),
            ..Default::default()
        };
        vec![
            metric::Data::Gauge(Gauge {
                data_points: vec![number.clone()],
            }),
            metric::Data::Sum(Sum {
                data_points: vec![number.clone()],
                aggregation_temporality: temporality as i32,
                is_monotonic: true,
            }),
            metric::Data::Sum(Sum {
                data_points: vec![number],
                aggregation_temporality: temporality as i32,
                is_monotonic: false,
            }),
            metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 2_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 2,
                    sum: Some(2.0),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 1],
                    ..Default::default()
                }],
                aggregation_temporality: temporality as i32,
            }),
            metric::Data::ExponentialHistogram(ExponentialHistogram {
                data_points: vec![ExponentialHistogramDataPoint {
                    time_unix_nano: 2_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 2,
                    sum: Some(2.0),
                    zero_count: 2,
                    ..Default::default()
                }],
                aggregation_temporality: temporality as i32,
            }),
            metric::Data::Summary(Summary {
                data_points: vec![SummaryDataPoint {
                    time_unix_nano: 2_000_000,
                    start_time_unix_nano: 1_000_000,
                    count: 2,
                    sum: 2.0,
                    ..Default::default()
                }],
            }),
        ]
    }

    // Valid points surround an invalid timestamp, so rejecting a point must
    // neither discard its siblings nor modify the accumulator for that point.
    fn batch(data: &metric::Data, mixed: bool) -> metric::Data {
        let mut data = data.clone();
        macro_rules! points {
            ($container:expr) => {{
                let points = &mut $container.data_points;
                let mut next = points[0].clone();
                next.time_unix_nano = 3_000_000;
                if mixed {
                    let mut invalid = next.clone();
                    invalid.time_unix_nano = u64::MAX;
                    points.push(invalid);
                }
                points.push(next);
            }};
        }
        match &mut data {
            metric::Data::Gauge(p) => points!(p),
            metric::Data::Sum(p) => points!(p),
            metric::Data::Histogram(p) => points!(p),
            metric::Data::ExponentialHistogram(p) => points!(p),
            metric::Data::Summary(p) => points!(p),
        }
        data
    }

    fn options(accumulator: &mut DeltaAccumulator) -> OtlpDecodeOptions<'_> {
        OtlpDecodeOptions {
            strategy: TranslationStrategy::default(),
            accumulator: Some(accumulator),
            additional_resource_attributes: &[],
        }
    }

    #[test]
    fn partial_decode_matches_strict_translation_for_every_kind_and_temporality() {
        for temporality in [
            AggregationTemporality::Unspecified,
            AggregationTemporality::Cumulative,
            AggregationTemporality::Delta,
        ] {
            for kind in kinds(temporality) {
                for mixed in [false, true] {
                    let valid = metric_data(Some(batch(&kind, false)));
                    let input = metric_data(Some(batch(&kind, mixed)));
                    let mut strict_state = DeltaAccumulator::default();
                    let mut partial_state = DeltaAccumulator::default();
                    // Reuse state across requests as HTTP does, including after
                    // a rejected delta point between two accepted ones.
                    for _ in 0..2 {
                        let expected =
                            decode_otlp_inner(&valid, options(&mut strict_state)).unwrap();
                        let decoded =
                            decode_otlp_inner_partial(&input, options(&mut partial_state)).unwrap();
                        check!(
                            decoded.series == expected,
                            "{temporality:?}, mixed={mixed}, {kind:?}"
                        );
                        check!(decoded.rejected_data_points == u64::from(mixed));
                        check!(decoded.error_message.is_some() == mixed);
                    }
                }
            }
        }
    }

    #[test]
    fn partial_decode_preserves_temporality_errors() {
        for mut kind in kinds(AggregationTemporality::Delta) {
            let temporality = match &mut kind {
                metric::Data::Sum(p) => &mut p.aggregation_temporality,
                metric::Data::Histogram(p) => &mut p.aggregation_temporality,
                metric::Data::ExponentialHistogram(p) => &mut p.aggregation_temporality,
                _ => continue,
            };
            *temporality = 99;
            let data = metric_data(Some(batch(&kind, false)));
            let mut state = DeltaAccumulator::default();
            let expected = decode_otlp_inner(&data, options(&mut state)).unwrap_err();
            let actual = decode_otlp_inner_partial(&data, options(&mut state))
                .err()
                .unwrap();
            check!(actual.to_string() == expected.to_string());
        }
        for kind in kinds(AggregationTemporality::Delta) {
            let data = metric_data(Some(batch(&kind, false)));
            let without_state = || OtlpDecodeOptions {
                strategy: TranslationStrategy::default(),
                accumulator: None,
                additional_resource_attributes: &[],
            };
            let expected = decode_otlp_inner(&data, without_state());
            let actual = decode_otlp_inner_partial(&data, without_state());
            match expected {
                Ok(series) => {
                    check!(actual.unwrap().series == series);
                }
                Err(error) => {
                    check!(actual.err().unwrap().to_string() == error.to_string());
                }
            }
        }
    }

    #[test]
    fn partial_decode_preserves_empty_and_all_rejected_results() {
        let mut state = DeltaAccumulator::default();
        for data in [None, Some(metric::Data::Gauge(Gauge::default()))] {
            let decoded =
                decode_otlp_inner_partial(&metric_data(data), options(&mut state)).unwrap();
            check!(decoded.series.is_empty());
            check!(decoded.rejected_data_points == 0);
            check!(decoded.error_message.is_none());
        }
        let invalid = metric_data(Some(metric::Data::Gauge(Gauge {
            data_points: vec![NumberDataPoint::default(); 2],
        })));
        let error = decode_otlp_inner_partial(&invalid, options(&mut state))
            .err()
            .unwrap();
        assert!(error.to_string().contains("missing number datapoint value"));
    }
}
