use opentelemetry_proto::tonic::{common::v1::KeyValue, metrics::v1::ResourceMetrics};

use super::{
    DecodedMetadata, DecodedSample, DecodedSeries, DeltaAccumulator, MetricsData, OtlpError,
    TranslationStrategy, labels, metric, metric_attributes, metric_series,
    promoted_resource_attributes, resource_metrics_timestamp_ms,
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
                for point_metric in one_point_metrics(metric) {
                    match metric_series(
                        &point_metric,
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

fn one_point_metrics(metric: &super::Metric) -> Vec<super::Metric> {
    let Some(data) = &metric.data else {
        return Vec::new();
    };
    match data {
        metric::Data::Gauge(points) => points
            .data_points
            .iter()
            .map(|point| {
                with_data(
                    metric,
                    metric::Data::Gauge(super::Gauge {
                        data_points: vec![point.clone()],
                    }),
                )
            })
            .collect(),
        metric::Data::Sum(points) => points
            .data_points
            .iter()
            .map(|point| {
                let mut one = points.clone();
                one.data_points = vec![point.clone()];
                with_data(metric, metric::Data::Sum(one))
            })
            .collect(),
        metric::Data::Histogram(points) => points
            .data_points
            .iter()
            .map(|point| {
                let mut one = points.clone();
                one.data_points = vec![point.clone()];
                with_data(metric, metric::Data::Histogram(one))
            })
            .collect(),
        metric::Data::ExponentialHistogram(points) => points
            .data_points
            .iter()
            .map(|point| {
                let mut one = points.clone();
                one.data_points = vec![point.clone()];
                with_data(metric, metric::Data::ExponentialHistogram(one))
            })
            .collect(),
        metric::Data::Summary(points) => points
            .data_points
            .iter()
            .map(|point| {
                with_data(
                    metric,
                    metric::Data::Summary(super::Summary {
                        data_points: vec![point.clone()],
                    }),
                )
            })
            .collect(),
    }
}

fn with_data(metric: &super::Metric, data: metric::Data) -> super::Metric {
    let mut one = metric.clone();
    one.data = Some(data);
    one
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
