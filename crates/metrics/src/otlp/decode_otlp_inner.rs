use super::{
    DecodedMetadata, DecodedSample, DecodedSeries, DeltaAccumulator, MetricsData, OtlpError,
    TranslationStrategy, labels, metric_attributes, metric_series, resource_metrics_timestamp_ms,
};

pub(crate) fn decode_otlp_inner(
    data: &MetricsData,
    strategy: TranslationStrategy,
    mut accumulator: Option<&mut DeltaAccumulator>,
) -> Result<Vec<DecodedSeries>, OtlpError> {
    let mut out = Vec::new();
    for resource_metrics in &data.resource_metrics {
        let resource_attributes = resource_metrics
            .resource
            .as_ref()
            .map_or(&[][..], |resource| resource.attributes.as_slice());

        if !resource_attributes.is_empty()
            && let Some(timestamp_ms) = resource_metrics_timestamp_ms(resource_metrics)
        {
            out.push(DecodedSeries {
                labels: labels("target_info", resource_attributes, &[], None),
                samples: vec![DecodedSample::new(timestamp_ms, 1.0)],
                histograms: Vec::new(),
                exemplars: Vec::new(),
                metadata: Some(DecodedMetadata {
                    metric_family_name: "target_info".into(),
                    metric_type: "gauge".into(),
                    help: "Target metadata.".into(),
                    unit: String::new(),
                }),
            });
        }

        for scope_metrics in &resource_metrics.scope_metrics {
            let metric_attributes = metric_attributes(resource_attributes, scope_metrics);
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
