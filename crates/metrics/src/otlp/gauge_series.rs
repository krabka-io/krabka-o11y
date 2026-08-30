use super::{Metric, Gauge, KeyValue, TranslationStrategy, DecodedSeries, OtlpError, translated_metric_name, metric_metadata, scalar_series, ExemplarPolicy};

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
            )
        })
        .collect()
}
