use super::{
    DecodedSeries, ExemplarPolicy, Gauge, KeyValue, Metric, OtlpError, TranslationStrategy,
    metric_metadata, scalar_series, translated_metric_name,
};

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
