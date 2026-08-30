use super::{
    DecodedSeries, KeyValue, Metric, Summary, TranslationStrategy, metric_metadata,
    summary_point_series, translated_metric_name,
};

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
            &name,
            point,
            resource_attributes,
            Some(metadata.clone()),
        ));
    }
    out
}
