use super::{MetricFilter, TraceMetricSeries, metric_filter_passes};

pub(crate) fn apply_metric_filter(
    series: Vec<TraceMetricSeries>,
    filter: Option<MetricFilter>,
) -> Vec<TraceMetricSeries> {
    let Some(filter) = filter else {
        return series;
    };
    series
        .into_iter()
        .filter_map(|mut series| {
            series
                .points
                .retain(|(_, value)| !value.is_nan() && metric_filter_passes(*value, filter));
            series.exemplars.retain(|exemplar| {
                !exemplar.value.is_nan() && metric_filter_passes(exemplar.value, filter)
            });
            if series.points.is_empty() {
                None
            } else {
                Some(series)
            }
        })
        .collect()
}
