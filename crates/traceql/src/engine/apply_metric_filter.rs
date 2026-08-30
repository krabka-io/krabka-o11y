use super::*;

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
                .retain(|(_, value)| metric_filter_passes(*value, filter));
            series.exemplars.retain(|exemplar| {
                series
                    .points
                    .iter()
                    .any(|(ts, _)| *ts == exemplar.timestamp_ns)
            });
            if series.points.is_empty() {
                None
            } else {
                Some(series)
            }
        })
        .collect()
}
