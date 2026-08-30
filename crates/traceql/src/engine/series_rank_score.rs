use super::TraceMetricSeries;

pub(crate) fn series_rank_score(series: &TraceMetricSeries) -> f64 {
    series.points.iter().map(|(_, value)| *value).sum()
}
