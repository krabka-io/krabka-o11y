use super::{MetricsResponseJson, MetricSeries, merge_metric_series, limit_exemplars};

/// Merge all metric partials' series into one response, then apply exemplar
/// limiting.
#[must_use]
pub fn merge_metrics(
    partials: Vec<MetricsResponseJson>,
    exemplar_limit: Option<usize>,
) -> MetricsResponseJson {
    let mut merged: Vec<MetricSeries> = Vec::new();
    for p in partials {
        for s in p.series {
            merge_metric_series(&mut merged, s);
        }
    }
    limit_exemplars(&mut merged, exemplar_limit);
    MetricsResponseJson { series: merged }
}
