use super::MetricSeries;

/// Truncate each series' exemplars to `limit`. `None` disables limiting.
pub fn limit_exemplars(series: &mut [MetricSeries], limit: Option<usize>) {
    let Some(limit) = limit else { return };
    for s in series {
        s.exemplars.truncate(limit);
    }
}
