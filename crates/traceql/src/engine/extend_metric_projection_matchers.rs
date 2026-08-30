use super::*;

pub(crate) fn extend_metric_projection_matchers(options: &mut ScanOptions, metric: &MetricPlan) {
    for matcher in metric_nested_projection_matchers(metric) {
        if !options.projection_matchers.contains(&matcher) {
            options.projection_matchers.push(matcher);
        }
    }
}
