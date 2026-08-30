use super::{MetricPlan, SpanMatcher, nested_metric_projection_matcher};

pub(crate) fn metric_nested_projection_matchers(metric: &MetricPlan) -> Vec<SpanMatcher> {
    let mut out = Vec::new();
    for field in metric.by.iter().chain(metric.value.iter()) {
        if let Some(matcher) = nested_metric_projection_matcher(field)
            && !out.contains(&matcher)
        {
            out.push(matcher);
        }
    }
    out
}
