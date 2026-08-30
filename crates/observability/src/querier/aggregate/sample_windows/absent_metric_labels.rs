use super::*;

pub(crate) fn absent_metric_labels(query: &MetricQuery) -> Labels {
    query
        .stream
        .matchers
        .iter()
        .filter(|matcher| matcher.op == MatchOp::Equal)
        .map(|matcher| (matcher.name.clone(), matcher.value.clone()))
        .collect::<Labels>()
}
