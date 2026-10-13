use super::{Labels, MatchOp, MetricQuery};

pub(crate) fn absent_metric_labels(query: &MetricQuery) -> Labels {
    let mut labels = Labels::new();
    for matcher in &query.stream.matchers {
        if matcher.op == MatchOp::Equal {
            labels.insert(matcher.name.clone(), matcher.value.clone());
        }
    }
    labels
}
