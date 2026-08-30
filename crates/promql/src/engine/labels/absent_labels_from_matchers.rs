use super::{LabelMatcher, Labels, MatchOp};

pub(crate) fn absent_labels_from_matchers(matchers: &[LabelMatcher]) -> Labels {
    let mut labels = Labels::new();
    for matcher in matchers {
        if matcher.name != "__name__" && matcher.op == MatchOp::Eq {
            labels.insert(&matcher.name, &matcher.value);
        }
    }
    labels
}
