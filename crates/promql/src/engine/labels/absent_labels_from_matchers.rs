use std::collections::BTreeSet;

use super::{LabelMatcher, Labels, MatchOp};

/// Builds the labelset that `absent` and `absent_over_time` return.
///
/// `createLabelsForAbsentFunction` walks the matchers in source order and keeps
/// the value of the FIRST equality matcher on each label. Any later matcher on
/// that label -- a second equality, or a matcher of any other kind -- deletes
/// the label instead of refining it, so
/// `absent(x{job="a",job="b",foo="bar"})` returns only `{foo="bar"}`. Upstream
/// calls that historic and arguably wrong, and keeps it for compatibility.
pub(crate) fn absent_labels_from_matchers(matchers: &[LabelMatcher]) -> Labels {
    let mut labels = Labels::new();
    let mut kept: BTreeSet<&str> = BTreeSet::new();
    let mut dropped: BTreeSet<&str> = BTreeSet::new();
    for matcher in matchers {
        if matcher.name == "__name__" {
            continue;
        }
        if matcher.op == MatchOp::Eq && kept.insert(matcher.name.as_str()) {
            labels.insert(&matcher.name, &matcher.value);
        } else {
            dropped.insert(matcher.name.as_str());
        }
    }
    labels
        .iter()
        .filter(|(name, _)| !dropped.contains(name.as_str()))
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect()
}
