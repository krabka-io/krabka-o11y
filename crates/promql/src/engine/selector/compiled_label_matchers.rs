use super::*;

/// A set of [`CompiledLabelMatcher`]s for a hot match loop.
///
/// The loop can match many label sets without a recompile of each `=~`/`!~`
/// regex per call. `labels_match` has that bug when a caller invokes it per
/// sample.
pub(crate) struct CompiledLabelMatchers {
    pub(crate) matchers: Vec<CompiledLabelMatcher>,
}

impl CompiledLabelMatchers {
    /// Returns `true` when `labels` satisfies every compiled matcher. This method
    /// is the precompiled equivalent of `labels_match`.
    pub(crate) fn matches(&self, labels: &Labels) -> bool {
        for matcher in &self.matchers {
            let value = labels.get(&matcher.name).unwrap_or("");
            let is_match = match matcher.op {
                MatchOp::Eq => value == matcher.value,
                MatchOp::Neq => value != matcher.value,
                MatchOp::Re | MatchOp::Nre => {
                    let regex_matches = matcher
                        .regex
                        .as_ref()
                        .is_some_and(|regex| regex.is_match(value));
                    if matcher.op == MatchOp::Re {
                        regex_matches
                    } else {
                        !regex_matches
                    }
                }
            };
            if !is_match {
                return false;
            }
        }
        true
    }
}
