use std::convert::Infallible;

use super::{CompiledLabelMatcher, LabelValueMatcher, Labels, label_value_matches};

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
        self.matchers.iter().all(|matcher| {
            let is_match = label_value_matches::<Infallible>(
                labels,
                LabelValueMatcher {
                    name: &matcher.name,
                    op: matcher.op,
                    expected: matcher.value.as_bytes(),
                },
                |label_value| {
                    Ok(matcher
                        .regex
                        .as_ref()
                        .is_some_and(|regex| regex.is_match(label_value)))
                },
            );
            match is_match {
                Ok(is_match) => is_match,
                Err(never) => match never {},
            }
        })
    }
}
