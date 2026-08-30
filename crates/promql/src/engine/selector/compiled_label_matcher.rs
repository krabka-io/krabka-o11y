use super::*;

/// One label matcher with its `=~`/`!~` regex compiled ahead of time, anchored
/// `^(?:...)$` exactly as `labels_match` anchors it.
pub(crate) struct CompiledLabelMatcher {
    pub(crate) name: String,
    pub(crate) op: MatchOp,
    /// The literal comparand for `Eq`/`Neq`. This field is also the source of the
    /// precompiled, anchored regex, but the compiled form lives in `regex`.
    pub(crate) value: String,
    pub(crate) regex: Option<Regex>,
}
