use super::{MatchCmp, MatchValue, ResourceAttrs};

/// One attribute comparison against a row: the attribute key, how to compare,
/// the value to compare with, and whether resource attributes take part.
#[derive(Clone, Copy)]
pub(crate) struct AttrMatch<'a> {
    pub(crate) key: &'a str,
    pub(crate) op: MatchCmp,
    pub(crate) expected: &'a MatchValue,
    pub(crate) resource: ResourceAttrs,
}
