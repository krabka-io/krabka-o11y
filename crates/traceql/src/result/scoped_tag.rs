use super::*;

/// Tag names grouped by scope.
#[derive(Clone, Debug, PartialEq)]
pub struct ScopedTag {
    pub scope: TagScope,
    pub tags: Vec<String>,
}
