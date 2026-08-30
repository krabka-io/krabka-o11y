use super::*;

/// One span's nested-set assignment, aligned by index with the input spans.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NestedSet {
    pub left: i32,
    pub right: i32,
    pub parent_id: i32,
}
