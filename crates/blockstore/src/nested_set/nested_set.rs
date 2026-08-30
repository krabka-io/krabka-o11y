use super::*;

/// The three nested-set columns for one span.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NestedSet {
    pub nested_set_left: i32,
    pub nested_set_right: i32,
    pub parent_id: i32,
}
