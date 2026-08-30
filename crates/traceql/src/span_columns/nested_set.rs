use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NestedSet {
    pub left: i32,
    pub right: i32,
    pub parent_id: i32,
}
