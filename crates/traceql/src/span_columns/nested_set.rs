/// One span's nested-set assignment, aligned by index with the input spans.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NestedSet {
    pub left: i32,
    pub right: i32,
    pub parent_id: i32,
}

impl From<krabka_blockstore::NestedSet> for NestedSet {
    fn from(block: krabka_blockstore::NestedSet) -> Self {
        Self {
            left: block.nested_set_left,
            right: block.nested_set_right,
            parent_id: block.parent_id,
        }
    }
}
