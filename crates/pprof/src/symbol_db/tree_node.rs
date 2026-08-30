use super::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TreeNode {
    pub(crate) parent: i32,
    pub(crate) location_ref: i32,
}
