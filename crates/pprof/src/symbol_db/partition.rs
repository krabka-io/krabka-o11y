use super::{Deserialize, HashMap, Serialize, TreeNode};

#[derive(Default, Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct TreeChildKey {
    pub parent: i32,
    pub location_ref: i32,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Partition {
    pub(crate) nodes: Vec<TreeNode>,
    #[serde(skip)]
    pub(crate) children: HashMap<TreeChildKey, u32>,
}

impl Partition {
    pub(crate) fn rebuild_children(&mut self) {
        self.children.clear();
        for idx in 0..self.nodes.len() {
            let node = &self.nodes[idx];
            let idx = u32::try_from(idx).expect("node index fits u32");
            self.children.insert(
                TreeChildKey {
                    parent: node.parent,
                    location_ref: node.location_ref,
                },
                idx,
            );
        }
    }
}
