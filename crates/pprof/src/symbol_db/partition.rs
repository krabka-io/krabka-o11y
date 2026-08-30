use super::{Deserialize, HashMap, Serialize, TreeNode};

#[derive(Default, Clone, Debug, Serialize, Deserialize)]
pub(crate) struct Partition {
    pub(crate) nodes: Vec<TreeNode>,
    #[serde(skip)]
    pub(crate) children: HashMap<(i32, i32), u32>,
}

impl Partition {
    pub(crate) fn rebuild_children(&mut self) {
        self.children.clear();
        for (idx, node) in self.nodes.iter().enumerate() {
            let idx = u32::try_from(idx).expect("node index fits u32");
            self.children.insert((node.parent, node.location_ref), idx);
        }
    }
}
