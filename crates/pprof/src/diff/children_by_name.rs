use super::{BTreeMap, TreeSnapshotNode};

pub(crate) fn children_by_name(
    node: Option<usize>,
    nodes: &[TreeSnapshotNode],
) -> BTreeMap<String, usize> {
    let mut children = BTreeMap::new();
    if let Some(index) = node {
        for child in &nodes[index].children {
            children.insert(nodes[*child].name.clone(), *child);
        }
    }
    children
}
