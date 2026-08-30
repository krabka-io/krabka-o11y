use super::{BTreeMap, TreeSnapshotNode};

pub(crate) fn children_by_name(
    node: Option<usize>,
    nodes: &[TreeSnapshotNode],
) -> BTreeMap<String, usize> {
    node.map_or_else(BTreeMap::new, |idx| {
        nodes[idx]
            .children
            .iter()
            .map(|child| (nodes[*child].name.clone(), *child))
            .collect()
    })
}
