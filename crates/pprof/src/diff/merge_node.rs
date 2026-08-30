use super::{BTreeSet, MergedNode, TreeSnapshotNode, children_by_name};

pub(crate) fn merge_node(
    left: Option<usize>,
    left_nodes: &[TreeSnapshotNode],
    right: Option<usize>,
    right_nodes: &[TreeSnapshotNode],
    fallback_name: &str,
    out: &mut Vec<MergedNode>,
) -> usize {
    let name = left
        .map(|idx| left_nodes[idx].name.clone())
        .or_else(|| right.map(|idx| right_nodes[idx].name.clone()))
        .unwrap_or_else(|| fallback_name.to_string());
    let idx = out.len();
    out.push(MergedNode {
        name,
        total_left: left.map_or(0, |node| left_nodes[node].total),
        self_left: left.map_or(0, |node| left_nodes[node].self_),
        total_right: right.map_or(0, |node| right_nodes[node].total),
        self_right: right.map_or(0, |node| right_nodes[node].self_),
        children: Vec::new(),
    });

    let left_children = children_by_name(left, left_nodes);
    let right_children = children_by_name(right, right_nodes);
    let child_names: BTreeSet<&String> =
        left_children.keys().chain(right_children.keys()).collect();
    for name in child_names {
        let child = merge_node(
            left_children.get(name).copied(),
            left_nodes,
            right_children.get(name).copied(),
            right_nodes,
            name,
            out,
        );
        out[idx].children.push(child);
    }
    idx
}
