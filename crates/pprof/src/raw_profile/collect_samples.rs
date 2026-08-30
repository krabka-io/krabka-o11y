use super::PprofBuilder;

pub(crate) fn collect_samples(
    root: usize,
    node_idx: usize,
    nodes: &[crate::tree::TreeSnapshotNode],
    path: &mut Vec<String>,
    builder: &mut PprofBuilder,
) {
    if node_idx != root {
        path.push(nodes[node_idx].name.clone());
    }
    if nodes[node_idx].self_ != 0 && !path.is_empty() {
        builder.add_sample(path, nodes[node_idx].self_);
    }
    for child in &nodes[node_idx].children {
        collect_samples(root, *child, nodes, path, builder);
    }
    if node_idx != root {
        path.pop();
    }
}
