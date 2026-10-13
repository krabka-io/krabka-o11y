use super::MergedNode;

pub(crate) fn parents(nodes: &[MergedNode]) -> Vec<Option<usize>> {
    let mut parents = vec![None; nodes.len()];
    for parent in 0..nodes.len() {
        let node = &nodes[parent];
        for child in &node.children {
            parents[*child] = Some(parent);
        }
    }
    parents
}
