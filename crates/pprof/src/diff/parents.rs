use super::MergedNode;

pub(crate) fn parents(nodes: &[MergedNode]) -> Vec<Option<usize>> {
    let mut parents = vec![None; nodes.len()];
    for (parent, node) in nodes.iter().enumerate() {
        for child in &node.children {
            parents[*child] = Some(parent);
        }
    }
    parents
}
