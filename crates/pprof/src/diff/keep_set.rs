use super::{HashSet, MergedNode, combined_total, parents};

pub(crate) fn keep_set(nodes: &[MergedNode], root: usize, max_nodes: i64) -> HashSet<usize> {
    if max_nodes <= 0 || nodes.len() <= usize::try_from(max_nodes).unwrap_or(usize::MAX) {
        return (0..nodes.len()).collect();
    }
    let max_nodes = usize::try_from(max_nodes).unwrap_or(usize::MAX);
    let parents = parents(nodes);
    let mut ranked: Vec<usize> = (0..nodes.len()).filter(|node| *node != root).collect();
    ranked.sort_by(|left, right| {
        combined_total(nodes, *right)
            .cmp(&combined_total(nodes, *left))
            .then_with(|| nodes[*left].name.cmp(&nodes[*right].name))
            .then_with(|| left.cmp(right))
    });
    let mut keep = HashSet::from([root]);
    for node in ranked {
        let mut path = Vec::new();
        let mut current = Some(node);
        while let Some(idx) = current {
            if keep.contains(&idx) {
                break;
            }
            path.push(idx);
            current = parents[idx];
        }
        if keep.len() + path.len() <= max_nodes {
            keep.extend(path);
        }
    }
    keep
}
