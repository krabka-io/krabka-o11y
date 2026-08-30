use super::{Bar, HashSet, MergedNode, OTHER_NAME, subtree_self_left, subtree_self_right};

pub(crate) fn append_children(
    nodes: &[MergedNode],
    keep: &HashSet<usize>,
    parent: &Bar,
    next: &mut Vec<Bar>,
) {
    let Some(parent_node) = parent.node else {
        return;
    };
    let mut x_left = parent.x_left;
    let mut x_right = parent.x_right;
    let mut other_total_left = 0;
    let mut other_self_left = 0;
    let mut other_total_right = 0;
    let mut other_self_right = 0;
    for child in &nodes[parent_node].children {
        let node = &nodes[*child];
        if keep.contains(child) {
            next.push(Bar {
                node: Some(*child),
                name: node.name.clone(),
                total_left: node.total_left,
                self_left: node.self_left,
                total_right: node.total_right,
                self_right: node.self_right,
                x_left,
                x_right,
            });
        } else {
            other_total_left += node.total_left;
            other_self_left += subtree_self_left(nodes, *child);
            other_total_right += node.total_right;
            other_self_right += subtree_self_right(nodes, *child);
        }
        x_left += node.total_left;
        x_right += node.total_right;
    }
    if other_total_left > 0 || other_total_right > 0 {
        next.push(Bar {
            node: None,
            name: OTHER_NAME.to_string(),
            total_left: other_total_left,
            self_left: other_self_left,
            total_right: other_total_right,
            self_right: other_self_right,
            x_left: parent.x_left + parent.total_left - other_total_left,
            x_right: parent.x_right + parent.total_right - other_total_right,
        });
    }
}
