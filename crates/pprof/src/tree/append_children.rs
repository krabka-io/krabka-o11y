use super::{Bar, HashSet, OTHER_NAME, Tree, subtree_self};

pub(crate) fn append_children(
    tree: &Tree,
    keep: &HashSet<usize>,
    parent: &Bar,
    next: &mut Vec<Bar>,
) {
    let Some(parent_node) = parent.node else {
        return;
    };
    let mut x = parent.x_start + parent.self_;
    let mut other_total = 0;
    let mut other_self = 0;
    for child in &tree.nodes[parent_node].children {
        let node = &tree.nodes[*child];
        if keep.contains(child) {
            next.push(Bar {
                node: Some(*child),
                name: node.name.clone(),
                total: node.total,
                self_: node.self_,
                x_start: x,
                level: parent.level + 1,
            });
        } else {
            other_total += node.total;
            other_self += subtree_self(tree, *child);
        }
        x += node.total;
    }
    if other_total > 0 {
        next.push(Bar {
            node: None,
            name: OTHER_NAME.to_string(),
            total: other_total,
            self_: other_self,
            x_start: parent.x_start + parent.total - other_total,
            level: parent.level + 1,
        });
    }
}
