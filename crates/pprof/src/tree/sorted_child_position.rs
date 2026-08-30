use super::Node;

pub(crate) fn sorted_child_position(children: &[usize], nodes: &[Node], name: &str) -> usize {
    children
        .binary_search_by(|candidate| nodes[*candidate].name.as_str().cmp(name))
        .unwrap_or_else(|pos| pos)
}
