use super::*;

pub(crate) fn child_counts(nested: &[crate::span::nested_set::NestedSet]) -> Vec<i32> {
    // Single O(n) pass: tally how many nodes name each `parent_id`, then each
    // node's child count is the tally for its own `left` interval.
    let mut counts: HashMap<i32, i32> = HashMap::with_capacity(nested.len());
    for node in nested {
        let count = counts.entry(node.parent_id).or_insert(0);
        *count = count.saturating_add(1);
    }
    nested
        .iter()
        .map(|node| counts.get(&node.left).copied().unwrap_or(0))
        .collect()
}
