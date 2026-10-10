use krabka_blockstore::{CycleSpans, SpanNode};

use super::{InputSpan, NestedSet};

/// Assigns nested-set intervals to `spans`, aligned by index.
///
/// Delegates to [`krabka_blockstore::assign_nested_set`], which owns the
/// traversal, the Tempo `-1` root sentinel, and the cycle handling.
#[must_use]
pub fn assign_nested_set(spans: &[InputSpan]) -> Vec<NestedSet> {
    let nodes: Vec<SpanNode> = spans
        .iter()
        .map(|span| SpanNode {
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
        })
        .collect();
    krabka_blockstore::assign_nested_set(&nodes, CycleSpans::AssignIntervals)
        .into_iter()
        .map(NestedSet::from)
        .collect()
}
