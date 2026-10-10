use std::borrow::Borrow;

use krabka_blockstore::{CycleSpans, SpanNode};

use super::{NestedSet, Span};

/// Assign modified pre-order traversal intervals to spans of one trace.
///
/// Delegates to [`krabka_blockstore::assign_nested_set`], which owns the
/// traversal and the Tempo `-1` root sentinel. Spans caught in a parent cycle
/// are left at `{0, 0, 0}` ([`CycleSpans::LeaveUnassigned`]), as this crate's
/// block writer and live store have always written them.
#[must_use]
pub fn assign_nested_set(spans: &[impl Borrow<Span>]) -> Vec<NestedSet> {
    let nodes: Vec<SpanNode> = spans
        .iter()
        .map(|span| {
            let span = span.borrow();
            SpanNode {
                span_id: span.span_id,
                parent_span_id: span.parent_span_id,
            }
        })
        .collect();
    krabka_blockstore::assign_nested_set(&nodes, CycleSpans::LeaveUnassigned)
        .into_iter()
        .map(NestedSet::from)
        .collect()
}
