use krabka_blockstore::{CycleSpans, SpanNode, assign_nested_set};

use super::SpanRef;

pub(crate) fn recompute_trace_nested_sets(spans: &mut [SpanRef]) {
    let nodes = spans
        .iter()
        .map(|span| SpanNode {
            span_id: span.span_id,
            parent_span_id: span.parent_span_id,
        })
        .collect::<Vec<_>>();
    for entry in spans
        .iter_mut()
        .zip(assign_nested_set(&nodes, CycleSpans::LeaveUnassigned))
    {
        let span = entry.0;
        let nested_set = entry.1;
        span.nested_set_left = nested_set.nested_set_left;
        span.nested_set_right = nested_set.nested_set_right;
        span.nested_set_parent = nested_set.parent_id;
    }
}
