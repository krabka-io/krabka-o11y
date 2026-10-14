use std::borrow::Borrow;

use super::Span;

pub(crate) fn order_spans(spans: &mut [impl Borrow<Span>]) {
    spans.sort_by(|left, right| {
        let left = left.borrow();
        let right = right.borrow();
        left.start_ns
            .cmp(&right.start_ns)
            .then_with(|| left.span_id.cmp(&right.span_id))
    });
}
