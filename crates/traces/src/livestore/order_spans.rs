use std::borrow::Borrow;

use super::Span;

pub(crate) fn order_spans(spans: &mut [impl Borrow<Span>]) {
    spans.sort_by_key(|span| {
        let span = span.borrow();
        (span.start_ns, span.span_id)
    });
}
