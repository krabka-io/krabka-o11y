use super::*;

pub(crate) fn order_spans(spans: &mut [Span]) {
    spans.sort_by_key(|span| (span.start_ns, span.span_id));
}
