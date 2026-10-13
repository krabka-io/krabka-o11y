use super::SpanRef;

pub(crate) fn deduplicate_search_spans(spans: &mut Vec<SpanRef>) {
    spans.sort_by_key(|span| span.span_id);
    spans.dedup_by_key(|span| span.span_id);
    spans.sort_by(|left, right| {
        left.start_time_unix_nano
            .cmp(&right.start_time_unix_nano)
            .then_with(|| left.span_id.cmp(&right.span_id))
    });
}
