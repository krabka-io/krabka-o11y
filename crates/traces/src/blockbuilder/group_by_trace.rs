use super::{BTreeMap, Span, SpanRecord};

/// Group records by tenant and trace id, and sort each trace for stable DFS
/// input.
#[must_use]
pub fn group_by_trace(records: &[SpanRecord]) -> BTreeMap<(String, [u8; 16]), Vec<Span>> {
    let mut grouped: BTreeMap<(String, [u8; 16]), Vec<Span>> = BTreeMap::new();
    for record in records {
        grouped
            .entry((record.tenant.clone(), record.span.trace_id))
            .or_default()
            .push(record.span.clone());
    }
    for spans in grouped.values_mut() {
        spans.sort_by_key(|span| (span.start_ns, span.span_id));
    }
    grouped
}
