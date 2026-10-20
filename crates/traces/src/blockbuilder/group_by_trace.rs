use super::{BTreeMap, Span, SpanRecord};

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct TraceIdentity {
    pub tenant: String,
    pub trace_id: [u8; 16],
}

/// Group records by tenant and trace id, and sort each trace for stable DFS
/// input.
#[must_use]
pub fn group_by_trace(records: &[SpanRecord]) -> BTreeMap<TraceIdentity, Vec<Span>> {
    let mut grouped = group_by_trace_in_record_order(records);
    for spans in grouped.values_mut() {
        spans.sort_by(|left, right| {
            left.start_ns
                .cmp(&right.start_ns)
                .then_with(|| left.span_id.cmp(&right.span_id))
        });
    }
    grouped
}

/// Group records by tenant and trace id, and keep each trace's spans in the
/// order the records hold them.
#[must_use]
pub fn group_by_trace_in_record_order(
    records: &[SpanRecord],
) -> BTreeMap<TraceIdentity, Vec<Span>> {
    let mut grouped: BTreeMap<TraceIdentity, Vec<Span>> = BTreeMap::new();
    for record in records {
        grouped
            .entry(TraceIdentity {
                tenant: record.tenant.clone(),
                trace_id: record.span.trace_id,
            })
            .or_default()
            .push(record.span.clone());
    }
    grouped
}
