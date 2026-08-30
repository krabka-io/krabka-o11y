use super::{
    BTreeSet, ByteSize, Metrics, TraceByIdResponseJson, TracePartial, TraceStatus, seed_seen,
    union_trace_bodies,
};

/// Assemble one trace from per-querier by-id partials.
///
/// This unions `resourceSpans`, dedupes spans by `spanId`, and accumulates
/// metrics. It flags `Partial` when the assembled trace exceeds `max_trace`, or
/// when any partial reported `PARTIAL`.
///
/// It returns `None` when no querier returned the trace.
#[must_use]
pub fn assemble_trace(
    partials: Vec<TracePartial>,
    max_trace: ByteSize,
) -> (Option<TraceByIdResponseJson>, Metrics, TraceStatus) {
    let mut metrics = Metrics::default();
    let mut acc: Option<TraceByIdResponseJson> = None;
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut any_partial = false;

    for p in partials {
        metrics.add(&p.metrics);
        if p.trace.status.eq_ignore_ascii_case("PARTIAL") {
            any_partial = true;
        }
        if p.trace.is_empty() {
            continue;
        }
        if let Some(existing) = &mut acc {
            union_trace_bodies(existing, p.trace, &mut seen);
        } else {
            seed_seen(&p.trace, &mut seen);
            acc = Some(p.trace);
        }
    }

    let status = match &acc {
        Some(t) if any_partial || t.approx_size() > max_trace => TraceStatus::Partial,
        _ => TraceStatus::Complete,
    };
    (acc, metrics, status)
}
