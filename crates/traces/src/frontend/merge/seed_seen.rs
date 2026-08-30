use super::*;

/// Record every spanId in `trace` so later unions can dedup against it.
pub(crate) fn seed_seen(trace: &TraceByIdResponseJson, seen: &mut BTreeSet<String>) {
    for rs in &trace.trace.resource_spans {
        for ss in &rs.scope_spans {
            for span in &ss.spans {
                seen.insert(span.span_id.clone());
            }
        }
    }
}
