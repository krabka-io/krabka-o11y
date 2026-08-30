use super::{TraceJson, merge_span_sets, parse_nanos};

/// Fold one trace into the merged set. This appends the trace when it is new.
/// Otherwise it reunions the trace's spanSets into the existing trace with the
/// same `traceID`.
pub(crate) fn merge_trace(merged: &mut Vec<TraceJson>, trace: TraceJson) {
    let Some(existing) = merged.iter_mut().find(|t| t.trace_id == trace.trace_id) else {
        merged.push(trace);
        return;
    };
    // Earliest start wins (newest-first ordering uses startTimeUnixNano).
    if parse_nanos(&trace.start_time_unix_nano) < parse_nanos(&existing.start_time_unix_nano) {
        existing
            .start_time_unix_nano
            .clone_from(&trace.start_time_unix_nano);
    }
    if trace.duration > existing.duration {
        existing.duration = trace.duration;
    }
    if existing.root_service_name.is_empty() {
        existing
            .root_service_name
            .clone_from(&trace.root_service_name);
    }
    if existing.root_trace_name.is_empty() {
        existing.root_trace_name.clone_from(&trace.root_trace_name);
    }
    merge_span_sets(&mut existing.span_sets, trace.span_sets);
}
