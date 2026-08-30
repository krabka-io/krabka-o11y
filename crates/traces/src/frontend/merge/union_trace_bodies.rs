use super::*;

/// Union another querier's by-id body into the accumulator, and dedupe spans by
/// `spanId`. This appends new resourceSpans and scopeSpans only as needed.
pub(crate) fn union_trace_bodies(
    acc: &mut TraceByIdResponseJson,
    other: TraceByIdResponseJson,
    seen: &mut BTreeSet<String>,
) {
    for mut rs in other.trace.resource_spans {
        for ss in &mut rs.scope_spans {
            // GAP6 (documented-as-acceptable): dedup is global across resources
            // (one `seen` set), not keyed on `(resource, spanId)`. This matches
            // OTLP's invariant that a span id is unique within a trace, so the
            // same span returned by multiple queriers (each reassembles the whole
            // trace) dedups correctly. The only case it mishandles is *malformed*
            // input that reuses a span id across resources — then the second
            // occurrence is dropped. Keying on `(resource, spanId)` would require
            // serializing each resource `Value` per span (not free) to defend a
            // spec-violating input, so we keep the cheaper global dedup.
            ss.spans.retain(|span| seen.insert(span.span_id.clone()));
        }
        rs.scope_spans.retain(|ss| !ss.spans.is_empty());
        if !rs.scope_spans.is_empty() {
            // Merge into an existing resourceSpans group with an equal resource,
            // else append a new group.
            //
            // GAP4 (documented-as-acceptable): grouping is by raw
            // `serde_json::Value` equality, so the *same logical resource* with a
            // different attribute ordering would form two sibling groups. A
            // correct canonicalization is NOT cheap here: OTLP arrays are
            // semantically ordered in general, and only the `attributes` array is
            // order-insensitive — sorting it blindly would require structural
            // OTLP knowledge this typed-`Value` mirror deliberately doesn't have.
            // In practice every querier renders a resource through the same
            // `attrs_json` code path with a deterministic key order, so the same
            // logical resource serializes identically across queriers and matches
            // exactly. Duplicated groups would only cosmetically split a resource;
            // no span is dropped or duplicated.
            if let Some(existing) = acc
                .trace
                .resource_spans
                .iter_mut()
                .find(|e| e.resource == rs.resource)
            {
                merge_scope_spans(existing, rs.scope_spans);
            } else {
                acc.trace.resource_spans.push(rs);
            }
        }
    }
}
