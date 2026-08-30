use super::*;

pub(crate) fn search_json(resp: SearchResponse) -> Value {
    let inspected_traces = resp.inspected_traces;
    let inspected_bytes = resp.inspected.bytes_u64();
    // Spans this response scanned/returned: the distinct matched spans across
    // every returned trace's spanSets. The frontend folds this per-job sum into
    // the merged `metrics.inspectedSpans`.
    let inspected_spans: usize = resp
        .traces
        .iter()
        .flat_map(|trace| trace.span_sets.iter())
        .map(|set| set.spans.len())
        .sum();
    json!({
        "traces": resp.traces.into_iter().map(|trace| {
            json!({
                "traceID": hex::encode(trace.trace_id),
                "rootServiceName": trace.root_service_name,
                "rootTraceName": trace.root_trace_name,
                "startTimeUnixNano": trace.start_time_unix_nano.to_string(),
                // Truncated, not rounded: Tempo integer-divides its nanosecond duration,
                // and the frontend merges this querier JSON into the public search
                // response, so a rounded value would surface there too.
                "durationMs": trace.duration.millis_i64_trunc(),
                "spanSets": trace.span_sets.into_iter().map(|set| {
                    json!({
                        "spans": set.spans.iter().map(search_span_json).collect::<Vec<_>>(),
                        "matched": set.matched,
                    })
                }).collect::<Vec<_>>(),
            })
        }).collect::<Vec<_>>(),
        // Per-response job accounting the frontend folds (`metrics.add`): this
        // search ran as one completed job. `inspectedBytes` is the decoded size of
        // the cold+live data the scan inspected (threaded up from the SpanStore).
        "metrics": {
            "completedJobs": 1,
            "totalBlocks": 0,
            "inspectedTraces": inspected_traces,
            "inspectedSpans": inspected_spans,
            "inspectedBytes": inspected_bytes,
        },
    })
}
