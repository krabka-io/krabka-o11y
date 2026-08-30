use super::{SpanStore, TraceqlEngine, ScanOptions, TraceSpans, TraceqlError, SearchOptions, BTreeSet};

pub(crate) async fn matching_traces<S>(
    engine: &TraceqlEngine<S>,
    tenant: &str,
    query: &str,
    start_ns: i64,
    end_ns: i64,
    scan_options: ScanOptions,
    limit: usize,
) -> Result<Vec<TraceSpans>, TraceqlError>
where
    S: SpanStore + 'static,
{
    let resp = engine
        .search_with_options(
            tenant,
            query,
            start_ns,
            end_ns,
            SearchOptions {
                limit,
                spss: 0,
                search_limit: Some(limit),
                scan_options,
            },
        )
        .await?;
    let mut seen = BTreeSet::new();
    let mut traces = Vec::new();
    for trace in resp.traces {
        if seen.insert(trace.trace_id)
            && let Some(trace) = engine
                .trace_by_id_within(tenant, &trace.trace_id, start_ns, end_ns)
                .await?
        {
            traces.push(trace);
        }
    }
    Ok(traces)
}
