use super::{
    BlockIndex, Empty, LabelIndex, PlanError, StreamPlan, StreamQuery, TimeRange, label_predicate,
};

#[tracing::instrument(
    level = "info",
    skip_all,
    fields(
        tenant = Empty,
        matchers = query.matchers.len(),
        pipeline_stages = query.pipeline.len(),
        fingerprints = Empty,
        blocks = Empty,
    ),
    err
)]
/// # Errors
/// Returns an error when the query or template is malformed, a requested conversion is invalid, or evaluation cannot read its input data.
pub fn plan_stream_query(
    tenant: impl Into<String>,
    time_range: TimeRange,
    query: StreamQuery,
    label_index: &LabelIndex,
    block_index: &BlockIndex,
) -> Result<StreamPlan, PlanError> {
    let tenant = tenant.into();
    tracing::Span::current().record("tenant", tenant.as_str());
    let predicates = query
        .matchers
        .iter()
        .map(label_predicate)
        .collect::<Result<Vec<_>, _>>()?;
    let fingerprints = label_index.match_series(&tenant, &predicates);
    let fingerprint_list = fingerprints.iter().copied().collect::<Vec<_>>();
    let blocks = if fingerprint_list.is_empty() {
        Vec::new()
    } else {
        block_index.match_blocks(&tenant, time_range, &fingerprint_list)
    };
    let span = tracing::Span::current();
    span.record("fingerprints", fingerprint_list.len());
    span.record("blocks", blocks.len());

    Ok(StreamPlan {
        tenant,
        time_range,
        query,
        fingerprints,
        blocks,
    })
}
