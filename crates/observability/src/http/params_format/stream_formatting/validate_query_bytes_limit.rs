use super::*;

pub(crate) fn validate_query_bytes_limit(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<(), HttpQueryError> {
    let Some(max_query_read) = state.max_query_read else {
        return Ok(());
    };
    let planned = planned_block_bytes(plan);
    if planned > max_query_read {
        // The error carries plain integers so its rendered message is fixed by
        // the `#[error]` format string alone.
        return Err(HttpQueryError::QueryBytesTooLarge {
            planned_bytes: planned.bytes_u64(),
            max_bytes: max_query_read.bytes_u64(),
        });
    }
    Ok(())
}
