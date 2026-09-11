use super::{ByteSize, ByteSizeExt, HttpQueryError, QuerierState, StreamPlan, planned_block_bytes};

/// Applies the tenant's cap on the summed size of the blocks a query plans to
/// read. `Loki` calls it `max_query_bytes_read`.
pub(crate) fn validate_query_bytes_limit(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<(), HttpQueryError> {
    let max_query_read = state.limits.max_query_read;
    if max_query_read <= ByteSize::ZERO {
        return Ok(());
    }
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
