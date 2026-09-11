use super::{ByteSize, ByteSizeExt, HttpQueryError, QuerierState};

/// Applies the tenant's cap on the bytes of `LogQL` source text.
///
/// Krabka's own limit, and not `Loki`'s `max_query_length`, which caps the
/// `[start, end]` window instead.
pub(crate) fn validate_query_string_bytes_limit(
    state: &QuerierState,
    query: &str,
) -> Result<(), HttpQueryError> {
    let limit = state.limits.max_query_string_bytes;
    if limit <= ByteSize::ZERO || query.len() <= limit.bytes_usize() {
        return Ok(());
    }
    Err(HttpQueryError::QueryStringTooLong {
        query_bytes: query.len(),
        max_bytes: limit.bytes_usize(),
    })
}
