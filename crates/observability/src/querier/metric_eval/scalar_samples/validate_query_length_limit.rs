use super::{ByteSizeExt, HttpQueryError, QuerierState};

pub(crate) fn validate_query_length_limit(
    state: &QuerierState,
    query: &str,
) -> Result<(), HttpQueryError> {
    let Some(max_query_length) = state.max_query_length.map(ByteSizeExt::bytes_usize) else {
        return Ok(());
    };
    let query_length = query.len();
    if query_length > max_query_length {
        return Err(HttpQueryError::QueryLengthTooLarge {
            query_length,
            max_query_length,
        });
    }
    Ok(())
}
