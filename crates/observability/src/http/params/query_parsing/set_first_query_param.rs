use super::HttpQueryError;

/// Fills `slot` from `parse` unless an earlier pair of the query already did.
///
/// Loki reads the first occurrence of a repeated parameter, so a later one is
/// neither parsed nor checked.
pub(crate) fn set_first_query_param<T>(
    slot: &mut Option<T>,
    parse: impl FnOnce() -> Result<T, HttpQueryError>,
) -> Result<(), HttpQueryError> {
    if slot.is_none() {
        *slot = Some(parse()?);
    }
    Ok(())
}
