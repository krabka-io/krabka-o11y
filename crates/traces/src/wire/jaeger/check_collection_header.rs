use super::WireError;

/// Refuse a collection of `len` elements of `element_type` that cannot fit in
/// the bytes left after `pos`, for a protocol whose every element occupies at
/// least one byte. `stop` is the protocol's stop type, which terminates a
/// struct and is no element type at all.
pub(crate) fn check_collection_header(
    bytes: &[u8],
    pos: usize,
    stop: u8,
    element_type: u8,
    len: usize,
) -> Result<(), WireError> {
    if len == 0 {
        return Ok(());
    }
    if element_type == stop {
        return Err(WireError::Decode(
            "stop is not a collection element type".into(),
        ));
    }
    let remaining = bytes.len().saturating_sub(pos);
    if len > remaining {
        return Err(WireError::Decode(format!(
            "collection of {len} elements exceeds the {remaining} bytes left"
        )));
    }
    Ok(())
}
