use super::WireError;

/// A collection header as read from the wire.
#[derive(Clone, Copy)]
pub(crate) struct CollectionHeader {
    pub(crate) element_type: u8,
    pub(crate) len: usize,
}

/// Refuse a collection whose elements cannot fit in the `remaining_bytes` of
/// the input, for a protocol whose every element occupies at least one byte.
/// `stop` is the protocol's stop type, which terminates a struct and is no
/// element type at all.
pub(crate) fn check_collection_header(
    header: CollectionHeader,
    stop: u8,
    remaining_bytes: usize,
) -> Result<(), WireError> {
    let CollectionHeader { element_type, len } = header;
    if len == 0 {
        return Ok(());
    }
    if element_type == stop {
        return Err(WireError::Decode(
            "stop is not a collection element type".into(),
        ));
    }
    if len > remaining_bytes {
        return Err(WireError::Decode(format!(
            "collection of {len} elements exceeds the {remaining_bytes} bytes left"
        )));
    }
    Ok(())
}
