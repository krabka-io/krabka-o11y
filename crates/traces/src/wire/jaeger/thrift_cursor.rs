//! Reading steps that the binary and compact Thrift inputs share.

use super::WireError;

/// The deepest struct or collection nesting that a Thrift `skip` descends
/// through.
///
/// Each level costs bytes on the wire and one stack frame, so an unbounded
/// skip turns a request body or a 64 KiB datagram into a stack overflow.
/// Apache Thrift's own protocols stop at the same depth, and a Jaeger batch
/// nests four deep.
pub(crate) const MAX_SKIP_DEPTH: u32 = 64;

/// Take the byte of `bytes` at `*pos`, and advance `*pos` past it.
pub(crate) fn next_byte(bytes: &[u8], pos: &mut usize) -> Result<u8, WireError> {
    let Some(byte) = bytes.get(*pos).copied() else {
        return Err(WireError::Decode("unexpected end of thrift payload".into()));
    };
    *pos += 1;
    Ok(byte)
}

/// Step one level deeper than `depth`, refusing nesting past
/// [`MAX_SKIP_DEPTH`].
pub(crate) fn descend_skip_depth(depth: u32) -> Result<u32, WireError> {
    let depth = depth.saturating_add(1);
    if depth > MAX_SKIP_DEPTH {
        return Err(WireError::Decode(format!(
            "thrift nesting deeper than {MAX_SKIP_DEPTH}"
        )));
    }
    Ok(depth)
}

/// A decoded length or count as a `usize`, or the decode error `out_of_range`.
pub(crate) fn wire_length<T>(decoded: T, out_of_range: &str) -> Result<usize, WireError>
where
    usize: TryFrom<T>,
{
    usize::try_from(decoded).map_err(|_| WireError::Decode(out_of_range.into()))
}


/// A collection header as read from the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

/// A map header as read from the wire.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct MapHeader {
    pub(crate) key_type: u8,
    pub(crate) value_type: u8,
    pub(crate) len: usize,
}

/// A field header as read from a Thrift struct.
#[derive(Clone, Copy)]
pub(crate) struct FieldHeader {
    pub(crate) field_type: u8,
    pub(crate) field_id: i16,
}

/// Refuse a map whose keys or values fail [`check_collection_header`].
pub(crate) fn check_map_header(
    header: MapHeader,
    stop: u8,
    remaining_bytes: usize,
) -> Result<(), WireError> {
    let MapHeader {
        key_type,
        value_type,
        len,
    } = header;
    for element_type in [key_type, value_type] {
        check_collection_header(
            CollectionHeader { element_type, len },
            stop,
            remaining_bytes,
        )?;
    }
    Ok(())
}


/// Decode a thrift string's bytes, which must be UTF-8.
pub(crate) fn utf8_string(bytes: Vec<u8>) -> Result<String, WireError> {
    String::from_utf8(bytes).map_err(|err| WireError::Decode(err.to_string()))
}

pub(crate) fn read_utf8_binary(
    read_binary: impl FnOnce() -> Result<Vec<u8>, WireError>,
) -> Result<String, WireError> {
    utf8_string(read_binary()?)
}
