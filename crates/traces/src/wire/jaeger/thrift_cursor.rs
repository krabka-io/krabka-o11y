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
