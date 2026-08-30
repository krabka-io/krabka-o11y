use super::{ByteSize, ByteSizeExt, parse};

pub(crate) fn parse_remote_read_max_body(value: &str) -> Result<ByteSize, String> {
    let value = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    let bytes = value.bytes_u64();
    if ByteSize::from_bytes(bytes) == value {
        Ok(value)
    } else {
        Err("remote-read maximum body must be a whole-byte value".to_owned())
    }
}
