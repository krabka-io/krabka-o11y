use super::*;

pub(crate) fn parse_non_negative_whole_byte_size_or_bytes(value: &str) -> Result<ByteSize, String> {
    let size = value.parse::<u64>().map_or_else(
        |_| parse::non_negative_byte_size(value).map_err(|error| error.to_string()),
        |bytes| Ok(ByteSize::from_bytes(bytes)),
    )?;
    let bytes = size.bytes_f64();
    if bytes.fract() != 0.0 || bytes > 9_007_199_254_740_992.0 {
        return Err(
            "size must be a non-negative whole-byte value exactly representable by UOM".to_owned(),
        );
    }
    Ok(size)
}
