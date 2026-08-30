use super::*;

pub(crate) fn parse_positive_whole_byte_size(value: &str) -> Result<ByteSize, String> {
    let size = parse::positive_byte_size(value).map_err(|error| error.to_string())?;
    let bytes = size.bytes_f64();
    if bytes.fract() != 0.0 || bytes > 9_007_199_254_740_992.0 {
        return Err(
            "size must be a positive whole-byte value exactly representable by UOM".to_owned(),
        );
    }
    Ok(size)
}
