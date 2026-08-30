use super::{ByteSize, parse_positive_whole_byte_size, DEFAULT_SCAN_CONCAT_MAX};

pub(crate) fn parse_scan_concat_max(value: &str) -> Result<ByteSize, String> {
    let size = parse_positive_whole_byte_size(value)?;
    if size > DEFAULT_SCAN_CONCAT_MAX {
        return Err("scan concatenation maximum must not exceed 1.5GB".to_owned());
    }
    Ok(size)
}
