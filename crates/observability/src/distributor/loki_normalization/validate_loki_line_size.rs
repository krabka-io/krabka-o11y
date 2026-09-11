use super::{ByteSize, ByteSizeExt, DistributorError, Labels, Limits, loki_stale_sample_label_set};

/// Applies `Loki`'s `max_line_size` to one entry.
///
/// The size is the line's UTF-8 byte count, which is what `Loki` measures.
/// Krabka always discards an oversize line: `Loki`'s
/// `max_line_size_truncate` cuts the line instead, and Krabka does not
/// implement that switch, so an operator who wants a line kept must raise the
/// cap.
pub(crate) fn validate_loki_line_size(
    line: &str,
    stream_labels: &Labels,
    limits: &Limits,
) -> Result<(), DistributorError> {
    if limits.max_line_size <= ByteSize::ZERO {
        return Ok(());
    }
    let limit_bytes = limits.max_line_size.bytes_usize();
    if line.len() <= limit_bytes {
        return Ok(());
    }
    Err(DistributorError::LineTooLong {
        stream: loki_stale_sample_label_set(stream_labels),
        limit_bytes,
        line_bytes: line.len(),
    })
}
