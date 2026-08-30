use super::{UnixNanos, MAX_SAMPLE_TIMESTAMP_MS};

/// Whether a reading sits beyond the sane future bound the OTLP path already
/// applies to a sample timestamp.
///
/// A clamp of such a value would poison the per-series out-of-order and too-old
/// window downstream, so the caller drops the request instead. The comparison
/// widens to `i128`, which holds both a negative millisecond coordinate and the
/// whole `u64` bound without a lossy conversion.
pub(crate) fn is_too_far_in_the_future(reading: UnixNanos) -> bool {
    i128::from(reading.epoch_millis()) > i128::from(MAX_SAMPLE_TIMESTAMP_MS)
}
