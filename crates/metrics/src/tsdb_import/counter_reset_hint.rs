use super::ResetHint;

/// The counter-reset hint of the sample at `index` in a histogram chunk.
///
/// This matches Prometheus `counterResetHint`: a gauge chunk marks every
/// sample as a gauge, and a counter chunk marks every sample after the first
/// as not a reset. The first sample is unknown, because the chunk before it
/// may be gone or may have been written later by a backfill.
pub fn counter_reset_hint(header: u8, index: u16) -> ResetHint {
    const GAUGE: u8 = 0xc0;
    if header & GAUGE == GAUGE {
        ResetHint::Gauge
    } else if index > 0 {
        ResetHint::No
    } else {
        ResetHint::Unknown
    }
}
