use super::{DecodedClockReading, projected_labels, CLOCK_READING_METRIC};

/// The label set that identifies one clock on one host.
pub(crate) fn clock_identity_labels(reading: &DecodedClockReading) -> Vec<(String, String)> {
    projected_labels(reading, CLOCK_READING_METRIC, &[])
}
