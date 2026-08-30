use super::{DecodedClockReading, DecodedSeries, UnixNanos, clock_identity_labels, clock_projection, decoded_series};

/// Builds every series a clock batch publishes.
///
/// The first series of each reading is the clock block's own identity, which
/// carries no float sample. The rest are the projection: ordinary float series
/// that `PromQL`, the ruler, and Grafana read with no query-path change. The
/// block stays the source of truth, and the projection is a derived view of it.
#[must_use]
pub fn clock_series(
    readings: &[DecodedClockReading],
    ingest_unix_nanos: UnixNanos,
) -> Vec<DecodedSeries> {
    let mut out = Vec::new();
    for reading in readings {
        out.push(decoded_series(clock_identity_labels(reading), None));
        out.extend(clock_projection(reading, ingest_unix_nanos));
    }
    out
}
