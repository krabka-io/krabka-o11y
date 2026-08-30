use super::*;

/// Builds the two state-enum families a clock reading publishes.
///
/// Prometheus carries an enumerated state as one series per value with an extra
/// label, the current value at `1` and every other value at `0`. Every value
/// goes out on every reading, so a transition overwrites the old `1` with a `0`
/// in the same scrape rather than leaving it to go stale.
pub(crate) fn clock_state_series(reading: &DecodedClockReading, timestamp_ms: i64) -> Vec<DecodedSeries> {
    let mut out = ClockSyncState::ALL
        .iter()
        .map(|state| {
            decoded_series(
                projected_labels(
                    reading,
                    "krabka_clock_sync_state",
                    &[("state", state.as_label())],
                ),
                Some(DecodedSample::new(
                    timestamp_ms,
                    indicator(*state == reading.sync_state),
                )),
            )
        })
        .collect::<Vec<_>>();

    // A reading from a source other than GNSS carries no fix quality, so it
    // publishes no fix family at all rather than a family of zeros.
    if let Some(current) = reading.gnss.and_then(|gnss| gnss.fix) {
        out.extend(GnssFix::ALL.iter().map(|fix| {
            decoded_series(
                projected_labels(reading, "krabka_gnss_fix", &[("fix", fix.as_label())]),
                Some(DecodedSample::new(timestamp_ms, indicator(*fix == current))),
            )
        }));
    }
    out
}
