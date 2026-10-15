use super::{
    ClockReadingPayload, ClockSyncState, DecodedClockReading, DecodedSample, DecodedSeries,
    GnssFix, Time, TimeExt, UnixNanos, decoded_series,
};

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


/// Builds the projected float series for one reading.
pub(crate) fn clock_projection(
    reading: &DecodedClockReading,
    ingest_unix_nanos: UnixNanos,
) -> Vec<DecodedSeries> {
    let timestamp_ms = reading.timestamp_ms();
    let payload = ClockReadingPayload {
        reading: reading.clone(),
        ingest_unix_nanos,
    };
    let mut out = Vec::new();

    let mut gauge = |name: &str, value: f64| {
        out.push(decoded_series(
            projected_labels(reading, name, &[]),
            Some(DecodedSample::new(timestamp_ms, value)),
        ));
    };

    // Always present.
    gauge(
        "krabka_clock_uncertainty_seconds",
        reading.uncertainty().secs_f64(),
    );
    gauge(
        "krabka_clock_offset_seconds",
        Time::from_nanos(reading.offset_nanos).secs_f64(),
    );
    gauge(
        "krabka_clock_ingest_skew_seconds",
        payload.ingest_skew().secs_f64(),
    );

    // Discipline state, when the host reported it.
    if let Some(last_sync) = reading.last_sync_unix_nanos {
        gauge("krabka_clock_last_sync_seconds", last_sync.epoch_secs_f64());
    }
    if let Some(frequency_ppb) = reading.frequency_ppb {
        gauge("krabka_clock_frequency_ppb", widen(frequency_ppb));
    }
    if let Some(last_step_nanos) = reading.last_step_nanos {
        gauge(
            "krabka_clock_step_seconds_total",
            Time::from_nanos(last_step_nanos).secs_f64(),
        );
    }

    // NTP.
    if let Some(ntp) = reading.ntp {
        gauge(
            "krabka_clock_root_delay_seconds",
            Time::from_nanos(ntp.root_delay_nanos).secs_f64(),
        );
        gauge(
            "krabka_clock_root_dispersion_seconds",
            Time::from_nanos(ntp.root_dispersion_nanos).secs_f64(),
        );
        gauge("krabka_clock_stratum", f64::from(ntp.stratum));
    }

    // PTP and PHC.
    if let Some(ptp) = reading.ptp {
        gauge(
            "krabka_clock_path_delay_seconds",
            Time::from_nanos(ptp.mean_path_delay_nanos).secs_f64(),
        );
        gauge("krabka_clock_steps_removed", f64::from(ptp.steps_removed));
        gauge("krabka_clock_class", f64::from(ptp.gm_clock_class));
    }

    // GNSS.
    if let Some(gnss) = reading.gnss {
        gauge(
            "krabka_gnss_satellites_used",
            f64::from(gnss.satellites_used),
        );
    }

    out.extend(clock_state_series(reading, timestamp_ms));
    out
}

/// Builds the two state-enum families a clock reading publishes.
///
/// Prometheus carries an enumerated state as one series per value with an extra
/// label, the current value at `1` and every other value at `0`. Every value
/// goes out on every reading, so a transition overwrites the old `1` with a `0`
/// in the same scrape rather than leaving it to go stale.
pub(crate) fn clock_state_series(
    reading: &DecodedClockReading,
    timestamp_ms: i64,
) -> Vec<DecodedSeries> {
    let mut out = ClockSyncState::ALL
        .iter()
        .map(|state| {
            decoded_series(
                projected_labels(
                    reading,
                    "krabka_clock_sync_state",
                    &[krabka_blockstore::LabelPair::new("state", state.as_label())],
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
                projected_labels(
                    reading,
                    "krabka_gnss_fix",
                    &[krabka_blockstore::LabelPair::new("fix", fix.as_label())],
                ),
                Some(DecodedSample::new(timestamp_ms, indicator(*fix == current))),
            )
        }));
    }
    out
}


/// The Prometheus encoding of a boolean: `1` when it holds, `0` otherwise.
fn indicator(holds: bool) -> f64 {
    f64::from(u8::from(holds))
}

/// The label set that identifies one clock on one host.
pub(crate) fn clock_identity_labels(
    reading: &DecodedClockReading,
) -> Vec<krabka_blockstore::LabelPair> {
    projected_labels(reading, CLOCK_READING_METRIC, &[])
}

/// Metric name of the clock reading series itself.
///
/// The columnar clock block is the source of truth for a reading. This series
/// names it so the block rows fingerprint, index, and shard exactly as every
/// other series does.
pub const CLOCK_READING_METRIC: &str = "krabka_clock_reading";

/// The label set for one projected series: the clock identity, the metric
/// name, and any state label the family adds.
pub(crate) fn projected_labels(
    reading: &DecodedClockReading,
    name: &str,
    extra: &[krabka_blockstore::LabelPair],
) -> Vec<krabka_blockstore::LabelPair> {
    let mut labels = vec![
        krabka_blockstore::LabelPair::new("__name__", name),
        krabka_blockstore::LabelPair::new("node", reading.node.clone()),
        krabka_blockstore::LabelPair::new("clock", reading.clock.clone()),
        krabka_blockstore::LabelPair::new("source", reading.source_kind.as_label()),
    ];
    labels.extend(extra.iter().cloned());
    labels
}

/// Widens a signed count for a projected sample value.
fn widen(value: i64) -> f64 {
    num_traits::ToPrimitive::to_f64(&value).unwrap_or(f64::MAX)
}
