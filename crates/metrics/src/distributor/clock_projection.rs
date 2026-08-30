use super::{ClockReadingPayload, DecodedClockReading, DecodedSample, DecodedSeries, Time, TimeExt, UnixNanos, clock_state_series, decoded_series, projected_labels, widen};

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
