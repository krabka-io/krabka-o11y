use super::{ClockSourceKind, ClockWireError, DecodedClockReading, GnssReading, NtpReading, PtpReading, TimexReading, UnixNanos, gnss_fix, is_too_far_in_the_future, pb, source_kind, sync_state};

pub(crate) fn decode_reading(
    index: usize,
    reading: pb::clocks::ClockReading,
) -> Result<DecodedClockReading, ClockWireError> {
    if reading.node.is_empty() {
        return Err(ClockWireError::EmptyIdentity {
            index,
            field: "node",
        });
    }
    if reading.clock.is_empty() {
        return Err(ClockWireError::EmptyIdentity {
            index,
            field: "clock",
        });
    }
    if reading.uncertainty_nanos < 0 {
        return Err(ClockWireError::NegativeUncertainty {
            index,
            uncertainty_nanos: reading.uncertainty_nanos,
        });
    }
    let reading_unix_nanos = UnixNanos::new(reading.reading_unix_nanos);
    if is_too_far_in_the_future(reading_unix_nanos) {
        return Err(ClockWireError::ReadingTooFarInFuture {
            index,
            reading_unix_nanos: reading.reading_unix_nanos,
        });
    }

    let source_kind = source_kind(index, reading.source_kind)?;
    let sync_state = sync_state(index, reading.sync_state)?;
    let gnss_fix = gnss_fix(index, reading.gnss_fix)?;

    Ok(DecodedClockReading {
        node: reading.node,
        clock: reading.clock,
        source_kind,
        reading_unix_nanos,
        uncertainty_nanos: reading.uncertainty_nanos,
        offset_nanos: reading.offset_nanos,
        sync_state,
        reference_id: (!reading.reference_id.is_empty()).then_some(reading.reference_id),
        last_sync_unix_nanos: (reading.last_sync_unix_nanos != 0)
            .then(|| UnixNanos::new(reading.last_sync_unix_nanos)),
        frequency_ppb: (reading.frequency_ppb != 0).then_some(reading.frequency_ppb),
        last_step_nanos: (reading.last_step_nanos != 0).then_some(reading.last_step_nanos),
        ntp: matches!(source_kind, ClockSourceKind::Ntp).then_some(NtpReading {
            root_delay_nanos: reading.root_delay_nanos,
            root_dispersion_nanos: reading.root_dispersion_nanos,
            stratum: reading.stratum,
        }),
        ptp: matches!(source_kind, ClockSourceKind::Ptp | ClockSourceKind::Phc).then_some(
            PtpReading {
                mean_path_delay_nanos: reading.mean_path_delay_nanos,
                steps_removed: reading.steps_removed,
                gm_clock_class: reading.gm_clock_class,
                gm_clock_accuracy: reading.gm_clock_accuracy,
            },
        ),
        timex: matches!(source_kind, ClockSourceKind::KernelTimex).then_some(TimexReading {
            max_error_nanos: reading.max_error_nanos,
            est_error_nanos: reading.est_error_nanos,
            unsynchronized: reading.unsynchronized,
        }),
        gnss: matches!(source_kind, ClockSourceKind::Gnss).then_some(GnssReading {
            satellites_used: reading.satellites_used,
            fix: gnss_fix,
        }),
    })
}
