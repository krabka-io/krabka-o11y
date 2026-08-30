use super::{Deserialize, Serialize, ClockSourceKind, UnixNanos, i64, ClockSyncState, NtpReading, PtpReading, TimexReading, GnssReading, Time, TimeExt};

/// One validated clock reading: one clock, on one host, at one moment.
///
/// The ingester adds its own receive stamp later, so this type holds only what
/// the host reported.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct DecodedClockReading {
    /// The host that owns the clock.
    pub node: String,
    /// The clock on that host, such as `CLOCK_REALTIME` or `/dev/ptp0`.
    pub clock: String,
    /// Where the clock gets its time.
    pub source_kind: ClockSourceKind,
    /// The host's own reading of the clock.
    pub reading_unix_nanos: UnixNanos,
    /// The half-width of the interval around the reading. True time is in
    /// `reading_unix_nanos` plus or minus this value, so it is never negative.
    pub uncertainty_nanos: i64,
    /// The signed offset of this clock from its reference. A positive value
    /// means the clock is ahead of the reference.
    pub offset_nanos: i64,
    /// What the clock discipline does now.
    pub sync_state: ClockSyncState,
    /// The reference this clock follows.
    pub reference_id: Option<String>,
    /// When this clock last held a valid reference.
    pub last_sync_unix_nanos: Option<UnixNanos>,
    /// The frequency correction the discipline applies, in parts per billion.
    pub frequency_ppb: Option<i64>,
    /// The magnitude of the most recent step the discipline applied.
    pub last_step_nanos: Option<i64>,
    /// The NTP measurements, on an NTP reading only.
    pub ntp: Option<NtpReading>,
    /// The PTP measurements, on a PTP or PHC reading only.
    pub ptp: Option<PtpReading>,
    /// The kernel timex measurements, on a kernel timex reading only.
    pub timex: Option<TimexReading>,
    /// The GNSS measurements, on a GNSS reading only.
    pub gnss: Option<GnssReading>,
}

impl DecodedClockReading {
    /// The block timestamp for this reading, in epoch milliseconds.
    ///
    /// Every metric block in this crate stamps its rows in milliseconds. The
    /// full nanosecond reading stays in its own column, so this conversion
    /// drops no precision from the block.
    #[must_use]
    pub const fn timestamp_ms(&self) -> i64 {
        self.reading_unix_nanos.epoch_millis()
    }

    /// The half-width of the interval around the reading, as an extent.
    #[must_use]
    pub fn uncertainty(&self) -> Time {
        Time::from_nanos(self.uncertainty_nanos)
    }
}
