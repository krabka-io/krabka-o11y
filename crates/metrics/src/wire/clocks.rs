//! Clock confidence ingest wire surface.
//!
//! An agent reads every clock on its host and pushes one snappy-framed
//! [`ClockReadingBatch`](pb::clocks::ClockReadingBatch). This module decodes
//! that batch into validated [`DecodedClockReading`] values and rejects any
//! reading that answers no clock confidence question.
//!
//! # Field presence
//!
//! proto3 scalars carry no presence bit, so a field the agent never set and a
//! field the agent set to zero look the same on the wire. This module derives
//! presence from the reading instead, with two rules:
//!
//! - **A source-specific group belongs to one source kind.** The reading fills
//!   the group its [`ClockSourceKind`] owns and leaves every other group empty.
//!   An NTP host therefore publishes no PTP path delay, and a PTP host
//!   publishes no NTP stratum. This is the rule the block schema already
//!   states: one exporter reads one kind of clock.
//! - **A discipline field is present when it is not zero or empty.** These
//!   fields cross every source kind, so no source rule can decide them. Epoch
//!   zero is not a last-sync instant a real discipline reports, and an empty
//!   string is not a reference identity.
//!
//! # Magnitudes stay integers
//!
//! Every nanosecond field here stays an `i64`. The block columns are exact
//! `Int64` nanosecond counts, and an `f64` quantity cannot represent every
//! value such a column holds, so a round trip through one would mangle a wire
//! value above 2^53 ns. The conversion to a [`crabka_units::Time`] happens at
//! the projection edge, where the target is a `f64` second count anyway.

use crabka_units::prelude::*;
use prost::Message;
use serde::{Deserialize, Serialize};

use super::{WireError, pb, snappy_block_decode};
use crate::otlp::MAX_SAMPLE_TIMESTAMP_MS;

/// Nanoseconds in one millisecond.
const NANOS_PER_MILLI: i64 = 1_000_000;

/// An instant on the Unix epoch timeline, in nanoseconds.
///
/// A clock reading carries three of these: the host's own reading, the last
/// instant the clock held a valid reference, and the stamp the ingester writes
/// on arrival. They share a primitive and a scale, and swapping the host
/// reading with the ingest stamp would invert the very skew this signal
/// measures. The newtype makes that swap a compile error.
///
/// This is a coordinate and not an extent, so it stays an integer and never
/// becomes a [`Time`].
#[derive(
    Clone, Copy, Debug, Default, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize,
)]
#[serde(transparent)]
pub struct UnixNanos(i64);

impl UnixNanos {
    /// The Unix epoch itself.
    pub const EPOCH: Self = Self(0);

    /// Wraps a raw nanosecond count.
    #[must_use]
    pub const fn new(nanos: i64) -> Self {
        Self(nanos)
    }

    /// The instant as a raw nanosecond count.
    #[must_use]
    pub const fn as_i64(self) -> i64 {
        self.0
    }

    /// The instant in epoch milliseconds, which is the unit every metric block
    /// timestamp column uses.
    ///
    /// The division floors, so an instant before the epoch lands on the
    /// millisecond that contains it rather than on the one after it.
    #[must_use]
    pub const fn epoch_millis(self) -> i64 {
        self.0.div_euclid(NANOS_PER_MILLI)
    }

    /// The instant in fractional epoch seconds, for a projected series.
    #[must_use]
    pub fn epoch_secs_f64(self) -> f64 {
        Time::from_nanos(self.0).secs_f64()
    }

    /// The extent from `self` to `later`.
    ///
    /// The subtraction saturates, so a wire value at either end of the `i64`
    /// range yields the widest representable extent rather than wrapping into
    /// a small one of the wrong sign.
    #[must_use]
    pub fn extent_to(self, later: Self) -> Time {
        Time::from_nanos(later.0.saturating_sub(self.0))
    }
}

impl From<i64> for UnixNanos {
    fn from(nanos: i64) -> Self {
        Self(nanos)
    }
}

impl From<UnixNanos> for i64 {
    fn from(instant: UnixNanos) -> Self {
        instant.0
    }
}

/// Where a clock gets its time.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ClockSourceKind {
    /// IEEE 1588 Precision Time Protocol.
    Ptp,
    /// Network Time Protocol.
    Ntp,
    /// A satellite receiver.
    Gnss,
    /// The kernel clock discipline that `adjtimex(2)` reports.
    KernelTimex,
    /// A PTP hardware clock device.
    Phc,
}

impl ClockSourceKind {
    /// Every source kind, in wire order.
    pub const ALL: [Self; 5] = [
        Self::Ptp,
        Self::Ntp,
        Self::Gnss,
        Self::KernelTimex,
        Self::Phc,
    ];

    /// The label and dictionary value for this source kind.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Ptp => "ptp",
            Self::Ntp => "ntp",
            Self::Gnss => "gnss",
            Self::KernelTimex => "kernel_timex",
            Self::Phc => "phc",
        }
    }
}

/// What a clock discipline does now.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum ClockSyncState {
    /// The clock tracks a valid reference.
    Synchronized,
    /// The reference is gone. The clock runs on the rate it learned before.
    Holdover,
    /// The clock never had a reference.
    FreeRunning,
    /// The clock lost its reference and holds no rate estimate.
    Unsynchronized,
    /// The discipline stepped the clock. Time is not continuous across this
    /// reading.
    Stepped,
}

impl ClockSyncState {
    /// Every discipline state, in wire order.
    ///
    /// The projection walks this list on every reading, so a state that stops
    /// being current gets an explicit zero rather than a stale one.
    pub const ALL: [Self; 5] = [
        Self::Synchronized,
        Self::Holdover,
        Self::FreeRunning,
        Self::Unsynchronized,
        Self::Stepped,
    ];

    /// The label and dictionary value for this state.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::Synchronized => "synchronized",
            Self::Holdover => "holdover",
            Self::FreeRunning => "free_running",
            Self::Unsynchronized => "unsynchronized",
            Self::Stepped => "stepped",
        }
    }
}

/// The quality of a GNSS position solution.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum GnssFix {
    /// The receiver has no fix.
    None,
    /// The receiver has a two-dimensional fix.
    TwoD,
    /// The receiver has a three-dimensional fix.
    ThreeD,
}

impl GnssFix {
    /// Every fix quality, in wire order.
    pub const ALL: [Self; 3] = [Self::None, Self::TwoD, Self::ThreeD];

    /// The label and dictionary value for this fix quality.
    #[must_use]
    pub const fn as_label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::TwoD => "2d",
            Self::ThreeD => "3d",
        }
    }
}

/// The NTP measurements from RFC 5905.
///
/// RFC 5905 names the sum of half the root delay and the root dispersion the
/// synchronization distance. That sum is the real NTP uncertainty bound, and
/// neither term alone is, so both terms travel together.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct NtpReading {
    /// The round-trip delay to the stratum 1 root.
    pub root_delay_nanos: i64,
    /// The accumulated dispersion to the stratum 1 root.
    pub root_dispersion_nanos: i64,
    /// The distance in NTP hops from a reference clock.
    pub stratum: u32,
}

/// The PTP measurements from `pmc GET TIME_STATUS_NP`, `CURRENT_DATA_SET`, and
/// `PARENT_DATA_SET`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct PtpReading {
    /// The one-way path delay the port measures to the master.
    pub mean_path_delay_nanos: i64,
    /// The count of boundary clocks between this port and the grandmaster.
    pub steps_removed: u32,
    /// The grandmaster `clockClass`, which states how traceable its time is.
    pub gm_clock_class: u32,
    /// The grandmaster `clockAccuracy`, which states the expected error of its
    /// time.
    pub gm_clock_accuracy: u32,
}

/// The kernel clock discipline measurements from `adjtimex(2)`.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TimexReading {
    /// The kernel `maxerror`, which the kernel grows at 500 ppm between
    /// updates, so it is already an uncertainty bound.
    pub max_error_nanos: i64,
    /// The kernel `esterror`, which is the discipline's own error estimate.
    pub est_error_nanos: i64,
    /// The kernel `STA_UNSYNC` bit.
    pub unsynchronized: bool,
}

/// The GNSS receiver measurements.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct GnssReading {
    /// The count of satellites in the position solution.
    pub satellites_used: u32,
    /// The quality of the position solution. A receiver that reports no fix
    /// quality leaves this empty.
    pub fix: Option<GnssFix>,
}

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

/// Errors raised while decoding a clock reading batch.
#[derive(Debug, thiserror::Error)]
pub enum ClockWireError {
    /// The snappy frame or the protobuf body did not decode.
    #[error(transparent)]
    Wire(#[from] WireError),

    /// A reading named no host or no clock, so nothing identifies the series.
    #[error("clock reading {index} has an empty `{field}`")]
    EmptyIdentity { index: usize, field: &'static str },

    /// An uncertainty is a half-width and can never be negative.
    #[error("clock reading {index} has a negative uncertainty of {uncertainty_nanos}ns")]
    NegativeUncertainty {
        index: usize,
        uncertainty_nanos: i64,
    },

    /// A reading so far in the future that it would poison the per-series
    /// out-of-order window downstream.
    #[error("clock reading {index} at {reading_unix_nanos}ns is too far in the future")]
    ReadingTooFarInFuture {
        index: usize,
        reading_unix_nanos: i64,
    },

    /// The agent left a required enum at its `*_UNSPECIFIED` zero value.
    #[error("clock reading {index} leaves `{field}` unspecified")]
    UnspecifiedEnum { index: usize, field: &'static str },

    /// The agent sent a discriminant this build does not know.
    #[error("clock reading {index} has an unknown `{field}` discriminant {value}")]
    UnknownEnum {
        index: usize,
        field: &'static str,
        value: i32,
    },
}

impl ClockWireError {
    /// HTTP status code for the clock ingest endpoint.
    #[must_use]
    pub fn status_code(&self) -> u16 {
        match self {
            Self::Wire(error) => error.status_code(),
            Self::EmptyIdentity { .. }
            | Self::NegativeUncertainty { .. }
            | Self::ReadingTooFarInFuture { .. }
            | Self::UnspecifiedEnum { .. }
            | Self::UnknownEnum { .. } => 400,
        }
    }
}

/// Decodes a snappy-framed [`ClockReadingBatch`](pb::clocks::ClockReadingBatch)
/// into validated readings.
///
/// `max_decompressed` is the same cap the `remote_write` push path applies, so
/// one setting bounds every ingest body this process decompresses.
///
/// # Errors
///
/// Returns an error when the snappy frame declares or produces more than
/// `max_decompressed`, when the protobuf body is malformed, or when any
/// reading fails validation. This function never panics on wire input.
pub fn decode_clock_readings(
    body: &[u8],
    max_decompressed: ByteSize,
) -> Result<Vec<DecodedClockReading>, ClockWireError> {
    let raw = snappy_block_decode(body, max_decompressed)?;
    let batch = pb::clocks::ClockReadingBatch::decode(raw.as_slice())
        .map_err(|error| WireError::ProtobufDecode(error.to_string()))?;

    batch
        .readings
        .into_iter()
        .enumerate()
        .map(|(index, reading)| decode_reading(index, reading))
        .collect()
}

fn decode_reading(
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

/// Whether a reading sits beyond the sane future bound the OTLP path already
/// applies to a sample timestamp.
///
/// A clamp of such a value would poison the per-series out-of-order and too-old
/// window downstream, so the caller drops the request instead. The comparison
/// widens to `i128`, which holds both a negative millisecond coordinate and the
/// whole `u64` bound without a lossy conversion.
fn is_too_far_in_the_future(reading: UnixNanos) -> bool {
    i128::from(reading.epoch_millis()) > i128::from(MAX_SAMPLE_TIMESTAMP_MS)
}

fn source_kind(index: usize, value: i32) -> Result<ClockSourceKind, ClockWireError> {
    match pb::clocks::SourceKind::try_from(value) {
        Ok(pb::clocks::SourceKind::Ptp) => Ok(ClockSourceKind::Ptp),
        Ok(pb::clocks::SourceKind::Ntp) => Ok(ClockSourceKind::Ntp),
        Ok(pb::clocks::SourceKind::Gnss) => Ok(ClockSourceKind::Gnss),
        Ok(pb::clocks::SourceKind::KernelTimex) => Ok(ClockSourceKind::KernelTimex),
        Ok(pb::clocks::SourceKind::Phc) => Ok(ClockSourceKind::Phc),
        Ok(pb::clocks::SourceKind::Unspecified) => Err(ClockWireError::UnspecifiedEnum {
            index,
            field: "source_kind",
        }),
        Err(_) => Err(ClockWireError::UnknownEnum {
            index,
            field: "source_kind",
            value,
        }),
    }
}

fn sync_state(index: usize, value: i32) -> Result<ClockSyncState, ClockWireError> {
    match pb::clocks::SyncState::try_from(value) {
        Ok(pb::clocks::SyncState::Synchronized) => Ok(ClockSyncState::Synchronized),
        Ok(pb::clocks::SyncState::Holdover) => Ok(ClockSyncState::Holdover),
        Ok(pb::clocks::SyncState::FreeRunning) => Ok(ClockSyncState::FreeRunning),
        Ok(pb::clocks::SyncState::Unsynchronized) => Ok(ClockSyncState::Unsynchronized),
        Ok(pb::clocks::SyncState::Stepped) => Ok(ClockSyncState::Stepped),
        Ok(pb::clocks::SyncState::Unspecified) => Err(ClockWireError::UnspecifiedEnum {
            index,
            field: "sync_state",
        }),
        Err(_) => Err(ClockWireError::UnknownEnum {
            index,
            field: "sync_state",
            value,
        }),
    }
}

/// Reads the GNSS fix quality, where the unspecified value means the receiver
/// reported none.
///
/// This is the one enum whose zero value is not a rejection. The fix quality is
/// a source-specific, nullable column, and every reading from a source other
/// than GNSS leaves it at zero by construction. `GNSS_FIX_NONE` already says
/// "the receiver has no fix", so the zero value stays free to mean "the agent
/// reported no fix quality".
fn gnss_fix(index: usize, value: i32) -> Result<Option<GnssFix>, ClockWireError> {
    match pb::clocks::GnssFix::try_from(value) {
        Ok(pb::clocks::GnssFix::None) => Ok(Some(GnssFix::None)),
        Ok(pb::clocks::GnssFix::TwoD) => Ok(Some(GnssFix::TwoD)),
        Ok(pb::clocks::GnssFix::ThreeD) => Ok(Some(GnssFix::ThreeD)),
        Ok(pb::clocks::GnssFix::Unspecified) => Ok(None),
        Err(_) => Err(ClockWireError::UnknownEnum {
            index,
            field: "gnss_fix",
            value,
        }),
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};

    use super::*;

    #[test]
    fn epoch_millis_floors_before_the_epoch() {
        check!(UnixNanos::new(1_500_000).epoch_millis() == 1);
        check!(UnixNanos::new(-1_500_000).epoch_millis() == -2);
        check!(UnixNanos::EPOCH.epoch_millis() == 0);
    }

    #[test]
    fn extent_saturates_rather_than_wrapping() {
        let extent = UnixNanos::new(i64::MIN).extent_to(UnixNanos::new(i64::MAX));

        assert!(extent > Time::ZERO);
    }

    #[test]
    fn far_future_bound_matches_the_otlp_sample_bound() {
        let bound_millis = i64::try_from(MAX_SAMPLE_TIMESTAMP_MS).expect("bound fits i64");

        check!(!is_too_far_in_the_future(UnixNanos::new(
            bound_millis * NANOS_PER_MILLI
        )));
        check!(is_too_far_in_the_future(UnixNanos::new(
            (bound_millis + 1) * NANOS_PER_MILLI
        )));
        check!(is_too_far_in_the_future(UnixNanos::new(i64::MAX)));
        check!(!is_too_far_in_the_future(UnixNanos::new(i64::MIN)));
    }
}
