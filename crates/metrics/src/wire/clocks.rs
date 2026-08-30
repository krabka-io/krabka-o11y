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
//! value above 2^53 ns. The conversion to a [`krabka_units::Time`] happens at
//! the projection edge, where the target is a `f64` second count anyway.

use krabka_units::prelude::*;
use prost::Message;
use serde::{Deserialize, Serialize};

use super::{WireError, pb, snappy_block_decode};
use crate::otlp::MAX_SAMPLE_TIMESTAMP_MS;

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

mod clock_source_kind;
mod clock_sync_state;
mod clock_wire_error;
mod decode_clock_readings;
mod decode_reading;
mod decoded_clock_reading;
mod gnss_fix;
mod gnss_reading;
mod i64;
mod is_too_far_in_the_future;
mod nanos_per_milli;
mod ntp_reading;
mod ptp_reading;
mod source_kind;
mod sync_state;
mod timex_reading;
mod unix_nanos;

pub use clock_source_kind::ClockSourceKind;
pub use clock_sync_state::ClockSyncState;
pub use clock_wire_error::ClockWireError;
pub use decode_clock_readings::decode_clock_readings;
use decode_reading::decode_reading;
pub use decoded_clock_reading::DecodedClockReading;
pub use gnss_fix::GnssFix;
use gnss_fix::gnss_fix;
pub use gnss_reading::GnssReading;
use is_too_far_in_the_future::is_too_far_in_the_future;
use nanos_per_milli::NANOS_PER_MILLI;
pub use ntp_reading::NtpReading;
pub use ptp_reading::PtpReading;
use source_kind::source_kind;
use sync_state::sync_state;
pub use timex_reading::TimexReading;
pub use unix_nanos::UnixNanos;
