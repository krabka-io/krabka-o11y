//! Property tests for the clock reading decoder.
//!
//! The decoder sits at an unauthenticated ingest edge, so the properties that
//! matter are that it never panics and that it never sizes an allocation from a
//! number an attacker chose.

use krabka_metrics::wire::{ClockSourceKind, GnssFix, decode_clock_readings, pb};
use krabka_units::prelude::*;
use proptest::prelude::*;
use prost::Message as _;

/// The bound the OTLP sample path applies, as nanoseconds. A generated reading
/// stays under it so a well-formed batch is never rejected for its timestamp.
const MAX_READING_NANOS: i64 = 7_258_118_400_000 * 1_000_000;

fn snappy_batch(readings: Vec<pb::clocks::ClockReading>) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(&pb::clocks::ClockReadingBatch { readings }.encode_to_vec())
        .expect("snappy compress")
}

/// A reading that passes every validation rule.
fn well_formed_reading() -> impl Strategy<Value = pb::clocks::ClockReading> {
    (
        "[a-z][a-z0-9-]{0,20}",
        "[A-Za-z0-9/_]{1,20}",
        1i32..=5,
        i64::MIN..=MAX_READING_NANOS,
        0i64..=i64::MAX,
        any::<i64>(),
        1i32..=5,
        "[a-z0-9.:]{0,16}",
        any::<i64>(),
        any::<u32>(),
        0i32..=3,
    )
        .prop_map(
            |(
                node,
                clock,
                source_kind,
                reading_unix_nanos,
                uncertainty_nanos,
                offset_nanos,
                sync_state,
                reference_id,
                magnitude,
                count,
                gnss_fix,
            )| pb::clocks::ClockReading {
                node,
                clock,
                source_kind,
                reading_unix_nanos,
                uncertainty_nanos,
                offset_nanos,
                sync_state,
                reference_id,
                last_sync_unix_nanos: magnitude,
                frequency_ppb: magnitude,
                last_step_nanos: magnitude,
                root_delay_nanos: magnitude,
                root_dispersion_nanos: magnitude,
                stratum: count,
                mean_path_delay_nanos: magnitude,
                steps_removed: count,
                gm_clock_class: count,
                gm_clock_accuracy: count,
                max_error_nanos: magnitude,
                est_error_nanos: magnitude,
                unsynchronized: count % 2 == 0,
                satellites_used: count,
                gnss_fix,
            },
        )
}

/// A reading with no constraints at all, so most of them are rejections.
fn arbitrary_reading() -> impl Strategy<Value = pb::clocks::ClockReading> {
    (
        ".{0,8}",
        ".{0,8}",
        any::<i32>(),
        any::<i64>(),
        any::<i64>(),
        any::<i32>(),
        any::<i32>(),
    )
        .prop_map(
            |(
                node,
                clock,
                source_kind,
                reading_unix_nanos,
                uncertainty_nanos,
                sync_state,
                gnss_fix,
            )| {
                pb::clocks::ClockReading {
                    node,
                    clock,
                    source_kind,
                    reading_unix_nanos,
                    uncertainty_nanos,
                    sync_state,
                    gnss_fix,
                    ..Default::default()
                }
            },
        )
}

proptest! {
    /// A well-formed batch decodes, and every reading keeps its identity and
    /// the source group its kind owns.
    #[test]
    fn well_formed_batches_decode_and_keep_their_identity(
        wire in prop::collection::vec(well_formed_reading(), 0..8),
    ) {
        let decoded = decode_clock_readings(&snappy_batch(wire.clone()), mebibytes(1))
            .expect("a well-formed batch decodes");

        prop_assert_eq!(decoded.len(), wire.len());
        for (reading, source) in decoded.iter().zip(&wire) {
            prop_assert_eq!(&reading.node, &source.node);
            prop_assert_eq!(&reading.clock, &source.clock);
            prop_assert!(reading.uncertainty_nanos >= 0);

            // Exactly the group this source kind owns is filled.
            let filled = [
                reading.ntp.is_some(),
                reading.ptp.is_some(),
                reading.timex.is_some(),
                reading.gnss.is_some(),
            ];
            prop_assert_eq!(filled.iter().filter(|present| **present).count(), 1);
            match reading.source_kind {
                ClockSourceKind::Ntp => prop_assert!(reading.ntp.is_some()),
                ClockSourceKind::Ptp | ClockSourceKind::Phc => prop_assert!(reading.ptp.is_some()),
                ClockSourceKind::KernelTimex => prop_assert!(reading.timex.is_some()),
                ClockSourceKind::Gnss => prop_assert!(reading.gnss.is_some()),
            }

            // Only a GNSS reading can carry a fix quality.
            if let Some(gnss) = reading.gnss {
                prop_assert!(
                    gnss.fix.is_none() || GnssFix::ALL.contains(&gnss.fix.expect("a fix"))
                );
            }
        }
    }

    /// An arbitrary reading is either decoded or rejected. Neither outcome is a
    /// panic, and no rejection carries an unexpected status.
    #[test]
    fn arbitrary_readings_never_panic_the_decoder(
        wire in prop::collection::vec(arbitrary_reading(), 0..8),
    ) {
        match decode_clock_readings(&snappy_batch(wire), mebibytes(1)) {
            Ok(readings) => {
                for reading in readings {
                    prop_assert!(!reading.node.is_empty());
                    prop_assert!(!reading.clock.is_empty());
                    prop_assert!(reading.uncertainty_nanos >= 0);
                }
            }
            Err(error) => prop_assert_eq!(error.status_code(), 400),
        }
    }

    /// Arbitrary bytes are not a snappy frame, or not a protobuf body, or both.
    /// The decoder returns an error for every one of them.
    #[test]
    fn arbitrary_bytes_never_panic_the_decoder(body in prop::collection::vec(any::<u8>(), 0..1024)) {
        let _ = decode_clock_readings(&body, mebibytes(1));
    }

    /// Arbitrary bytes wrapped in a valid snappy frame reach the protobuf
    /// decoder, which is the deeper path.
    #[test]
    fn arbitrary_snappy_framed_bytes_never_panic_the_decoder(
        body in prop::collection::vec(any::<u8>(), 0..1024),
    ) {
        let framed = snap::raw::Encoder::new()
            .compress_vec(&body)
            .expect("snappy compress");

        let _ = decode_clock_readings(&framed, mebibytes(1));
    }

    /// A tiny cap rejects a body rather than pre-allocating for it.
    #[test]
    fn a_tiny_cap_rejects_every_body(
        wire in prop::collection::vec(well_formed_reading(), 1..4),
    ) {
        let error = decode_clock_readings(&snappy_batch(wire), bytes(4))
            .expect_err("the cap rejects the body");

        prop_assert_eq!(error.status_code(), 400);
    }
}
