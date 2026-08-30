//! Jaeger API v2 gRPC decoding.

use prost_types::{Duration, Timestamp};

use crate::{
    span::{AttrValue, KeyValue, Span},
    wire::{
        WireError,
        jaeger::{JaegerBatch, JaegerLog, JaegerProcess, JaegerRef, JaegerSpan, spans_from_batch},
    },
};

pub mod api_v2 {
    tonic::include_proto!("jaeger.api_v2");
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use prost_types::{Duration, Timestamp};

    use super::{duration_micros, timestamp_micros, trace_id_parts};

    /// `span_id_part` reads a span id as a big-endian i64 and insists on
    /// exactly eight bytes. The values are chosen so none of the constants a
    /// collapsed body could return -- 0, 1, -1 -- passes for a real answer,
    /// and byte order is pinned by a value that differs when reversed.
    #[test]
    fn a_grpc_span_id_is_eight_big_endian_bytes() {
        let part = super::span_id_part;

        check!(part(&[0, 0, 0, 0, 0, 0, 0, 2]).expect("eight bytes") == 2);
        check!(part(&[1, 0, 0, 0, 0, 0, 0, 0]).expect("eight bytes") == 1 << 56);
        check!(part(&[0, 0, 0, 0, 0, 0, 0, 0]).expect("eight bytes") == 0);
        check!(
            part(&[255; 8]).expect("eight bytes") == -1,
            "the id is signed"
        );

        // Seven or nine bytes is a decode error, not a pad or a truncation.
        check!(part(&[0; 7]).is_err());
        check!(part(&[0; 9]).is_err());
        check!(part(&[]).is_err());
    }

    /// `timestamp_micros` and `duration_micros` are near-twins over different
    /// types, so each is given values the other does not share. Both truncate
    /// sub-microsecond nanos rather than rounding, which is the behaviour a
    /// division swapped for a multiplication would destroy.
    #[test]
    fn timestamps_and_durations_convert_to_whole_microseconds() {
        check!(
            timestamp_micros(None) == 0,
            "an absent timestamp is the epoch"
        );
        check!(duration_micros(None) == 0, "an absent duration is nothing");

        let stamp = |seconds, nanos| Timestamp { seconds, nanos };
        let span = |seconds, nanos| Duration { seconds, nanos };

        // Seconds scale to microseconds and nanos divide down into them.
        check!(timestamp_micros(Some(&stamp(1, 0))) == 1_000_000);
        check!(
            timestamp_micros(Some(&stamp(0, 1_000))) == 1,
            "a thousand nanos is one micro"
        );
        check!(
            timestamp_micros(Some(&stamp(1, 500_000))) == 1_000_500,
            "both parts add"
        );

        // Sub-microsecond nanos truncate rather than round.
        check!(
            timestamp_micros(Some(&stamp(0, 999))) == 0,
            "under a micro is nothing"
        );
        check!(
            timestamp_micros(Some(&stamp(0, 1_999))) == 1,
            "not rounded up to two"
        );

        // The duration twin carries its own values, so a call routed to the
        // wrong one returns a recognisably different number.
        check!(duration_micros(Some(&span(2, 0))) == 2_000_000);
        check!(duration_micros(Some(&span(0, 3_000))) == 3);
        check!(duration_micros(Some(&span(7, 250_000))) == 7_000_250);

        // Negative durations are a difference, not an error.
        check!(duration_micros(Some(&span(-1, 0))) == -1_000_000);
    }

    /// `trace_id_parts` splits sixteen bytes into two signed halves, high
    /// first. The two halves carry different values so a swap is visible, and
    /// the high half is given a top bit so the sign is exercised.
    #[test]
    fn a_trace_id_splits_into_a_high_and_low_half() {
        let mut id = [0_u8; 16];
        id[7] = 1; // low byte of the high half
        id[15] = 2; // low byte of the low half
        let (high, low) = trace_id_parts(&id).expect("sixteen bytes");
        check!(high == 1, "the first eight bytes");
        check!(low == 2, "and the second eight, not the same eight twice");

        // Big-endian: the first byte is the most significant.
        let mut id = [0_u8; 16];
        id[0] = 1;
        let (high, low) = trace_id_parts(&id).expect("sixteen bytes");
        check!(high == 1 << 56, "byte zero is the top of the high half");
        check!(low == 0);

        // The top bit makes the half negative, which is what signing means.
        let mut id = [0_u8; 16];
        id[0] = 0xff;
        let (high, _) = trace_id_parts(&id).expect("sixteen bytes");
        check!(high < 0, "the high half is signed");

        // Any length but sixteen is refused, on both sides.
        check!(trace_id_parts(&[0; 15]).is_err(), "one short");
        check!(trace_id_parts(&[0; 17]).is_err(), "one long");
        check!(trace_id_parts(&[]).is_err());
    }
}

// === split-modules: generated submodules ===
mod decode_jaeger_grpc_batch;
mod duration_micros;
mod key_value_from_proto;
mod log_from_proto;
mod process_from_proto;
mod ref_from_proto;
mod span_from_proto;
mod span_id_part;
mod timestamp_micros;
mod trace_id_parts;

pub use decode_jaeger_grpc_batch::decode_jaeger_grpc_batch;
use duration_micros::duration_micros;
use key_value_from_proto::key_value_from_proto;
use log_from_proto::log_from_proto;
use process_from_proto::process_from_proto;
use ref_from_proto::ref_from_proto;
use span_from_proto::span_from_proto;
use span_id_part::span_id_part;
use timestamp_micros::timestamp_micros;
use trace_id_parts::trace_id_parts;
