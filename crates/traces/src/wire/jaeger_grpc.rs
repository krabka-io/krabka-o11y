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

///
/// # Errors
/// Returns an error when the query is malformed, an expression has incompatible operand types, or the backing span store fails.
pub fn decode_jaeger_grpc_batch(batch: api_v2::Batch) -> Result<Vec<Span>, WireError> {
    let batch_process = batch
        .process
        .as_ref()
        .map(process_from_proto)
        .unwrap_or_default();
    let mut spans = Vec::with_capacity(batch.spans.len());
    for span in batch.spans {
        let process = span
            .process
            .as_ref()
            .map_or_else(|| batch_process.clone(), process_from_proto);
        spans.extend(spans_from_batch(&JaegerBatch {
            process,
            spans: vec![span_from_proto(span)?],
        }));
    }
    Ok(spans)
}

fn process_from_proto(process: &api_v2::Process) -> JaegerProcess {
    JaegerProcess {
        service_name: process.service_name.clone(),
        tags: process.tags.iter().map(key_value_from_proto).collect(),
    }
}

fn span_from_proto(span: api_v2::Span) -> Result<JaegerSpan, WireError> {
    let (trace_id_high, trace_id_low) = trace_id_parts(&span.trace_id)?;
    let span_id = span_id_part(&span.span_id)?;
    Ok(JaegerSpan {
        trace_id_low,
        trace_id_high,
        span_id,
        parent_span_id: 0,
        operation_name: span.operation_name,
        references: span
            .references
            .iter()
            .map(ref_from_proto)
            .collect::<Result<Vec<_>, _>>()?,
        start_time_micros: timestamp_micros(span.start_time.as_ref()),
        duration_micros: duration_micros(span.duration.as_ref()),
        tags: span.tags.iter().map(key_value_from_proto).collect(),
        logs: span.logs.iter().map(log_from_proto).collect(),
    })
}

fn ref_from_proto(reference: &api_v2::SpanRef) -> Result<JaegerRef, WireError> {
    let (trace_id_high, trace_id_low) = trace_id_parts(&reference.trace_id)?;
    Ok(JaegerRef {
        ref_type: reference.ref_type,
        trace_id_low,
        trace_id_high,
        span_id: span_id_part(&reference.span_id)?,
    })
}

fn log_from_proto(log: &api_v2::Log) -> JaegerLog {
    JaegerLog {
        timestamp_micros: timestamp_micros(log.timestamp.as_ref()),
        fields: log.fields.iter().map(key_value_from_proto).collect(),
    }
}

fn key_value_from_proto(kv: &api_v2::KeyValue) -> KeyValue {
    let value_type = api_v2::ValueType::try_from(kv.v_type).unwrap_or(api_v2::ValueType::String);
    let value = match value_type {
        api_v2::ValueType::String => AttrValue::Str(kv.v_str.clone()),
        api_v2::ValueType::Bool => AttrValue::Bool(kv.v_bool),
        api_v2::ValueType::Int64 => AttrValue::Int(kv.v_int64),
        api_v2::ValueType::Float64 => AttrValue::Double(kv.v_float64),
        api_v2::ValueType::Binary => AttrValue::Bytes(kv.v_binary.clone()),
    };
    KeyValue {
        key: kv.key.clone(),
        value,
    }
}

fn trace_id_parts(bytes: &[u8]) -> Result<(i64, i64), WireError> {
    if bytes.len() != 16 {
        return Err(WireError::Decode("jaeger trace_id must be 16 bytes".into()));
    }
    let high = i64::from_be_bytes(bytes[0..8].try_into().expect("slice length checked"));
    let low = i64::from_be_bytes(bytes[8..16].try_into().expect("slice length checked"));
    Ok((high, low))
}

fn span_id_part(bytes: &[u8]) -> Result<i64, WireError> {
    if bytes.len() != 8 {
        return Err(WireError::Decode("jaeger span_id must be 8 bytes".into()));
    }
    Ok(i64::from_be_bytes(
        bytes[0..8].try_into().expect("slice length checked"),
    ))
}

fn timestamp_micros(timestamp: Option<&Timestamp>) -> i64 {
    timestamp.map_or(0, |ts| {
        ts.seconds
            .saturating_mul(1_000_000)
            .saturating_add(i64::from(ts.nanos) / 1_000)
    })
}

fn duration_micros(duration: Option<&Duration>) -> i64 {
    duration.map_or(0, |duration| {
        duration
            .seconds
            .saturating_mul(1_000_000)
            .saturating_add(i64::from(duration.nanos) / 1_000)
    })
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
        check!(part(&[255; 8]).expect("eight bytes") == -1, "the id is signed");

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
        check!(timestamp_micros(None) == 0, "an absent timestamp is the epoch");
        check!(duration_micros(None) == 0, "an absent duration is nothing");

        let stamp = |seconds, nanos| Timestamp { seconds, nanos };
        let span = |seconds, nanos| Duration { seconds, nanos };

        // Seconds scale to microseconds and nanos divide down into them.
        check!(timestamp_micros(Some(&stamp(1, 0))) == 1_000_000);
        check!(timestamp_micros(Some(&stamp(0, 1_000))) == 1, "a thousand nanos is one micro");
        check!(timestamp_micros(Some(&stamp(1, 500_000))) == 1_000_500, "both parts add");

        // Sub-microsecond nanos truncate rather than round.
        check!(timestamp_micros(Some(&stamp(0, 999))) == 0, "under a micro is nothing");
        check!(timestamp_micros(Some(&stamp(0, 1_999))) == 1, "not rounded up to two");

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
