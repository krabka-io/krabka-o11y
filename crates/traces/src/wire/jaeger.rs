//! Jaeger push-door decoding.

use crate::{
    ids::{TraceIdHigh, TraceIdLow},
    span::{AttrValue, KeyValue, LinkRecord, Span, SpanKind, StatusCode},
    wire::WireError,
};

#[cfg(test)]
pub(crate) mod test_support {
    use crate::ids::{TraceIdHigh, TraceIdLow};

    pub fn encode_sample_batch() -> Vec<u8> {
        let mut out = Vec::new();
        write_process(&mut out, 1, "checkout");
        write_span_list(&mut out, 2);
        out.push(0);
        out
    }

    fn write_process(out: &mut Vec<u8>, field_id: i16, service: &str) {
        write_field_header(out, 12, field_id, &mut 0);
        let mut last = 0;
        write_string_field(out, 1, service, &mut last);
        write_field_header(out, 9, 2, &mut last);
        write_list_header(out, 12, 1);
        write_key_value_string(out, "process.tag", "present");
        out.push(0);
    }

    fn write_span_list(out: &mut Vec<u8>, field_id: i16) {
        write_field_header(out, 9, field_id, &mut 1);
        write_list_header(out, 12, 1);
        let mut last = 0;
        write_i64_field(out, 1, 2, &mut last);
        write_i64_field(out, 2, 1, &mut last);
        write_i64_field(out, 3, 3, &mut last);
        write_i64_field(out, 4, 0, &mut last);
        write_string_field(out, 5, "GET /", &mut last);
        write_field_header(out, 9, 6, &mut last);
        write_list_header(out, 12, 2);
        write_span_ref(out, 0, TraceIdLow(2), TraceIdHigh(1), 4);
        write_span_ref(out, 1, TraceIdLow(5), TraceIdHigh(6), 7);
        write_i32_field(out, 7, 0, &mut last);
        write_i64_field(out, 8, 1_000, &mut last);
        write_i64_field(out, 9, 25, &mut last);
        write_field_header(out, 9, 10, &mut last);
        write_list_header(out, 12, 3);
        write_key_value_string(out, "span.kind", "server");
        write_key_value_string(out, "http.method", "GET");
        write_key_value_bool(out, "error", true);
        write_field_header(out, 9, 11, &mut last);
        write_list_header(out, 12, 1);
        write_log(out);
        out.push(0);
    }

    fn write_span_ref(
        out: &mut Vec<u8>,
        ref_type: i32,
        low: TraceIdLow,
        high: TraceIdHigh,
        span_id: i64,
    ) {
        let mut last = 0;
        write_i32_field(out, 1, ref_type, &mut last);
        write_i64_field(out, 2, low.0, &mut last);
        write_i64_field(out, 3, high.0, &mut last);
        write_i64_field(out, 4, span_id, &mut last);
        out.push(0);
    }

    fn write_log(out: &mut Vec<u8>) {
        let mut last = 0;
        write_i64_field(out, 1, 1_005, &mut last);
        write_field_header(out, 9, 2, &mut last);
        write_list_header(out, 12, 2);
        write_key_value_string(out, "event", "cache.miss");
        write_key_value_string(out, "cache.key", "users");
        out.push(0);
    }

    fn write_key_value_string(out: &mut Vec<u8>, key: &str, value: &str) {
        let mut last = 0;
        write_string_field(out, 1, key, &mut last);
        write_i32_field(out, 2, 0, &mut last);
        write_string_field(out, 3, value, &mut last);
        out.push(0);
    }

    fn write_key_value_bool(out: &mut Vec<u8>, key: &str, value: bool) {
        let mut last = 0;
        write_string_field(out, 1, key, &mut last);
        write_i32_field(out, 2, 3, &mut last);
        write_bool_field(out, 5, value, &mut last);
        out.push(0);
    }

    fn write_i32_field(out: &mut Vec<u8>, id: i16, value: i32, last: &mut i16) {
        write_field_header(out, 5, id, last);
        write_varint(out, zigzag_i32(value));
    }

    pub fn write_i64_field(out: &mut Vec<u8>, id: i16, value: i64, last: &mut i16) {
        write_field_header(out, 6, id, last);
        write_varint(out, zigzag_i64(value));
    }

    pub fn write_string_field(out: &mut Vec<u8>, id: i16, value: &str, last: &mut i16) {
        write_field_header(out, 8, id, last);
        write_varint(out, u64::try_from(value.len()).unwrap());
        out.extend_from_slice(value.as_bytes());
    }

    fn write_bool_field(out: &mut Vec<u8>, id: i16, value: bool, last: &mut i16) {
        write_field_header(out, if value { 1 } else { 2 }, id, last);
    }

    pub fn write_field_header(out: &mut Vec<u8>, type_id: u8, id: i16, last: &mut i16) {
        let delta = id - *last;
        if (1..=15).contains(&delta) {
            out.push((u8::try_from(delta).unwrap() << 4) | type_id);
        } else {
            out.push(type_id);
            write_varint(out, zigzag_i32(i32::from(id)));
        }
        *last = id;
    }

    fn write_list_header(out: &mut Vec<u8>, element_type: u8, size: usize) {
        if size < 15 {
            out.push((u8::try_from(size).unwrap() << 4) | element_type);
        } else {
            out.push(0xF0 | element_type);
            write_varint(out, u64::try_from(size).unwrap());
        }
    }

    fn write_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
            value >>= 7;
        }
        out.push(u8::try_from(value).unwrap());
    }

    fn zigzag_i32(value: i32) -> u64 {
        u64::from(((value << 1) ^ (value >> 31)).cast_unsigned())
    }

    fn zigzag_i64(value: i64) -> u64 {
        ((value << 1) ^ (value >> 63)).cast_unsigned()
    }
}

#[cfg(test)]
mod tests {

    /// A varint's payload is the low seven bits of each byte, with the top bit
    /// marking continuation. Every other decode in this file drives only
    /// single-byte varints, where masking the continuation bit off is a no-op
    /// -- a value spanning two bytes is the only thing that tells the mask
    /// apart from keeping the whole byte.
    #[test]
    fn a_multi_byte_varint_drops_each_continuation_bit() {
        let read = |bytes: &[u8]| {
            let mut input = super::CompactInput { bytes, pos: 0 };
            input.read_varint()
        };

        check!(read(&[0x05]).expect("a one-byte varint") == 5);
        // 300 is 0b1_0010_1100: seven low bits (0x2C) with the continuation
        // bit set, then 0x02. Keeping the continuation bit would read 428.
        check!(read(&[0xAC, 0x02]).expect("a two-byte varint") == 300);

        let overlong = read(&[0x80; 10]);
        check!(
            matches!(
                &overlong,
                Err(crate::wire::WireError::Decode(message))
                    if message.contains("varint too long")
            ),
            "a varint past 64 bits is refused by the length guard"
        );
    }

    /// `read_binary_ref` dispatches on field id and wire type. Three of its
    /// four fields are i64, so a field reading its neighbour's id still yields
    /// a well-formed reference -- every value here differs, and the two halves
    /// of the trace id differ from each other as well as from the span id.
    #[test]
    fn a_binary_span_reference_takes_each_field_from_its_own_id() {
        /// Binary Thrift: a type byte, a two-byte field id, then the payload.
        fn field(out: &mut Vec<u8>, type_id: u8, id: i16, payload: &[u8]) {
            out.push(type_id);
            out.extend_from_slice(&id.to_be_bytes());
            out.extend_from_slice(payload);
        }

        let mut bytes = Vec::new();
        field(&mut bytes, 8, 1, &7_i32.to_be_bytes());
        field(&mut bytes, 10, 2, &11_i64.to_be_bytes());
        field(&mut bytes, 10, 3, &22_i64.to_be_bytes());
        field(&mut bytes, 10, 4, &33_i64.to_be_bytes());
        bytes.push(0);

        let mut input = super::BinaryInput {
            bytes: &bytes,
            pos: 0,
        };
        let reference = super::read_binary_ref(&mut input).expect("reads");
        check!(reference.ref_type == 7);
        check!(reference.trace_id_low == 11, "field two is the low half");
        check!(
            reference.trace_id_high == 22,
            "field three is the high half, not the low"
        );
        check!(
            reference.span_id == 33,
            "field four is the span, not a trace half"
        );

        // Fields may arrive in any order, since each carries its own id.
        let mut bytes = Vec::new();
        field(&mut bytes, 10, 4, &33_i64.to_be_bytes());
        field(&mut bytes, 10, 2, &11_i64.to_be_bytes());
        bytes.push(0);
        let mut input = super::BinaryInput {
            bytes: &bytes,
            pos: 0,
        };
        let reference = super::read_binary_ref(&mut input).expect("reads");
        check!(reference.span_id == 33);
        check!(reference.trace_id_low == 11);
        check!(
            reference.trace_id_high == 0,
            "an absent field keeps its default"
        );

        // An unknown field is skipped rather than consuming the one after it.
        let mut bytes = Vec::new();
        field(&mut bytes, 10, 9, &99_i64.to_be_bytes());
        field(&mut bytes, 10, 4, &33_i64.to_be_bytes());
        bytes.push(0);
        let mut input = super::BinaryInput {
            bytes: &bytes,
            pos: 0,
        };
        let reference = super::read_binary_ref(&mut input).expect("reads");
        check!(
            reference.span_id == 33,
            "the field after the unknown one still lands"
        );

        // An empty struct is all defaults rather than an error.
        let mut input = super::BinaryInput {
            bytes: &[0],
            pos: 0,
        };
        let reference = super::read_binary_ref(&mut input).expect("reads");
        check!(reference.ref_type == 0);
        check!(reference.span_id == 0);
    }

    /// The two inputs prefix a binary field with different length encodings:
    /// compact a varint, binary a big-endian i32. Each is given both framings
    /// and must read only its own, and both are checked where the payload
    /// exactly fills the buffer -- the case that separates `end > len` from
    /// `end >= len`.
    #[test]
    fn binary_fields_are_framed_by_each_protocol_own_length() {
        // Compact: a one-byte varint length of three, then three bytes.
        let compact_bytes = [0x03, b'a', b'b', b'c'];
        let mut input = super::CompactInput {
            bytes: &compact_bytes,
            pos: 0,
        };
        check!(input.read_binary().expect("reads") == b"abc".to_vec());
        check!(
            input.pos == 4,
            "the cursor covers the length and the payload"
        );

        // Exactly filling the buffer is complete, not truncated.
        let mut input = super::CompactInput {
            bytes: &compact_bytes,
            pos: 0,
        };
        check!(input.read_binary().is_ok(), "a payload may end the buffer");

        // One byte short is truncated.
        let mut input = super::CompactInput {
            bytes: &compact_bytes[..3],
            pos: 0,
        };
        check!(input.read_binary().is_err(), "one byte short");

        // Trailing bytes are left for the caller.
        let with_tail = [0x03, b'a', b'b', b'c', 0xff];
        let mut input = super::CompactInput {
            bytes: &with_tail,
            pos: 0,
        };
        check!(input.read_binary().expect("reads") == b"abc".to_vec());
        check!(input.pos == 4, "and the tail is not consumed");

        // Binary: a four-byte length of three, then three bytes.
        let binary_bytes = [0x00, 0x00, 0x00, 0x03, b'x', b'y', b'z'];
        let mut input = super::BinaryInput {
            bytes: &binary_bytes,
            pos: 0,
        };
        check!(input.read_binary().expect("reads") == b"xyz".to_vec());
        check!(input.pos == 7, "four length bytes plus three payload");

        let mut input = super::BinaryInput {
            bytes: &binary_bytes[..6],
            pos: 0,
        };
        check!(input.read_binary().is_err(), "one byte short");

        // An empty payload is a value, not an absence.
        let mut input = super::CompactInput {
            bytes: &[0x00],
            pos: 0,
        };
        check!(input.read_binary().expect("reads").is_empty());
        let mut input = super::BinaryInput {
            bytes: &[0, 0, 0, 0],
            pos: 0,
        };
        check!(input.read_binary().expect("reads").is_empty());

        // A negative length cannot be a size, and only the binary framing can
        // express one.
        let mut input = super::BinaryInput {
            bytes: &[0xff, 0xff, 0xff, 0xff, 0, 0],
            pos: 0,
        };
        check!(
            input.read_binary().is_err(),
            "a negative i32 length is refused"
        );
    }

    /// `read_key_value` dispatches on the field id *and* its wire type
    /// together, so a tag's value takes the variant its type says. Each
    /// variant is built and read back, with values that differ from one
    /// another so a variant reading a neighbouring field is visible.
    #[test]
    fn a_jaeger_tag_takes_the_variant_its_wire_type_names() {
        use super::test_support::{write_field_header, write_i64_field, write_string_field};

        // Key in field 1, then one value field, then the stop byte.
        let string_tag = {
            let mut out = Vec::new();
            let mut last = 0;
            write_string_field(&mut out, 1, "http.method", &mut last);
            write_string_field(&mut out, 3, "GET", &mut last);
            out.push(0);
            out
        };
        let mut input = super::CompactInput {
            bytes: &string_tag,
            pos: 0,
        };
        let tag = super::read_key_value(&mut input).expect("reads");
        check!(tag.key == "http.method");
        check!(tag.value == AttrValue::Str("GET".into()));

        let int_tag = {
            let mut out = Vec::new();
            let mut last = 0;
            write_string_field(&mut out, 1, "http.status", &mut last);
            write_i64_field(&mut out, 6, 503, &mut last);
            out.push(0);
            out
        };
        let mut input = super::CompactInput {
            bytes: &int_tag,
            pos: 0,
        };
        let tag = super::read_key_value(&mut input).expect("reads");
        check!(tag.key == "http.status");
        check!(
            tag.value == AttrValue::Int(503),
            "an i64 field is an int, not a string"
        );

        // The two boolean types carry their value in the type itself rather
        // than in a payload, so each needs its own field header.
        for (type_id, expected) in [(1_u8, true), (2_u8, false)] {
            let mut out = Vec::new();
            let mut last = 0;
            write_string_field(&mut out, 1, "retryable", &mut last);
            write_field_header(&mut out, type_id, 5, &mut last);
            out.push(0);
            let mut input = super::CompactInput {
                bytes: &out,
                pos: 0,
            };
            let tag = super::read_key_value(&mut input).expect("reads");
            check!(
                tag.value == AttrValue::Bool(expected),
                "type {type_id} is {expected}"
            );
        }

        // A tag with no recognised value field falls back to an empty string
        // rather than failing, and an unknown field is skipped rather than
        // consuming the ones after it.
        let bare = {
            let mut out = Vec::new();
            let mut last = 0;
            write_string_field(&mut out, 1, "lonely", &mut last);
            out.push(0);
            out
        };
        let mut input = super::CompactInput {
            bytes: &bare,
            pos: 0,
        };
        let tag = super::read_key_value(&mut input).expect("reads");
        check!(tag.key == "lonely");
        check!(
            tag.value == AttrValue::Str(String::new()),
            "no value means an empty string"
        );

        let with_unknown = {
            let mut out = Vec::new();
            let mut last = 0;
            write_string_field(&mut out, 1, "kept", &mut last);
            write_i64_field(&mut out, 9, 77, &mut last);
            write_i64_field(&mut out, 6, 42, &mut last);
            out.push(0);
            out
        };
        let mut input = super::CompactInput {
            bytes: &with_unknown,
            pos: 0,
        };
        let tag = super::read_key_value(&mut input).expect("reads");
        check!(tag.key == "kept");
        check!(
            tag.value == AttrValue::Int(42),
            "the unknown field is skipped, not read as the value"
        );
    }

    /// The two Thrift inputs read doubles with opposite byte order: compact is
    /// little-endian, binary is big-endian. That is a real protocol
    /// difference, not an accident of two similar functions, so each reader is
    /// given both encodings and must take only its own.
    #[test]
    fn the_two_thrift_inputs_read_doubles_with_opposite_byte_order() {
        let value = 1.5_f64;
        let little = value.to_le_bytes();
        let big = value.to_be_bytes();
        check!(little != big, "the fixture must distinguish the two orders");

        let mut compact = super::CompactInput {
            bytes: &little,
            pos: 0,
        };
        check!(
            compact.read_double().expect("reads").to_bits() == value.to_bits(),
            "compact is little-endian"
        );

        let mut compact_wrong = super::CompactInput {
            bytes: &big,
            pos: 0,
        };
        check!(
            compact_wrong.read_double().expect("reads").to_bits() != value.to_bits(),
            "and does not read the other order as the same number"
        );

        let mut binary = super::BinaryInput {
            bytes: &big,
            pos: 0,
        };
        check!(
            binary.read_double().expect("reads").to_bits() == value.to_bits(),
            "binary is big-endian"
        );

        let mut binary_wrong = super::BinaryInput {
            bytes: &little,
            pos: 0,
        };
        check!(binary_wrong.read_double().expect("reads").to_bits() != value.to_bits());

        // Each consumes exactly eight bytes and leaves the rest.
        let mut trailing = [0_u8; 9];
        trailing[..8].copy_from_slice(&big);
        trailing[8] = 0x7f;
        let mut binary = super::BinaryInput {
            bytes: &trailing,
            pos: 0,
        };
        check!(binary.read_double().expect("reads").to_bits() == value.to_bits());
        check!(binary.pos == 8, "the cursor stops after the double");

        // Seven bytes is not a double.
        let mut short = super::BinaryInput {
            bytes: &big[..7],
            pos: 0,
        };
        check!(short.read_double().is_err(), "one byte short");
    }

    /// The binary input reads its integers big-endian too, and each width
    /// consumes only its own bytes.
    #[test]
    fn binary_thrift_integers_are_big_endian_and_sized() {
        let bytes = [
            0x00, 0x00, 0x01, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x04,
        ];

        let mut input = super::BinaryInput {
            bytes: &bytes,
            pos: 0,
        };
        check!(
            input.read_i32().expect("reads") == 0x0102,
            "four bytes, most significant first"
        );
        check!(input.pos == 4);

        let mut input = super::BinaryInput {
            bytes: &bytes[4..],
            pos: 0,
        };
        check!(
            input.read_i64().expect("reads") == 0x0304,
            "eight bytes, most significant first"
        );
        check!(input.pos == 8);

        // A leading high bit makes the value negative.
        let mut input = super::BinaryInput {
            bytes: &[0xff, 0xff, 0xff, 0xff],
            pos: 0,
        };
        check!(input.read_i32().expect("reads") == -1);

        // Short input on each width.
        let mut input = super::BinaryInput {
            bytes: &[0, 0, 0],
            pos: 0,
        };
        check!(input.read_i32().is_err(), "three bytes is not an i32");
        let mut input = super::BinaryInput {
            bytes: &[0; 7],
            pos: 0,
        };
        check!(input.read_i64().is_err(), "seven bytes is not an i64");
    }
    use assert2::check;

    use super::*;
    use crate::{
        ids::{TraceIdHigh, TraceIdLow},
        span::{AttrValue, EventRecord, SpanKind, StatusCode},
    };

    /// Jaeger carries the span kind as a `span.kind` tag rather than a field.
    /// Every name maps to its own kind, an unknown or absent one falls back
    /// to internal, and a tag whose value is not a string is not a kind at
    /// all -- a fallback reached by the wrong route still looks right until
    /// a real kind is present and ignored.
    #[test]
    fn the_jaeger_span_kind_tag_maps_each_name_and_falls_back_to_internal() {
        let tag = |key: &str, value: AttrValue| KeyValue {
            key: key.to_string(),
            value,
        };
        let str_tag = |key: &str, value: &str| tag(key, AttrValue::Str(value.to_string()));
        let kind = |tags: Vec<KeyValue>| super::span_kind(&tags);

        for (name, expected) in [
            ("server", SpanKind::Server),
            ("client", SpanKind::Client),
            ("producer", SpanKind::Producer),
            ("consumer", SpanKind::Consumer),
            ("internal", SpanKind::Internal),
        ] {
            check!(kind(vec![str_tag("span.kind", name)]) == expected, "{name}");
        }

        check!(kind(vec![]) == SpanKind::Internal, "no tags at all");
        check!(
            kind(vec![str_tag("other", "server")]) == SpanKind::Internal,
            "the key is matched, not the value"
        );
        check!(
            kind(vec![str_tag("span.kind", "gateway")]) == SpanKind::Internal,
            "an unknown kind falls back"
        );
        check!(
            kind(vec![str_tag("span.kind", "Server")]) == SpanKind::Internal,
            "the match is case-sensitive"
        );
        check!(
            kind(vec![tag("span.kind", AttrValue::Int(2))]) == SpanKind::Internal,
            "a non-string value is not a kind"
        );

        // A decoy in front of the real tag, so the key is shown to be found
        // rather than the first tag taken.
        check!(
            kind(vec![
                str_tag("service", "api"),
                str_tag("span.kind", "server")
            ]) == SpanKind::Server,
            "the tag is found wherever it sits"
        );
    }

    #[test]
    fn decodes_jaeger_thrift_batch() {
        let spans = decode_jaeger_thrift(&encode_sample_batch()).unwrap();

        assert2::assert!(
            spans
                == vec![Span {
                    trace_id: [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
                    span_id: [0, 0, 0, 0, 0, 0, 0, 3],
                    parent_span_id: Some([0, 0, 0, 0, 0, 0, 0, 4]),
                    name: "GET /".into(),
                    kind: SpanKind::Server,
                    start_ns: 1_000_000,
                    duration_ns: 25_000,
                    status: StatusCode::Error,
                    status_message: String::new(),
                    resource_attrs: vec![
                        KeyValue {
                            key: "process.tag".into(),
                            value: AttrValue::Str("present".into()),
                        },
                        KeyValue {
                            key: "service.name".into(),
                            value: AttrValue::Str("checkout".into()),
                        },
                    ],
                    span_attrs: vec![
                        KeyValue {
                            key: "span.kind".into(),
                            value: AttrValue::Str("server".into()),
                        },
                        KeyValue {
                            key: "http.method".into(),
                            value: AttrValue::Str("GET".into()),
                        },
                        KeyValue {
                            key: "error".into(),
                            value: AttrValue::Bool(true),
                        },
                    ],
                    events: vec![EventRecord {
                        time_unix_nano: 1_005_000,
                        name: "cache.miss".into(),
                        attrs: vec![
                            KeyValue {
                                key: "event".into(),
                                value: AttrValue::Str("cache.miss".into()),
                            },
                            KeyValue {
                                key: "cache.key".into(),
                                value: AttrValue::Str("users".into()),
                            },
                        ],
                    }],
                    links: vec![LinkRecord {
                        trace_id: [0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 5],
                        span_id: [0, 0, 0, 0, 0, 0, 0, 7],
                        attrs: vec![KeyValue {
                            key: "ref.type".into(),
                            value: AttrValue::Str("follows_from".into()),
                        }],
                    }],
                    instrumentation_scope: String::new(),
                    instrumentation_version: String::new(),
                }]
        );
    }

    #[test]
    fn decodes_jaeger_binary_thrift_batch() {
        let spans = decode_jaeger_binary_thrift(&encode_binary_sample_batch()).unwrap();

        assert2::assert!(
            spans
                == vec![Span {
                    trace_id: [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
                    span_id: [0, 0, 0, 0, 0, 0, 0, 3],
                    parent_span_id: None,
                    name: "GET /binary".into(),
                    kind: SpanKind::Server,
                    start_ns: 1_000_000,
                    duration_ns: 25_000,
                    status: StatusCode::Error,
                    status_message: String::new(),
                    resource_attrs: vec![
                        KeyValue {
                            key: "process.tag".into(),
                            value: AttrValue::Str("present".into()),
                        },
                        KeyValue {
                            key: "service.name".into(),
                            value: AttrValue::Str("checkout".into()),
                        },
                    ],
                    span_attrs: vec![
                        KeyValue {
                            key: "span.kind".into(),
                            value: AttrValue::Str("server".into()),
                        },
                        KeyValue {
                            key: "http.method".into(),
                            value: AttrValue::Str("GET".into()),
                        },
                        KeyValue {
                            key: "error".into(),
                            value: AttrValue::Bool(true),
                        },
                    ],
                    events: Vec::new(),
                    links: Vec::new(),
                    instrumentation_scope: String::new(),
                    instrumentation_version: String::new(),
                }]
        );
    }

    #[test]
    fn compact_thrift_skips_unknown_map_fields() {
        let spans = decode_jaeger_thrift(&encode_sample_batch_with_unknown_map()).unwrap();

        assert2::assert!(spans.len() == 1);
        assert2::assert!(spans[0].name.as_str() == "GET /");
    }

    #[test]
    fn binary_thrift_skips_unknown_map_fields() {
        let spans =
            decode_jaeger_binary_thrift(&encode_binary_sample_batch_with_unknown_map()).unwrap();

        assert2::assert!(spans.len() == 1);
        assert2::assert!(spans[0].name.as_str() == "GET /binary");
    }

    /// The binary-thrift sample batch leaves several span fields untouched,
    /// and one of them -- the parent span id -- it sets to zero, which is
    /// also its default. Deleting that arm therefore changed nothing. This
    /// batch gives every uncovered field a value distinguishable from its
    /// default: a non-zero parent, a reference list, a log list, and tag
    /// types the sample never uses.
    #[test]
    fn a_binary_thrift_span_reads_the_fields_the_sample_batch_leaves_empty() {
        const T_STOP: u8 = 0;
        const T_DOUBLE: u8 = 4;
        const T_I32: u8 = 8;
        const T_I64: u8 = 10;
        const T_BINARY: u8 = 11;
        const T_STRUCT: u8 = 12;
        const T_LIST: u8 = 15;

        fn field(out: &mut Vec<u8>, type_: u8, id: i16) {
            out.push(type_);
            out.extend_from_slice(&id.to_be_bytes());
        }
        fn bytes(out: &mut Vec<u8>, value: &[u8]) {
            out.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
            out.extend_from_slice(value);
        }
        fn string_field(out: &mut Vec<u8>, id: i16, value: &str) {
            field(out, T_BINARY, id);
            bytes(out, value.as_bytes());
        }
        fn i32_field(out: &mut Vec<u8>, id: i16, value: i32) {
            field(out, T_I32, id);
            out.extend_from_slice(&value.to_be_bytes());
        }
        fn i64_field(out: &mut Vec<u8>, id: i16, value: i64) {
            field(out, T_I64, id);
            out.extend_from_slice(&value.to_be_bytes());
        }
        fn list_header(out: &mut Vec<u8>, id: i16, count: i32) {
            field(out, T_LIST, id);
            out.push(T_STRUCT);
            out.extend_from_slice(&count.to_be_bytes());
        }
        // The value's wire type is named by field 2 and carried by the field
        // id that follows, and the reader takes the variant from the id.
        fn kv_string(out: &mut Vec<u8>, key: &str, value: &str) {
            string_field(out, 1, key);
            i32_field(out, 2, 0);
            string_field(out, 3, value);
            out.push(T_STOP);
        }
        fn kv_double(out: &mut Vec<u8>, key: &str, value: f64) {
            string_field(out, 1, key);
            i32_field(out, 2, 1);
            field(out, T_DOUBLE, 4);
            out.extend_from_slice(&value.to_be_bytes());
            out.push(T_STOP);
        }
        fn kv_int(out: &mut Vec<u8>, key: &str, value: i64) {
            string_field(out, 1, key);
            i32_field(out, 2, 3);
            i64_field(out, 6, value);
            out.push(T_STOP);
        }
        fn kv_bytes(out: &mut Vec<u8>, key: &str, value: &[u8]) {
            string_field(out, 1, key);
            i32_field(out, 2, 4);
            field(out, T_BINARY, 7);
            bytes(out, value);
            out.push(T_STOP);
        }

        let mut out = Vec::new();
        field(&mut out, T_STRUCT, 1);
        string_field(&mut out, 1, "checkout");
        out.push(T_STOP);

        list_header(&mut out, 2, 1);
        i64_field(&mut out, 1, 2);
        i64_field(&mut out, 2, 1);
        i64_field(&mut out, 3, 3);
        // Non-zero, and no child_of reference to override it.
        i64_field(&mut out, 4, 9);
        string_field(&mut out, 5, "GET /full");
        // A follows_from reference becomes a link; a child_of one would
        // instead have become the parent.
        list_header(&mut out, 6, 1);
        i32_field(&mut out, 1, 1);
        i64_field(&mut out, 2, 5);
        i64_field(&mut out, 3, 4);
        i64_field(&mut out, 4, 6);
        out.push(T_STOP);
        i64_field(&mut out, 8, 1_000);
        i64_field(&mut out, 9, 25);
        list_header(&mut out, 10, 3);
        kv_double(&mut out, "latency.ms", 1.5);
        kv_int(&mut out, "retry.count", 7);
        kv_bytes(&mut out, "payload", &[1, 2, 3]);
        list_header(&mut out, 11, 1);
        i64_field(&mut out, 1, 1_005);
        list_header(&mut out, 2, 2);
        kv_string(&mut out, "event", "cache.miss");
        kv_int(&mut out, "retries", 2);
        out.push(T_STOP);
        out.push(T_STOP);
        out.push(T_STOP);

        let spans = decode_jaeger_binary_thrift(&out).expect("the batch decodes");

        check!(
            spans
                == vec![Span {
                    trace_id: [0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2],
                    span_id: [0, 0, 0, 0, 0, 0, 0, 3],
                    parent_span_id: Some([0, 0, 0, 0, 0, 0, 0, 9]),
                    name: "GET /full".into(),
                    kind: SpanKind::Internal,
                    start_ns: 1_000_000,
                    duration_ns: 25_000,
                    status: StatusCode::Unset,
                    status_message: String::new(),
                    resource_attrs: vec![KeyValue {
                        key: "service.name".into(),
                        value: AttrValue::Str("checkout".into()),
                    }],
                    span_attrs: vec![
                        KeyValue {
                            key: "latency.ms".into(),
                            value: AttrValue::Double(1.5),
                        },
                        KeyValue {
                            key: "retry.count".into(),
                            value: AttrValue::Int(7),
                        },
                        KeyValue {
                            key: "payload".into(),
                            value: AttrValue::Bytes(vec![1, 2, 3]),
                        },
                    ],
                    events: vec![EventRecord {
                        time_unix_nano: 1_005_000,
                        name: "cache.miss".into(),
                        attrs: vec![
                            KeyValue {
                                key: "event".into(),
                                value: AttrValue::Str("cache.miss".into()),
                            },
                            KeyValue {
                                key: "retries".into(),
                                value: AttrValue::Int(2),
                            },
                        ],
                    }],
                    links: vec![LinkRecord {
                        trace_id: [0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 5],
                        span_id: [0, 0, 0, 0, 0, 0, 0, 6],
                        attrs: vec![KeyValue {
                            key: "ref.type".into(),
                            value: AttrValue::Str("follows_from".into()),
                        }],
                    }],
                    instrumentation_scope: String::new(),
                    instrumentation_version: String::new(),
                }]
        );
    }

    fn encode_binary_sample_batch() -> Vec<u8> {
        const T_STOP: u8 = 0;
        const T_BOOL: u8 = 2;
        const T_I32: u8 = 8;
        const T_I64: u8 = 10;
        const T_BINARY: u8 = 11;
        const T_STRUCT: u8 = 12;
        const T_LIST: u8 = 15;

        fn field(out: &mut Vec<u8>, type_: u8, id: i16) {
            out.push(type_);
            out.extend_from_slice(&id.to_be_bytes());
        }
        fn string(out: &mut Vec<u8>, value: &str) {
            out.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
            out.extend_from_slice(value.as_bytes());
        }
        fn string_field(out: &mut Vec<u8>, id: i16, value: &str) {
            field(out, T_BINARY, id);
            string(out, value);
        }
        fn i32_field(out: &mut Vec<u8>, id: i16, value: i32) {
            field(out, T_I32, id);
            out.extend_from_slice(&value.to_be_bytes());
        }
        fn i64_field(out: &mut Vec<u8>, id: i16, value: i64) {
            field(out, T_I64, id);
            out.extend_from_slice(&value.to_be_bytes());
        }
        fn bool_field(out: &mut Vec<u8>, id: i16, value: bool) {
            field(out, T_BOOL, id);
            out.push(u8::from(value));
        }
        fn key_value_string(out: &mut Vec<u8>, key: &str, value: &str) {
            string_field(out, 1, key);
            i32_field(out, 2, 0);
            string_field(out, 3, value);
            out.push(T_STOP);
        }
        fn key_value_bool(out: &mut Vec<u8>, key: &str, value: bool) {
            string_field(out, 1, key);
            i32_field(out, 2, 3);
            bool_field(out, 5, value);
            out.push(T_STOP);
        }

        let mut out = Vec::new();
        field(&mut out, T_STRUCT, 1);
        string_field(&mut out, 1, "checkout");
        field(&mut out, T_LIST, 2);
        out.push(T_STRUCT);
        out.extend_from_slice(&1_i32.to_be_bytes());
        key_value_string(&mut out, "process.tag", "present");
        out.push(T_STOP);

        field(&mut out, T_LIST, 2);
        out.push(T_STRUCT);
        out.extend_from_slice(&1_i32.to_be_bytes());
        i64_field(&mut out, 1, 2);
        i64_field(&mut out, 2, 1);
        i64_field(&mut out, 3, 3);
        i64_field(&mut out, 4, 0);
        string_field(&mut out, 5, "GET /binary");
        i64_field(&mut out, 8, 1_000);
        i64_field(&mut out, 9, 25);
        field(&mut out, T_LIST, 10);
        out.push(T_STRUCT);
        out.extend_from_slice(&3_i32.to_be_bytes());
        key_value_string(&mut out, "span.kind", "server");
        key_value_string(&mut out, "http.method", "GET");
        key_value_bool(&mut out, "error", true);
        out.push(T_STOP);
        out.push(T_STOP);
        out
    }

    fn encode_binary_sample_batch_with_unknown_map() -> Vec<u8> {
        const T_MAP: u8 = 13;
        const T_BINARY: u8 = 11;
        const T_I32: u8 = 8;

        let mut out = encode_binary_sample_batch();
        out.pop();
        out.push(T_MAP);
        out.extend_from_slice(&3_i16.to_be_bytes());
        out.push(T_BINARY);
        out.push(T_I32);
        out.extend_from_slice(&1_i32.to_be_bytes());
        out.extend_from_slice(&7_i32.to_be_bytes());
        out.extend_from_slice(b"ignored");
        out.extend_from_slice(&42_i32.to_be_bytes());
        out.push(0);
        out
    }

    fn encode_sample_batch() -> Vec<u8> {
        let mut out = Vec::new();
        write_process(&mut out, 1, "checkout");
        write_span_list(&mut out, 2);
        out.push(0);
        out
    }

    fn encode_sample_batch_with_unknown_map() -> Vec<u8> {
        let mut out = Vec::new();
        write_process(&mut out, 1, "checkout");
        write_span_list(&mut out, 2);
        let mut last = 2;
        write_field_header(&mut out, 11, 3, &mut last);
        write_map_header(&mut out, 8, 5, 1);
        write_varint(&mut out, 7);
        out.extend_from_slice(b"ignored");
        write_varint(&mut out, zigzag_i32(42));
        out.push(0);
        out
    }

    fn write_process(out: &mut Vec<u8>, field_id: i16, service: &str) {
        write_field_header(out, 12, field_id, &mut 0);
        let mut last = 0;
        write_string_field(out, 1, service, &mut last);
        write_field_header(out, 9, 2, &mut last);
        write_list_header(out, 12, 1);
        write_key_value_string(out, "process.tag", "present");
        out.push(0);
    }

    fn write_span_list(out: &mut Vec<u8>, field_id: i16) {
        write_field_header(out, 9, field_id, &mut 1);
        write_list_header(out, 12, 1);
        let mut last = 0;
        write_i64_field(out, 1, 2, &mut last);
        write_i64_field(out, 2, 1, &mut last);
        write_i64_field(out, 3, 3, &mut last);
        write_i64_field(out, 4, 0, &mut last);
        write_string_field(out, 5, "GET /", &mut last);
        write_field_header(out, 9, 6, &mut last);
        write_list_header(out, 12, 2);
        write_span_ref(out, 0, TraceIdLow(2), TraceIdHigh(1), 4);
        write_span_ref(out, 1, TraceIdLow(5), TraceIdHigh(6), 7);
        write_i32_field(out, 7, 0, &mut last);
        write_i64_field(out, 8, 1_000, &mut last);
        write_i64_field(out, 9, 25, &mut last);
        write_field_header(out, 9, 10, &mut last);
        write_list_header(out, 12, 3);
        write_key_value_string(out, "span.kind", "server");
        write_key_value_string(out, "http.method", "GET");
        write_key_value_bool(out, "error", true);
        write_field_header(out, 9, 11, &mut last);
        write_list_header(out, 12, 1);
        write_log(out);
        out.push(0);
    }

    fn write_span_ref(
        out: &mut Vec<u8>,
        ref_type: i32,
        low: TraceIdLow,
        high: TraceIdHigh,
        span_id: i64,
    ) {
        let mut last = 0;
        write_i32_field(out, 1, ref_type, &mut last);
        write_i64_field(out, 2, low.0, &mut last);
        write_i64_field(out, 3, high.0, &mut last);
        write_i64_field(out, 4, span_id, &mut last);
        out.push(0);
    }

    fn write_log(out: &mut Vec<u8>) {
        let mut last = 0;
        write_i64_field(out, 1, 1_005, &mut last);
        write_field_header(out, 9, 2, &mut last);
        write_list_header(out, 12, 2);
        write_key_value_string(out, "event", "cache.miss");
        write_key_value_string(out, "cache.key", "users");
        out.push(0);
    }

    fn write_key_value_string(out: &mut Vec<u8>, key: &str, value: &str) {
        let mut last = 0;
        write_string_field(out, 1, key, &mut last);
        write_i32_field(out, 2, 0, &mut last);
        write_string_field(out, 3, value, &mut last);
        out.push(0);
    }

    fn write_key_value_bool(out: &mut Vec<u8>, key: &str, value: bool) {
        let mut last = 0;
        write_string_field(out, 1, key, &mut last);
        write_i32_field(out, 2, 3, &mut last);
        write_bool_field(out, 5, value, &mut last);
        out.push(0);
    }

    fn write_i32_field(out: &mut Vec<u8>, id: i16, value: i32, last: &mut i16) {
        write_field_header(out, 5, id, last);
        write_varint(out, zigzag_i32(value));
    }

    fn write_i64_field(out: &mut Vec<u8>, id: i16, value: i64, last: &mut i16) {
        write_field_header(out, 6, id, last);
        write_varint(out, zigzag_i64(value));
    }

    fn write_string_field(out: &mut Vec<u8>, id: i16, value: &str, last: &mut i16) {
        write_field_header(out, 8, id, last);
        write_varint(out, u64::try_from(value.len()).unwrap());
        out.extend_from_slice(value.as_bytes());
    }

    fn write_bool_field(out: &mut Vec<u8>, id: i16, value: bool, last: &mut i16) {
        write_field_header(out, if value { 1 } else { 2 }, id, last);
    }

    fn write_field_header(out: &mut Vec<u8>, type_id: u8, id: i16, last: &mut i16) {
        let delta = id - *last;
        if (1..=15).contains(&delta) {
            out.push((u8::try_from(delta).unwrap() << 4) | type_id);
        } else {
            out.push(type_id);
            write_varint(out, zigzag_i32(i32::from(id)));
        }
        *last = id;
    }

    fn write_list_header(out: &mut Vec<u8>, element_type: u8, size: usize) {
        if size < 15 {
            out.push((u8::try_from(size).unwrap() << 4) | element_type);
        } else {
            out.push(0xF0 | element_type);
            write_varint(out, u64::try_from(size).unwrap());
        }
    }

    fn write_map_header(out: &mut Vec<u8>, key_type: u8, value_type: u8, size: usize) {
        if size == 0 {
            out.push(0);
        } else {
            write_varint(out, u64::try_from(size).unwrap());
            out.push((key_type << 4) | value_type);
        }
    }

    fn write_varint(out: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            out.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
            value >>= 7;
        }
        out.push(u8::try_from(value).unwrap());
    }

    fn zigzag_i32(value: i32) -> u64 {
        u64::from(((value << 1) ^ (value >> 31)).cast_unsigned())
    }

    fn zigzag_i64(value: i64) -> u64 {
        ((value << 1) ^ (value >> 63)).cast_unsigned()
    }
}

// === split-modules: generated submodules ===
mod binary_input;
mod bt_binary;
mod bt_bool;
mod bt_byte;
mod bt_double;
mod bt_i16;
mod bt_i32;
mod bt_i64;
mod bt_list;
mod bt_map;
mod bt_set;
mod bt_stop;
mod bt_struct;
mod compact_input;
mod decode_jaeger_binary_thrift;
mod decode_jaeger_thrift;
mod i64_bytes;
mod jaeger_batch;
mod jaeger_log;
mod jaeger_process;
mod jaeger_ref;
mod jaeger_span;
mod jaeger_span_to_internal;
mod read_batch;
mod read_binary_batch;
mod read_binary_key_value;
mod read_binary_log;
mod read_binary_process;
mod read_binary_ref;
mod read_binary_span;
mod read_key_value;
mod read_log;
mod read_process;
mod read_ref;
mod read_span;
mod ref_type_name;
mod span_kind;
mod span_logs_to_events;
mod span_status;
mod spans_from_batch;
mod t_binary;
mod t_bool_false;
mod t_bool_true;
mod t_byte;
mod t_double;
mod t_i16;
mod t_i32;
mod t_i64;
mod t_list;
mod t_map;
mod t_set;
mod t_stop;
mod t_struct;
mod trace_id;

use binary_input::BinaryInput;
use bt_binary::BT_BINARY;
use bt_bool::BT_BOOL;
use bt_byte::BT_BYTE;
use bt_double::BT_DOUBLE;
use bt_i16::BT_I16;
use bt_i32::BT_I32;
use bt_i64::BT_I64;
use bt_list::BT_LIST;
use bt_map::BT_MAP;
use bt_set::BT_SET;
use bt_stop::BT_STOP;
use bt_struct::BT_STRUCT;
use compact_input::CompactInput;
pub use decode_jaeger_binary_thrift::decode_jaeger_binary_thrift;
pub use decode_jaeger_thrift::decode_jaeger_thrift;
use i64_bytes::i64_bytes;
pub(super) use jaeger_batch::JaegerBatch;
pub(super) use jaeger_log::JaegerLog;
pub(super) use jaeger_process::JaegerProcess;
pub(super) use jaeger_ref::JaegerRef;
pub(super) use jaeger_span::JaegerSpan;
use jaeger_span_to_internal::jaeger_span_to_internal;
use read_batch::read_batch;
use read_binary_batch::read_binary_batch;
use read_binary_key_value::read_binary_key_value;
use read_binary_log::read_binary_log;
use read_binary_process::read_binary_process;
use read_binary_ref::read_binary_ref;
use read_binary_span::read_binary_span;
use read_key_value::read_key_value;
use read_log::read_log;
use read_process::read_process;
use read_ref::read_ref;
use read_span::read_span;
use ref_type_name::ref_type_name;
use span_kind::span_kind;
use span_logs_to_events::span_logs_to_events;
use span_status::span_status;
pub(super) use spans_from_batch::spans_from_batch;
use t_binary::T_BINARY;
use t_bool_false::T_BOOL_FALSE;
use t_bool_true::T_BOOL_TRUE;
use t_byte::T_BYTE;
use t_double::T_DOUBLE;
use t_i16::T_I16;
use t_i32::T_I32;
use t_i64::T_I64;
use t_list::T_LIST;
use t_map::T_MAP;
use t_set::T_SET;
use t_stop::T_STOP;
use t_struct::T_STRUCT;
use trace_id::trace_id;
