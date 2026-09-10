//! A Thrift compact-protocol writer driven by fuzzer input.
//!
//! The models below mirror the Jaeger `Batch` schema that
//! //crates/traces/src/wire/jaeger decodes, one model field per schema field.
//! What the fuzzer chooses is the value of each field, whether the field is
//! written at all, and whether it is written with the wire type the schema
//! names or with some other one -- the last of those is what drives the
//! decoder's `skip` arm, which is the part of a hand-rolled decoder that has
//! to get every length prefix right without a schema to check itself against.
//!
//! The shapes are deliberately not recursive. `Arbitrary` derived on a
//! recursive type recurses on the fuzzer's say-so, and a stack overflow in the
//! generator says nothing about the decoder.

use arbitrary::Arbitrary;

// Compact-protocol type ids. //crates/traces/src/wire/jaeger holds the same
// constants; they are private there, and repeating them is what lets this
// writer disagree with the decoder on purpose.
const T_STOP: u8 = 0;
const T_BOOL_TRUE: u8 = 1;
const T_BOOL_FALSE: u8 = 2;
const T_BYTE: u8 = 3;
const T_I16: u8 = 4;
const T_I32: u8 = 5;
const T_I64: u8 = 6;
const T_DOUBLE: u8 = 7;
const T_BINARY: u8 = 8;
const T_LIST: u8 = 9;
const T_SET: u8 = 10;
const T_MAP: u8 = 11;
const T_STRUCT: u8 = 12;

/// One field of a schema, as the fuzzer chose to write it.
#[derive(Arbitrary, Debug)]
pub enum Field<T> {
    /// Written with the wire type the schema names.
    Present(T),
    /// Not written at all, so the decoder keeps its default.
    Missing,
    /// Written under the schema's field id but with a different wire type, so
    /// the decoder has to skip a payload it did not expect.
    Confused(Scalar),
}

/// A value the writer can emit without knowing the schema, used for the
/// type-confused fields above and for the stray fields below.
#[derive(Arbitrary, Debug)]
pub enum Scalar {
    BoolTrue,
    BoolFalse,
    Byte(u8),
    I16(i16),
    I32(i32),
    I64(i64),
    Double(f64),
    Binary(Vec<u8>),
    /// A list of i64, which the decoder's `skip` has to walk element by
    /// element rather than by a byte length.
    I64List(Vec<i64>),
    /// A set, which shares the list framing but a different type id.
    I64Set(Vec<i64>),
    /// A map, whose header carries two type ids in one byte and whose length
    /// is a bare varint rather than a nibble.
    I64Map(Vec<(i64, i64)>),
    /// An empty struct: one stop byte.
    EmptyStruct,
}

/// A field the schema does not name, written between the ones it does.
#[derive(Arbitrary, Debug)]
pub struct StrayField {
    pub id: i16,
    pub value: Scalar,
}

/// The Jaeger `Batch` a compact-Thrift push body carries.
#[derive(Arbitrary, Debug)]
pub struct BatchModel {
    pub process: Field<ProcessModel>,
    pub spans: Field<Vec<SpanModel>>,
    pub stray: Vec<StrayField>,
    /// Cut the finished body short at this many bytes. A truncated body whose
    /// prefix is well formed reaches the truncation guard of whichever read
    /// the cut lands inside, which random bytes almost never do.
    pub truncate_to: Option<u16>,
}

#[derive(Arbitrary, Debug)]
pub struct ProcessModel {
    pub service_name: Field<String>,
    pub tags: Field<Vec<TagModel>>,
    pub stray: Vec<StrayField>,
}

#[derive(Arbitrary, Debug)]
pub struct SpanModel {
    pub trace_id_low: Field<i64>,
    pub trace_id_high: Field<i64>,
    pub span_id: Field<i64>,
    pub parent_span_id: Field<i64>,
    pub operation_name: Field<String>,
    pub references: Field<Vec<RefModel>>,
    pub flags: Field<i32>,
    pub start_time_micros: Field<i64>,
    pub duration_micros: Field<i64>,
    pub tags: Field<Vec<TagModel>>,
    pub logs: Field<Vec<LogModel>>,
    pub stray: Vec<StrayField>,
}

#[derive(Arbitrary, Debug)]
pub struct RefModel {
    pub ref_type: Field<i32>,
    pub trace_id_low: Field<i64>,
    pub trace_id_high: Field<i64>,
    pub span_id: Field<i64>,
    pub stray: Vec<StrayField>,
}

#[derive(Arbitrary, Debug)]
pub struct LogModel {
    pub timestamp_micros: Field<i64>,
    pub fields: Field<Vec<TagModel>>,
    pub stray: Vec<StrayField>,
}

/// A Jaeger tag. `value_type` is written independently of `value`, because the
/// decoder dispatches on the value field's own id and wire type and ignores
/// this one -- a disagreement between them is a shape a real client cannot
/// produce and an attacker can.
#[derive(Arbitrary, Debug)]
pub struct TagModel {
    pub key: Field<String>,
    pub value_type: Field<i32>,
    pub value: TagValueModel,
    pub stray: Vec<StrayField>,
}

#[derive(Arbitrary, Debug)]
pub enum TagValueModel {
    /// No value field at all, which the decoder fills with an empty string.
    None,
    Str(String),
    Double(f64),
    Bool(bool),
    Long(i64),
    Binary(Vec<u8>),
}

/// Encode a batch model as a compact-Thrift body.
#[must_use]
pub fn encode_batch(model: &BatchModel) -> Vec<u8> {
    let mut out = CompactWriter::default();
    let mut last = 0;
    out.field(&model.process, T_STRUCT, 1, &mut last, |writer, process| {
        writer.process(process);
    });
    out.field(&model.spans, T_LIST, 2, &mut last, |writer, spans| {
        writer.struct_list(spans, CompactWriter::span);
    });
    out.strays(&model.stray, &mut last);
    out.stop();

    let mut bytes = out.into_bytes();
    if let Some(limit) = model.truncate_to {
        bytes.truncate(usize::from(limit));
    }
    bytes
}

#[derive(Default)]
struct CompactWriter {
    out: Vec<u8>,
}

impl CompactWriter {
    fn into_bytes(self) -> Vec<u8> {
        self.out
    }

    /// Write one schema field, in whichever of the three forms the model names.
    fn field<T>(
        &mut self,
        field: &Field<T>,
        type_id: u8,
        id: i16,
        last: &mut i16,
        write: impl FnOnce(&mut Self, &T),
    ) {
        match field {
            Field::Present(value) => {
                self.field_header(type_id, id, last);
                write(self, value);
            }
            Field::Missing => {}
            Field::Confused(scalar) => self.scalar_field(scalar, id, last),
        }
    }

    fn strays(&mut self, strays: &[StrayField], last: &mut i16) {
        for stray in strays {
            self.scalar_field(&stray.value, stray.id, last);
        }
    }

    fn scalar_field(&mut self, scalar: &Scalar, id: i16, last: &mut i16) {
        match scalar {
            Scalar::BoolTrue => self.field_header(T_BOOL_TRUE, id, last),
            Scalar::BoolFalse => self.field_header(T_BOOL_FALSE, id, last),
            Scalar::Byte(value) => {
                self.field_header(T_BYTE, id, last);
                self.out.push(*value);
            }
            Scalar::I16(value) => {
                self.field_header(T_I16, id, last);
                self.varint(zigzag_i32(i32::from(*value)));
            }
            Scalar::I32(value) => {
                self.field_header(T_I32, id, last);
                self.varint(zigzag_i32(*value));
            }
            Scalar::I64(value) => {
                self.field_header(T_I64, id, last);
                self.varint(zigzag_i64(*value));
            }
            Scalar::Double(value) => {
                self.field_header(T_DOUBLE, id, last);
                self.out.extend_from_slice(&value.to_le_bytes());
            }
            Scalar::Binary(value) => {
                self.field_header(T_BINARY, id, last);
                self.binary(value);
            }
            Scalar::I64List(values) => {
                self.field_header(T_LIST, id, last);
                self.list_header(T_I64, values.len());
                for value in values {
                    self.varint(zigzag_i64(*value));
                }
            }
            Scalar::I64Set(values) => {
                self.field_header(T_SET, id, last);
                self.list_header(T_I64, values.len());
                for value in values {
                    self.varint(zigzag_i64(*value));
                }
            }
            Scalar::I64Map(entries) => {
                self.field_header(T_MAP, id, last);
                self.map_header(T_I64, T_I64, entries.len());
                for (key, value) in entries {
                    self.varint(zigzag_i64(*key));
                    self.varint(zigzag_i64(*value));
                }
            }
            Scalar::EmptyStruct => {
                self.field_header(T_STRUCT, id, last);
                self.stop();
            }
        }
    }

    fn process(&mut self, model: &ProcessModel) {
        let mut last = 0;
        self.field(
            &model.service_name,
            T_BINARY,
            1,
            &mut last,
            |writer, value| writer.binary(value.as_bytes()),
        );
        self.field(&model.tags, T_LIST, 2, &mut last, |writer, tags| {
            writer.struct_list(tags, CompactWriter::tag);
        });
        self.strays(&model.stray, &mut last);
        self.stop();
    }

    fn span(&mut self, model: &SpanModel) {
        let mut last = 0;
        self.i64_field(&model.trace_id_low, 1, &mut last);
        self.i64_field(&model.trace_id_high, 2, &mut last);
        self.i64_field(&model.span_id, 3, &mut last);
        self.i64_field(&model.parent_span_id, 4, &mut last);
        self.field(
            &model.operation_name,
            T_BINARY,
            5,
            &mut last,
            |writer, value| writer.binary(value.as_bytes()),
        );
        self.field(&model.references, T_LIST, 6, &mut last, |writer, refs| {
            writer.struct_list(refs, CompactWriter::span_ref);
        });
        self.i32_field(&model.flags, 7, &mut last);
        self.i64_field(&model.start_time_micros, 8, &mut last);
        self.i64_field(&model.duration_micros, 9, &mut last);
        self.field(&model.tags, T_LIST, 10, &mut last, |writer, tags| {
            writer.struct_list(tags, CompactWriter::tag);
        });
        self.field(&model.logs, T_LIST, 11, &mut last, |writer, logs| {
            writer.struct_list(logs, CompactWriter::log);
        });
        self.strays(&model.stray, &mut last);
        self.stop();
    }

    fn span_ref(&mut self, model: &RefModel) {
        let mut last = 0;
        self.i32_field(&model.ref_type, 1, &mut last);
        self.i64_field(&model.trace_id_low, 2, &mut last);
        self.i64_field(&model.trace_id_high, 3, &mut last);
        self.i64_field(&model.span_id, 4, &mut last);
        self.strays(&model.stray, &mut last);
        self.stop();
    }

    fn log(&mut self, model: &LogModel) {
        let mut last = 0;
        self.i64_field(&model.timestamp_micros, 1, &mut last);
        self.field(&model.fields, T_LIST, 2, &mut last, |writer, fields| {
            writer.struct_list(fields, CompactWriter::tag);
        });
        self.strays(&model.stray, &mut last);
        self.stop();
    }

    fn tag(&mut self, model: &TagModel) {
        let mut last = 0;
        self.field(&model.key, T_BINARY, 1, &mut last, |writer, value| {
            writer.binary(value.as_bytes());
        });
        self.i32_field(&model.value_type, 2, &mut last);
        match &model.value {
            TagValueModel::None => {}
            TagValueModel::Str(value) => {
                self.field_header(T_BINARY, 3, &mut last);
                self.binary(value.as_bytes());
            }
            TagValueModel::Double(value) => {
                self.field_header(T_DOUBLE, 4, &mut last);
                self.out.extend_from_slice(&value.to_le_bytes());
            }
            TagValueModel::Bool(value) => {
                let type_id = if *value { T_BOOL_TRUE } else { T_BOOL_FALSE };
                self.field_header(type_id, 5, &mut last);
            }
            TagValueModel::Long(value) => {
                self.field_header(T_I64, 6, &mut last);
                self.varint(zigzag_i64(*value));
            }
            TagValueModel::Binary(value) => {
                self.field_header(T_BINARY, 7, &mut last);
                self.binary(value);
            }
        }
        self.strays(&model.stray, &mut last);
        self.stop();
    }

    fn i64_field(&mut self, field: &Field<i64>, id: i16, last: &mut i16) {
        self.field(field, T_I64, id, last, |writer, value| {
            writer.varint(zigzag_i64(*value));
        });
    }

    fn i32_field(&mut self, field: &Field<i32>, id: i16, last: &mut i16) {
        self.field(field, T_I32, id, last, |writer, value| {
            writer.varint(zigzag_i32(*value));
        });
    }

    fn struct_list<T>(&mut self, items: &[T], write_one: impl Fn(&mut Self, &T)) {
        self.list_header(T_STRUCT, items.len());
        for item in items {
            write_one(self, item);
        }
    }

    fn field_header(&mut self, type_id: u8, id: i16, last: &mut i16) {
        // The short form carries a 1..=15 delta in the high nibble. Anything
        // else -- including a backwards or absent delta -- takes the long form,
        // a zero nibble followed by the zigzag field id.
        match id.checked_sub(*last) {
            Some(delta) if (1..=15).contains(&delta) => {
                let nibble = u8::try_from(delta).unwrap_or(1);
                self.out.push((nibble << 4) | type_id);
            }
            _ => {
                self.out.push(type_id);
                self.varint(zigzag_i32(i32::from(id)));
            }
        }
        *last = id;
    }

    fn list_header(&mut self, element_type: u8, len: usize) {
        if len < 15 {
            let nibble = u8::try_from(len).unwrap_or(0);
            self.out.push((nibble << 4) | element_type);
        } else {
            self.out.push(0xF0 | element_type);
            self.varint(u64::try_from(len).unwrap_or(u64::MAX));
        }
    }

    fn map_header(&mut self, key_type: u8, value_type: u8, len: usize) {
        self.varint(u64::try_from(len).unwrap_or(u64::MAX));
        if len > 0 {
            self.out.push((key_type << 4) | value_type);
        }
    }

    fn binary(&mut self, bytes: &[u8]) {
        self.varint(u64::try_from(bytes.len()).unwrap_or(u64::MAX));
        self.out.extend_from_slice(bytes);
    }

    fn varint(&mut self, mut value: u64) {
        while value >= 0x80 {
            self.out
                .push(u8::try_from(value & 0x7f).unwrap_or(0) | 0x80);
            value >>= 7;
        }
        self.out.push(u8::try_from(value).unwrap_or(0));
    }

    fn stop(&mut self) {
        self.out.push(T_STOP);
    }
}

fn zigzag_i32(value: i32) -> u64 {
    u64::from(((value << 1) ^ (value >> 31)).cast_unsigned())
}

fn zigzag_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)).cast_unsigned()
}
