// Thrift encoders for Jaeger test fixtures.
//
// The library's unit tests and the `grafana_e2e` suite share this file, the
// suite through `#[path]`, so it reaches nothing outside itself.

/// Writes one compact-protocol struct into a borrowed buffer.
///
/// Compact field headers encode the field id as a delta from the previous
/// field in the same struct, so the writer remembers that id. A nested struct
/// starts its own writer through [`CompactStructWriter::nested_struct`].
pub struct CompactStructWriter<'out> {
    out: &'out mut Vec<u8>,
    last_field_id: i16,
}

impl<'out> CompactStructWriter<'out> {
    pub fn new(out: &'out mut Vec<u8>) -> Self {
        Self {
            out,
            last_field_id: 0,
        }
    }

    /// Starts a struct written into the same buffer, such as a list element
    /// or the payload of a struct field.
    pub fn nested_struct(&mut self) -> CompactStructWriter<'_> {
        CompactStructWriter::new(self.out)
    }

    pub fn i32_field(&mut self, field_id: i16, number: i32) {
        self.field_header(5, field_id);
        write_varint(self.out, zigzag_i32(number));
    }

    pub fn i64_field(&mut self, field_id: i16, number: i64) {
        self.field_header(6, field_id);
        write_varint(self.out, zigzag_i64(number));
    }

    pub fn string_field(&mut self, field_id: i16, text: &str) {
        self.field_header(8, field_id);
        write_varint(self.out, u64::try_from(text.len()).unwrap());
        self.out.extend_from_slice(text.as_bytes());
    }

    /// The compact protocol carries a boolean in the field's type nibble.
    pub fn bool_field(&mut self, field_id: i16, flag: bool) {
        self.field_header(if flag { 1 } else { 2 }, field_id);
    }

    pub fn field_header(&mut self, type_id: u8, field_id: i16) {
        let delta = field_id - self.last_field_id;
        if (1..=15).contains(&delta) {
            self.out.push((u8::try_from(delta).unwrap() << 4) | type_id);
        } else {
            self.out.push(type_id);
            write_varint(self.out, zigzag_i32(i32::from(field_id)));
        }
        self.last_field_id = field_id;
    }

    pub fn list_header(&mut self, element_type: u8, size: usize) {
        if size < 15 {
            self.out
                .push((u8::try_from(size).unwrap() << 4) | element_type);
        } else {
            self.out.push(0xF0 | element_type);
            write_varint(self.out, u64::try_from(size).unwrap());
        }
    }

    /// Writes a Jaeger `Tag` struct with a string value as a list element.
    pub fn string_tag(&mut self, key: &str, text: &str) {
        let mut tag = self.nested_struct();
        tag.string_field(1, key);
        tag.i32_field(2, 0);
        tag.string_field(3, text);
        tag.stop();
    }

    /// Writes a Jaeger `Tag` struct with a boolean value as a list element.
    pub fn bool_tag(&mut self, key: &str, flag: bool) {
        let mut tag = self.nested_struct();
        tag.string_field(1, key);
        tag.i32_field(2, 3);
        tag.bool_field(5, flag);
        tag.stop();
    }

    /// Ends the struct with the stop byte.
    pub fn stop(self) {
        self.out.push(0);
    }
}

pub fn write_varint(out: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        out.push(u8::try_from(value & 0x7f).unwrap() | 0x80);
        value >>= 7;
    }
    out.push(u8::try_from(value).unwrap());
}

pub fn zigzag_i32(value: i32) -> u64 {
    u64::from(((value << 1) ^ (value >> 31)).cast_unsigned())
}

pub fn zigzag_i64(value: i64) -> u64 {
    ((value << 1) ^ (value >> 63)).cast_unsigned()
}

// A Jaeger binary-thrift batch holding one span, `GET /binary`, whose
// embedded process names the service `checkout`.
pub fn encode_binary_sample_batch() -> Vec<u8> {
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
