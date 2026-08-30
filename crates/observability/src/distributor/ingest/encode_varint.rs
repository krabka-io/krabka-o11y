pub(crate) fn encode_varint(mut value: u64, body: &mut Vec<u8>) {
    while value >= 0x80 {
        // `|` against `^` is a permanent mutation survivor here: the masked
        // byte has its top bit clear, so setting it and flipping it agree.
        body.push(u8::try_from(value & 0x7f).expect("masked varint byte fits in u8") | 0x80);
        value >>= 7;
    }
    body.push(u8::try_from(value).expect("final varint byte fits in u8"));
}
