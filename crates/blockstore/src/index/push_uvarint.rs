/// Appends `value` to `out` as an LEB128 unsigned varint.
///
/// Every count, length and ordinal in the index encoding goes through this.
/// The values are almost always small. A block ordinal, a label count and a
/// dictionary id all fit in a byte or two, and a fixed-width field would spend
/// eight bytes on each of them. Seven bits per byte keeps the common ones to
/// one byte.
pub(crate) fn push_uvarint(out: &mut Vec<u8>, value: u64) {
    let mut rest = value;
    while rest >= 0x80 {
        let byte = u8::try_from(rest & 0x7f).expect("seven bits fit a u8");
        out.push(byte | 0x80);
        rest >>= 7;
    }
    out.push(u8::try_from(rest).expect("a value below 0x80 fits a u8"));
}
