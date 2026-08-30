pub(crate) fn write_uvarint(out: &mut Vec<u8>, mut value: u64) {
    for _ in 0..10 {
        if value < 0x80 {
            out.push(u8::try_from(value).expect("terminal uvarint byte fits in u8"));
            return;
        }
        let low_bits = u8::try_from(value & 0x7f).expect("masked uvarint byte fits in u8");
        out.push(low_bits + 0x80);
        value >>= 7;
    }
    unreachable!("u64 uvarint uses at most 10 bytes");
}
