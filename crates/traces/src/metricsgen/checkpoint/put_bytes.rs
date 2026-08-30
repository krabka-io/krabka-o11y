use super::*;

pub(crate) fn put_bytes(buf: &mut BytesMut, bytes: &[u8]) {
    let len = u32::try_from(bytes.len()).expect("checkpoint key segment too long");
    buf.put_u32(len);
    buf.put_slice(bytes);
}
