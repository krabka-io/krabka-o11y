use super::{BytesMut, BufMut};

pub(crate) fn put_optional_i64(buf: &mut BytesMut, value: Option<i64>) {
    match value {
        Some(value) => {
            buf.put_u8(1);
            buf.put_i64(value);
        }
        None => buf.put_u8(0),
    }
}
