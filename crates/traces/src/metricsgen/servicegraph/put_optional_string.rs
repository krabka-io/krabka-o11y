use super::{BytesMut, BufMut};

pub(crate) fn put_optional_string(buf: &mut BytesMut, value: Option<&str>) {
    match value {
        Some(value) => {
            buf.put_u8(1);
            let len = u32::try_from(value.len()).expect("service name too long");
            buf.put_u32(len);
            buf.put_slice(value.as_bytes());
        }
        None => buf.put_u8(0),
    }
}
