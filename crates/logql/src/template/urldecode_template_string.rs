pub(crate) fn urldecode_template_string(value: &str) -> String {
    let mut bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    while let Some((&byte, rest)) = bytes.split_first() {
        if byte == b'%'
            && let Some(hex_bytes) = rest.get(..2)
            && let Ok(hex) = std::str::from_utf8(hex_bytes)
            && let Ok(decoded_byte) = u8::from_str_radix(hex, 16)
        {
            decoded.push(decoded_byte);
            bytes = &rest[2..];
            continue;
        }
        decoded.push(byte);
        bytes = rest;
    }
    String::from_utf8_lossy(&decoded).into_owned()
}
