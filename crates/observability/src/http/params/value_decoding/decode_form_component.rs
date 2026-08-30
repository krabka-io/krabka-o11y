use super::*;

pub(crate) fn decode_form_component(component: &str) -> Result<String, HttpQueryError> {
    let mut bytes = Vec::with_capacity(component.len());
    let mut iter = component.as_bytes().iter().copied();
    while let Some(byte) = iter.next() {
        match byte {
            b'+' => bytes.push(b' '),
            b'%' => {
                let high = iter
                    .next()
                    .and_then(hex_value)
                    .ok_or(HttpQueryError::InvalidPercentEncoding)?;
                let low = iter
                    .next()
                    .and_then(hex_value)
                    .ok_or(HttpQueryError::InvalidPercentEncoding)?;
                bytes.push(high << 4 | low);
            }
            _ => bytes.push(byte),
        }
    }

    String::from_utf8(bytes).map_err(|_| HttpQueryError::InvalidPercentEncoding)
}
