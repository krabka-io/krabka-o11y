pub(crate) fn append_len_prefixed(bytes: &mut Vec<u8>, value: &str) {
    bytes.extend_from_slice(value.len().to_string().as_bytes());
    bytes.push(b':');
    bytes.extend_from_slice(value.as_bytes());
    bytes.push(0);
}

#[cfg(test)]
mod tests {
    use super::append_len_prefixed;

    #[test]
    fn canonical_bytes_preserve_decimal_byte_lengths_and_existing_prefixes() {
        let mut actual = b"prefix".to_vec();
        for value in ["", "x", "=\n\0", "é", "🦀", "0123456789"] {
            append_len_prefixed(&mut actual, value);
        }
        assert2::assert!(
            actual.as_slice()
                == b"prefix0:\0\
                     1:x\0\
                     3:=\n\0\0\
                     2:\xc3\xa9\0\
                     4:\xf0\x9f\xa6\x80\0\
                     10:0123456789\0"
        );
    }
}
