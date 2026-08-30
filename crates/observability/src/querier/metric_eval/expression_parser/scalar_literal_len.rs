pub(crate) fn scalar_literal_len(input: &str) -> Option<usize> {
    let bytes = input.as_bytes();
    let mut position = 0;
    if matches!(bytes.get(position), Some(b'+' | b'-')) {
        position += 1;
    }

    let whole_start = position;
    while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
        position += 1;
    }
    let whole_digits = position > whole_start;

    let mut fractional_digits = false;
    if matches!(bytes.get(position), Some(b'.')) {
        position += 1;
        let fractional_start = position;
        while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
            position += 1;
        }
        fractional_digits = position > fractional_start;
    }

    if !whole_digits && !fractional_digits {
        return None;
    }

    if matches!(bytes.get(position), Some(b'e' | b'E')) {
        position += 1;
        if matches!(bytes.get(position), Some(b'+' | b'-')) {
            position += 1;
        }
        let exponent_start = position;
        while matches!(bytes.get(position), Some(byte) if byte.is_ascii_digit()) {
            position += 1;
        }
        if position == exponent_start {
            return None;
        }
    }

    Some(position)
}
