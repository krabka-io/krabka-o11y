use super::{ByteSize, ByteSizeExt, Limits};

pub(crate) fn truncate_loki_line(line: &mut String, limits: &Limits) {
    if !limits.max_line_size_truncate || limits.max_line_size <= ByteSize::ZERO {
        return;
    }
    let mut end = limits.max_line_size.bytes_usize().min(line.len());
    while !line.is_char_boundary(end) {
        end -= 1;
    }
    line.truncate(end);
}

#[cfg(test)]
mod tests {
    use krabka_units::bytes;

    use super::*;

    #[test]
    fn truncation_keeps_a_utf8_boundary() {
        let mut line = "abéz".to_string();
        truncate_loki_line(
            &mut line,
            &Limits {
                max_line_size: bytes(3),
                max_line_size_truncate: true,
                ..Limits::unenforced()
            },
        );
        assert_eq!(line, "ab");
    }
}
