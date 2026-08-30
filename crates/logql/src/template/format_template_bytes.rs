use super::*;

pub(crate) fn format_template_bytes(value: &str) -> String {
    let Some(bytes) = parse_bytes_literal(value) else {
        return String::new();
    };
    let bytes = bytes.bytes_f64();
    if bytes.fract() == 0.0 {
        format!("{bytes:.0}")
    } else {
        bytes.to_string()
    }
}
