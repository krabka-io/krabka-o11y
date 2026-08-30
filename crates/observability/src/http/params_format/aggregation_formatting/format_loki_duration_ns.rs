use super::*;

pub(crate) fn format_loki_duration_ns(duration_ns: i64) -> Option<String> {
    if duration_ns < 0 {
        return None;
    }
    if duration_ns == 0 {
        return Some("0s".to_string());
    }

    let mut remaining = duration_ns;
    let mut formatted = String::new();
    for (unit_ns, suffix) in [
        (3_600_000_000_000_i64, "h"),
        (60_000_000_000_i64, "m"),
        (1_000_000_000_i64, "s"),
        (1_000_000_i64, "ms"),
        (1_000_i64, "us"),
        (1_i64, "ns"),
    ] {
        if remaining >= unit_ns {
            let value = remaining / unit_ns;
            remaining %= unit_ns;
            write!(formatted, "{value}{suffix}").expect("writing to a String cannot fail");
        }
    }
    Some(formatted)
}
