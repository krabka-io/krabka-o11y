use super::{format_loki_decimal_unit, format_loki_offset_seconds};

pub(crate) fn format_loki_offset_duration_ns(duration_ns: i64) -> Option<String> {
    const HOUR_NS: i64 = 3_600_000_000_000;
    const MINUTE_NS: i64 = 60_000_000_000;
    const SECOND_NS: i64 = 1_000_000_000;
    const MILLISECOND_NS: i64 = 1_000_000;
    const MICROSECOND_NS: i64 = 1_000;

    if duration_ns < 0 {
        return None;
    }
    if duration_ns == 0 {
        return Some("0s".to_string());
    }

    let mut remaining = duration_ns;
    let hours = remaining / HOUR_NS;
    remaining %= HOUR_NS;
    let minutes = remaining / MINUTE_NS;
    remaining %= MINUTE_NS;

    if hours > 0 {
        return Some(format!(
            "{hours}h{minutes}m{}",
            format_loki_offset_seconds(remaining)
        ));
    }
    if minutes > 0 {
        return Some(format!(
            "{minutes}m{}",
            format_loki_offset_seconds(remaining)
        ));
    }
    if remaining >= SECOND_NS {
        return Some(format_loki_offset_seconds(remaining));
    }
    if remaining >= MILLISECOND_NS {
        return Some(format_loki_decimal_unit(remaining, MILLISECOND_NS, 6, "ms"));
    }
    if remaining >= MICROSECOND_NS {
        return Some(format_loki_decimal_unit(
            remaining,
            MICROSECOND_NS,
            3,
            "\u{00b5}s",
        ));
    }
    Some(format!("{remaining}ns"))
}
