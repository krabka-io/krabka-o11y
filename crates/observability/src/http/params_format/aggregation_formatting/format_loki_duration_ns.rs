use super::*;

struct DurationUnit {
    nanoseconds: i64,
    suffix: &'static str,
}

pub(crate) fn format_loki_duration_ns(duration_ns: i64) -> Option<String> {
    if duration_ns < 0 {
        return None;
    }
    if duration_ns == 0 {
        return Some("0s".to_string());
    }

    let mut remaining = duration_ns;
    let mut formatted = String::new();
    for unit in [
        DurationUnit {
            nanoseconds: 3_600_000_000_000_i64,
            suffix: "h",
        },
        DurationUnit {
            nanoseconds: 60_000_000_000_i64,
            suffix: "m",
        },
        DurationUnit {
            nanoseconds: 1_000_000_000_i64,
            suffix: "s",
        },
        DurationUnit {
            nanoseconds: 1_000_000_i64,
            suffix: "ms",
        },
        DurationUnit {
            nanoseconds: 1_000_i64,
            suffix: "us",
        },
        DurationUnit {
            nanoseconds: 1_i64,
            suffix: "ns",
        },
    ] {
        if remaining >= unit.nanoseconds {
            let value = remaining / unit.nanoseconds;
            remaining %= unit.nanoseconds;
            write!(formatted, "{value}{}", unit.suffix).expect("writing to a String cannot fail");
        }
    }
    Some(formatted)
}
