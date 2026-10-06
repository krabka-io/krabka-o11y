pub(in crate::template) fn string(value: i64) -> String {
    let magnitude = value.unsigned_abs();
    let sign = if value < 0 { "-" } else { "" };
    if magnitude == 0 {
        return "0s".into();
    }
    if magnitude < 1_000_000_000 {
        let (scale, suffix) = if magnitude < 1000 {
            (1, "ns")
        } else if magnitude < 1_000_000 {
            (1000, "µs")
        } else {
            (1_000_000, "ms")
        };
        return format!("{sign}{}{suffix}", decimal(magnitude, scale));
    }
    let hours = magnitude / 3_600_000_000_000;
    let minutes = magnitude / 60_000_000_000 % 60;
    let seconds = magnitude % 60_000_000_000;
    format!(
        "{sign}{}{}{}s",
        if hours > 0 {
            format!("{hours}h")
        } else {
            String::new()
        },
        if hours > 0 || minutes > 0 {
            format!("{minutes}m")
        } else {
            String::new()
        },
        decimal(seconds, 1_000_000_000)
    )
}
fn decimal(value: u64, scale: u64) -> String {
    let fraction = value % scale;
    if fraction == 0 {
        return (value / scale).to_string();
    }
    let width = usize::try_from(scale.ilog10()).expect("u64 logarithm is at most 19");
    format!("{}.{:0width$}", value / scale, fraction)
        .trim_end_matches('0')
        .to_string()
}
pub(super) fn round(value: i64, multiple: i64) -> i64 {
    if multiple <= 0 {
        return value;
    }
    let remainder = value % multiple;
    let rounded = if remainder.unsigned_abs() * 2 < multiple.unsigned_abs() {
        i128::from(value - remainder)
    } else if value < 0 {
        i128::from(value) - i128::from(multiple) - i128::from(remainder)
    } else {
        i128::from(value) + i128::from(multiple) - i128::from(remainder)
    };
    i64::try_from(rounded.clamp(i128::from(i64::MIN), i128::from(i64::MAX)))
        .expect("clamped duration fits i64")
}
