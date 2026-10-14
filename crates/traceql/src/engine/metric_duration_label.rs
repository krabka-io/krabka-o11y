/// Match Go time.Duration.String used by Tempo Static.AsAnyValue.
pub(crate) fn metric_duration_label(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".into();
    }
    let magnitude = nanos.unsigned_abs();
    let sign = if nanos < 0 { "-" } else { "" };
    for unit in [
        DurationUnit {
            bound: 1_000,
            divisor: 1,
            suffix: "ns",
            precision: 0,
        },
        DurationUnit {
            bound: 1_000_000,
            divisor: 1_000,
            suffix: "µs",
            precision: 3,
        },
        DurationUnit {
            bound: 1_000_000_000,
            divisor: 1_000_000,
            suffix: "ms",
            precision: 6,
        },
    ] {
        if magnitude < unit.bound {
            return format!(
                "{sign}{}{suffix}",
                decimal_duration(magnitude, unit.divisor, unit.precision),
                suffix = unit.suffix,
            );
        }
    }
    let hours = magnitude / 3_600_000_000_000;
    let remainder = magnitude % 3_600_000_000_000;
    let minutes = remainder / 60_000_000_000;
    let seconds = decimal_duration(remainder % 60_000_000_000, 1_000_000_000, 9);
    if hours > 0 {
        format!("{sign}{hours}h{minutes}m{seconds}s")
    } else if minutes > 0 {
        format!("{sign}{minutes}m{seconds}s")
    } else {
        format!("{sign}{seconds}s")
    }
}

struct DurationUnit {
    bound: u64,
    divisor: u64,
    suffix: &'static str,
    precision: usize,
}

fn decimal_duration(value: u64, divisor: u64, precision: usize) -> String {
    let integral = value / divisor;
    let remainder = value % divisor;
    if remainder == 0 {
        return integral.to_string();
    }
    let fraction = format!("{remainder:0precision$}");
    format!("{integral}.{}", fraction.trim_end_matches('0'))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[yare::parameterized(
        zero = { 0, "0s" },
        nanoseconds = { 5, "5ns" },
        fractional_microseconds = { 1_001, "1.001µs" },
        fractional_milliseconds = { 1_500_000, "1.5ms" },
        fractional_seconds = { 1_000_000_001, "1.000000001s" },
        negative = { -61_500_000_000, "-1m1.5s" },
        hours = { 3_600_000_000_000, "1h0m0s" },
        min_i64 = { i64::MIN, "-2562047h47m16.854775808s" },
    )]
    fn duration_labels_preserve_units_fractional_parts_and_sign(nanos: i64, expected: &str) {
        ::core::assert_eq!(metric_duration_label(nanos), expected);
    }
}
