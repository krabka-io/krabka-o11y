/// Match Go time.Duration.String used by Tempo Static.AsAnyValue.
pub(crate) fn metric_duration_label(nanos: i64) -> String {
    if nanos == 0 {
        return "0s".into();
    }
    let magnitude = nanos.unsigned_abs();
    let sign = if nanos < 0 { "-" } else { "" };
    for (bound, divisor, unit, precision) in [
        (1_000, 1, "ns", 0),
        (1_000_000, 1_000, "µs", 3),
        (1_000_000_000, 1_000_000, "ms", 6),
    ] {
        if magnitude < bound {
            return format!(
                "{sign}{}{unit}",
                decimal_duration(magnitude, divisor, precision)
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
    use assert2::assert;

    use super::*;
    #[test]
    fn duration_labels_preserve_units_fractional_parts_and_sign() {
        for (nanos, expected) in [
            (0, "0s"),
            (5, "5ns"),
            (1_001, "1.001µs"),
            (1_500_000, "1.5ms"),
            (1_000_000_001, "1.000000001s"),
            (-61_500_000_000, "-1m1.5s"),
            (3_600_000_000_000, "1h0m0s"),
            (i64::MIN, "-2562047h47m16.854775808s"),
        ] {
            assert!(metric_duration_label(nanos) == expected);
        }
    }
}
