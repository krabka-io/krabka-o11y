use num_traits::ToPrimitive;

use super::format_template_float;

pub(crate) fn format_template_duration_seconds(value: &str) -> String {
    parse_template_duration(value).map_or_else(String::new, |nanos| {
        format_template_float(nanos.to_f64().expect("i64 fits finite f64") / 1_000_000_000.0)
    })
}

// time.ParseDuration accepts signed, repeated, unordered Go units, and checks
// the sum in nanoseconds before returning the signed int64 duration.
pub(crate) fn parse_template_duration(value: &str) -> Option<i64> {
    let negative = value.starts_with('-');
    let mut remaining = value.strip_prefix(['-', '+']).unwrap_or(value);
    if remaining == "0" {
        return Some(0);
    }
    if remaining.is_empty() {
        return None;
    }
    let limit = 1_u64 << 63;
    let mut total = 0_u64;
    while !remaining.is_empty() {
        let digits = remaining.bytes().take_while(u8::is_ascii_digit).count();
        let whole = if digits == 0 {
            0
        } else {
            remaining[..digits].parse::<u64>().ok()?
        };
        remaining = &remaining[digits..];
        let mut fraction = 0_u64;
        let mut scale = 1.0;
        let mut fractional_digits = 0;
        if let Some(rest) = remaining.strip_prefix('.') {
            fractional_digits = rest.bytes().take_while(u8::is_ascii_digit).count();
            // Go stops accumulating fractional precision at the int64 limit.
            let mut overflow = false;
            for byte in rest[..fractional_digits].bytes() {
                if !overflow {
                    if fraction > (limit - 1) / 10 {
                        overflow = true;
                        continue;
                    }
                    let next = fraction * 10 + u64::from(byte - b'0');
                    if next > limit {
                        overflow = true;
                    } else {
                        fraction = next;
                        scale *= 10.0;
                    }
                }
            }
            remaining = &rest[fractional_digits..];
        }
        if digits == 0 && fractional_digits == 0 {
            return None;
        }
        let unit_end = remaining
            .find(|ch: char| ch == '.' || ch.is_ascii_digit())
            .unwrap_or(remaining.len());
        let unit = match &remaining[..unit_end] {
            "ns" => 1,
            "us" | "µs" | "μs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return None,
        };
        remaining = &remaining[unit_end..];
        let nanos = whole
            .checked_mul(unit)?
            .checked_add((fraction.to_f64()? * (unit.to_f64()? / scale)).to_u64()?)?;
        total = total.checked_add(nanos)?;
        if total > limit {
            return None;
        }
    }
    if negative {
        Some(i64::from_ne_bytes(total.to_ne_bytes()).wrapping_neg())
    } else {
        i64::try_from(total).ok()
    }
}
