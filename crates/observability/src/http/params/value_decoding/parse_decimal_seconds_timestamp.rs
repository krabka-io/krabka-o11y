pub(crate) fn parse_decimal_seconds_timestamp(value: &str) -> Option<i64> {
    let (negative, unsigned) = match value.as_bytes().first() {
        Some(b'-') => (true, &value[1..]),
        Some(b'+') => (false, &value[1..]),
        _ => (false, value),
    };
    let (seconds, fraction) = unsigned.split_once('.')?;
    if seconds.is_empty() && fraction.is_empty() {
        return None;
    }
    if !seconds.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }

    let seconds = if seconds.is_empty() {
        0
    } else {
        seconds.parse::<i128>().ok()?
    };
    let mut fraction_ns = 0_i128;
    let mut scale = 100_000_000_i128;
    for digit in fraction.bytes().take(9) {
        fraction_ns += i128::from(digit - b'0') * scale;
        scale /= 10;
    }

    let timestamp_ns = seconds
        .checked_mul(1_000_000_000)?
        .checked_add(fraction_ns)?;
    let timestamp_ns = if negative {
        timestamp_ns.checked_neg()?
    } else {
        timestamp_ns
    };
    i64::try_from(timestamp_ns).ok()
}
