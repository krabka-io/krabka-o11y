use super::prometheus_duration_unit;

/// Parses a Prometheus duration literal such as `1h30m` into nanoseconds.
///
/// This follows Prometheus `model.ParseDuration`: one or more `<amount><unit>`
/// chunks, units strictly from larger to smaller, and no sign, fraction or
/// whitespace. Returns `None` for an empty, malformed, out-of-order or
/// overflowing literal, so each caller keeps its own upstream error message.
#[must_use]
pub fn parse_prometheus_duration_literal(value: &str) -> Option<i64> {
    let mut rest = value;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;
    let mut total_ns = 0_i128;

    while !rest.is_empty() {
        let amount_len = rest.bytes().take_while(u8::is_ascii_digit).count();
        if amount_len == 0 {
            return None;
        }
        let amount = rest.get(..amount_len)?.parse::<i128>().ok()?;
        rest = rest.get(amount_len..)?;

        let unit_len = rest.bytes().take_while(u8::is_ascii_alphabetic).count();
        let (unit_order, _unit_bit, multiplier) = prometheus_duration_unit(rest.get(..unit_len)?)?;
        rest = rest.get(unit_len..)?;
        if previous_unit_order.is_some_and(|previous| unit_order <= previous) {
            return None;
        }

        let chunk_ns = amount.checked_mul(multiplier)?;
        total_ns = total_ns.checked_add(chunk_ns)?;
        previous_unit_order = Some(unit_order);
        parsed_chunk = true;
    }

    if !parsed_chunk {
        return None;
    }
    i64::try_from(total_ns).ok()
}
