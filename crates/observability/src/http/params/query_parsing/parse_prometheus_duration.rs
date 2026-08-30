use super::prometheus_duration_unit;

pub(crate) fn parse_prometheus_duration(value: &str) -> Option<i64> {
    let mut pos = 0;
    let mut parsed_chunk = false;
    let mut previous_unit_order = None;
    let mut total_ns = 0_i128;

    while pos < value.len() {
        let amount_start = pos;
        while value.as_bytes().get(pos).is_some_and(u8::is_ascii_digit) {
            pos += 1;
        }
        if pos == amount_start {
            return None;
        }
        let amount = value[amount_start..pos].parse::<i128>().ok()?;

        let unit_start = pos;
        while value
            .as_bytes()
            .get(pos)
            .is_some_and(u8::is_ascii_alphabetic)
        {
            pos += 1;
        }
        let (unit_order, _, multiplier) = prometheus_duration_unit(&value[unit_start..pos])?;
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
