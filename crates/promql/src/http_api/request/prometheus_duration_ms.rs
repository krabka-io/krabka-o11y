use super::*;

pub(crate) fn prometheus_duration_ms(value: &str) -> Option<i64> {
    let mut total_ms = 0_i64;
    let mut index = 0;
    let bytes = value.as_bytes();

    while index < bytes.len() {
        let amount_start = index;
        while index < bytes.len() && bytes[index].is_ascii_digit() {
            index += 1;
        }
        if amount_start == index {
            return None;
        }
        let amount = value[amount_start..index].parse::<i64>().ok()?;

        let unit_start = index;
        while index < bytes.len() && bytes[index].is_ascii_alphabetic() {
            index += 1;
        }
        let unit = &value[unit_start..index];
        let multiplier = match unit {
            "ms" => 1,
            "s" => 1_000,
            "m" => 60_000,
            "h" => 3_600_000,
            "d" => 86_400_000,
            "w" => 604_800_000,
            "y" => 31_536_000_000,
            _ => return None,
        };
        total_ms = total_ms.checked_add(amount.checked_mul(multiplier)?)?;
    }

    Some(total_ms)
}
