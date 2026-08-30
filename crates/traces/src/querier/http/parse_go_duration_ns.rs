use super::*;

pub(crate) fn parse_go_duration_ns(value: &str) -> Result<u64, String> {
    if value.is_empty() {
        return Err("empty duration".into());
    }

    let mut total = 0_u128;
    let mut rest = value;
    while !rest.is_empty() {
        let number_len = rest
            .char_indices()
            .take_while(|(_, c)| c.is_ascii_digit() || *c == '.')
            .map(|(idx, c)| idx + c.len_utf8())
            .last()
            .ok_or_else(|| format!("expected number in {value:?}"))?;
        let (number, tail) = rest.split_at(number_len);
        let unit_len = tail
            .char_indices()
            .take_while(|(_, c)| c.is_ascii_alphabetic() || *c == 'µ')
            .map(|(idx, c)| idx + c.len_utf8())
            .last()
            .ok_or_else(|| format!("expected unit after {number:?}"))?;
        let (unit, next) = tail.split_at(unit_len);
        let multiplier = match unit {
            "ns" => 1,
            "us" | "µs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            _ => return Err(format!("unsupported unit {unit:?}")),
        };
        total = total
            .checked_add(parse_duration_component_ns(number, multiplier)?)
            .ok_or_else(|| "duration out of range".to_string())?;
        rest = next;
    }

    u64::try_from(total).map_err(|_| "duration out of range".into())
}
