use super::*;

pub(crate) fn parse_duration_nanos(s: &str) -> Result<i64> {
    if s.is_empty() {
        return Err(TraceqlError::Parse("empty duration".into()));
    }

    let mut total = 0_i128;
    let mut rest = s;
    while !rest.is_empty() {
        let number_len = rest
            .char_indices()
            .take_while(|(_, ch)| ch.is_ascii_digit() || *ch == '.')
            .map(|(idx, ch)| idx + ch.len_utf8())
            .last()
            .ok_or_else(|| TraceqlError::Parse(format!("expected duration number in {s:?}")))?;
        let (number, tail) = rest.split_at(number_len);
        let unit_len = tail
            .char_indices()
            .take_while(|(_, ch)| ch.is_ascii_alphabetic() || *ch == 'µ')
            .map(|(idx, ch)| idx + ch.len_utf8())
            .last()
            .ok_or_else(|| {
                TraceqlError::Parse(format!("missing duration unit after {number:?}"))
            })?;
        let (unit, next) = tail.split_at(unit_len);
        let multiplier = match unit {
            "ns" => 1_i128,
            "us" | "µs" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            other => {
                return Err(TraceqlError::Parse(format!(
                    "unknown duration unit {other:?}"
                )));
            }
        };
        let component = parse_duration_component_nanos(number, multiplier, s)?;
        total = total
            .checked_add(component)
            .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {s:?}")))?;
        rest = next;
    }

    i64::try_from(total).map_err(|e| TraceqlError::Parse(e.to_string()))
}
