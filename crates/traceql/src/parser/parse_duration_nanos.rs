use super::{Result, TraceqlError, parse_duration_component_nanos};
use crate::next_duration_part;

pub(crate) fn parse_duration_nanos(s: &str) -> Result<i64> {
    if s.is_empty() {
        return Err(TraceqlError::Parse("empty duration".into()));
    }

    let mut total = 0_i128;
    let mut rest = s;
    while !rest.is_empty() {
        let part = next_duration_part(rest)
            .ok_or_else(|| TraceqlError::Parse(format!("expected duration number in {s:?}")))?;
        let number = part.number;
        if part.unit.is_empty() {
            return Err(TraceqlError::Parse(format!(
                "missing duration unit after {number:?}"
            )));
        }
        let unit = part.unit;
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
        rest = part.rest;
    }

    i64::try_from(total).map_err(|e| TraceqlError::Parse(e.to_string()))
}
