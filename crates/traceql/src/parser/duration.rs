use super::{Result, TraceqlError};
use crate::next_duration_part;

// Parse Duration Component Nanos
pub(crate) fn parse_duration_component_nanos(
    number: &str,
    multiplier: i128,
    original: &str,
) -> Result<i128> {
    let decimal_parts = number.split_once('.');
    let whole = decimal_parts.map_or(number, |parts| parts.0);
    let fraction = decimal_parts.map_or("", |parts| parts.1);
    if whole.is_empty() && fraction.is_empty() {
        return Err(TraceqlError::Parse(format!(
            "invalid duration number {number:?}"
        )));
    }
    if fraction.contains('.') {
        return Err(TraceqlError::Parse(format!(
            "invalid duration number {number:?}"
        )));
    }

    let whole = if whole.is_empty() {
        0
    } else {
        whole
            .parse::<i128>()
            .map_err(|e| TraceqlError::Parse(e.to_string()))?
    };
    let whole_ns = whole
        .checked_mul(multiplier)
        .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {original:?}")))?;
    if fraction.is_empty() {
        return Ok(whole_ns);
    }

    let fraction_digits = fraction
        .parse::<i128>()
        .map_err(|e| TraceqlError::Parse(e.to_string()))?;
    let scale = 10_i128
        .checked_pow(u32::try_from(fraction.len()).map_err(|e| TraceqlError::Parse(e.to_string()))?)
        .ok_or_else(|| {
            TraceqlError::Parse(format!("duration precision too large: {original:?}"))
        })?;
    let fraction_ns = fraction_digits
        .checked_mul(multiplier)
        .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {original:?}")))?
        / scale;

    whole_ns
        .checked_add(fraction_ns)
        .ok_or_else(|| TraceqlError::Parse(format!("duration out of range: {original:?}")))
}

// Parse Duration Nanos

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
