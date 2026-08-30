use super::*;

pub(crate) fn parse_seconds_to_ns(value: &str) -> Option<i64> {
    let (negative, value) = value
        .strip_prefix('-')
        .map_or((false, value), |rest| (true, rest));
    let (whole, fraction) = value.split_once('.').unwrap_or((value, ""));
    if whole.is_empty()
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || !fraction.bytes().all(|b| b.is_ascii_digit())
        || fraction.len() > 9
    {
        return None;
    }
    let whole_ns = whole.parse::<i64>().ok()?.checked_mul(1_000_000_000)?;
    let fraction_ns = if fraction.is_empty() {
        0
    } else {
        format!("{fraction:0<9}").parse::<i64>().ok()?
    };
    let ns = whole_ns.checked_add(fraction_ns)?;
    if negative { ns.checked_neg() } else { Some(ns) }
}
