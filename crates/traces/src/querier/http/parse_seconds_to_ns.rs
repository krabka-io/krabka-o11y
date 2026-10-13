pub(crate) fn parse_seconds_to_ns(value: &str) -> Option<i64> {
    let negative = value.starts_with('-');
    let value = value.strip_prefix('-').unwrap_or(value);
    let separator = value.find('.');
    let whole = separator.map_or(value, |index| &value[..index]);
    let fraction = separator.map_or("", |index| &value[index + 1..]);
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
        let padded = format!("{fraction:0<9}");
        padded.parse::<i64>().ok()?
    };
    let ns = whole_ns.checked_add(fraction_ns)?;
    if negative { ns.checked_neg() } else { Some(ns) }
}
