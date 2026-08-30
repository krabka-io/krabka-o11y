
pub(crate) fn unix_to_template_timestamp(epoch: &str) -> String {
    let Ok(value) = epoch.parse::<i128>() else {
        return String::new();
    };
    let nanos = match epoch.len() {
        5 => value.checked_mul(86_400_000_000_000),
        10 => value.checked_mul(1_000_000_000),
        13 => value.checked_mul(1_000_000),
        16 => value.checked_mul(1_000),
        19 => Some(value),
        _ => None,
    };
    nanos.map_or_else(String::new, |value| value.to_string())
}
