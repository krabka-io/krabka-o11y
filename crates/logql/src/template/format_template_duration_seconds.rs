use super::*;

pub(crate) fn format_template_duration_seconds(value: &str) -> String {
    let Some(duration_ns) = parse_prometheus_duration_literal(value) else {
        return String::new();
    };
    let Ok(duration_ns) = u128::try_from(duration_ns) else {
        return String::new();
    };
    format_decimal_ratio(duration_ns, 1_000_000_000)
}
