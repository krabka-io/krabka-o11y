use super::*;

pub(crate) fn format_loki_decimal_unit(
    duration_ns: i64,
    unit_ns: i64,
    width: usize,
    suffix: &str,
) -> String {
    let whole = duration_ns / unit_ns;
    let fractional_ns = duration_ns % unit_ns;
    if fractional_ns == 0 {
        return format!("{whole}{suffix}");
    }

    let mut fraction = format!("{fractional_ns:0width$}");
    while fraction.ends_with('0') {
        fraction.pop();
    }
    format!("{whole}.{fraction}{suffix}")
}
