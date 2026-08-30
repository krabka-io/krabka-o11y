use super::*;

pub(crate) fn parse_template_float(value: &str) -> String {
    value
        .parse::<f64>()
        .ok()
        .filter(|value| value.is_finite())
        .map_or_else(String::new, format_template_float)
}
