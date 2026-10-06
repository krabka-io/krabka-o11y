use super::format_template_float;

pub(crate) fn parse_template_float(value: &str) -> String {
    super::template_value::number::parse_float(value)
        .map_or_else(|| "0".to_string(), format_template_float)
}
