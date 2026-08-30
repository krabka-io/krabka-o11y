use super::parse_scalar_sample;

pub(crate) fn format_scalar_text(scalar: &str) -> Option<String> {
    Some(parse_scalar_sample(scalar)?.format())
}
