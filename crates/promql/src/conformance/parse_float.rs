use super::*;

pub(crate) fn parse_float(src: &str, line: Line<'_>) -> Result<f64> {
    src.parse::<f64>()
        .map_err(|err| parse_error(line, format!("invalid float `{src}`: {err}")))
}
