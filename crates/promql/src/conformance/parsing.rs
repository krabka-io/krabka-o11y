use std::collections::BTreeMap;

use crate::duration_terms::duration_terms_ms;

use super::{Line, Result, parse_error};

pub(crate) fn parse_optional_histogram_f64(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<f64>> {
    fields
        .get(name)
        .map(|value| parse_float(value, line))
        .transpose()
}

pub(crate) fn parse_optional_histogram_i32(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<i32>> {
    fields
        .get(name)
        .map(|value| {
            value.parse::<i32>().map_err(|error| {
                parse_error(line, format!("invalid histogram {name} `{value}`: {error}"))
            })
        })
        .transpose()
}

pub(crate) fn parse_optional_histogram_i8(
    fields: &BTreeMap<&str, &str>,
    name: &str,
    line: Line<'_>,
) -> Result<Option<i8>> {
    fields
        .get(name)
        .map(|value| {
            value.parse::<i8>().map_err(|error| {
                parse_error(line, format!("invalid histogram {name} `{value}`: {error}"))
            })
        })
        .transpose()
}

pub(crate) fn parse_duration_ms(src: &str, line: Line<'_>) -> Result<i64> {
    let src = src.trim();
    if src == "0" {
        return Ok(0);
    }
    duration_terms_ms(src).map_err(|message| parse_error(line, message))
}

pub(crate) fn parse_float(src: &str, line: Line<'_>) -> Result<f64> {
    src.parse::<f64>()
        .map_err(|err| parse_error(line, format!("invalid float `{src}`: {err}")))
}

pub(crate) fn parse_bucket_bound(value: &str, line: Line<'_>) -> Result<f64> {
    match value {
        "+Inf" | "Inf" => Ok(f64::INFINITY),
        "-Inf" => Ok(f64::NEG_INFINITY),
        _ => parse_float(value, line),
    }
}
