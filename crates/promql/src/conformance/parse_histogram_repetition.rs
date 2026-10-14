use super::{HistogramRepetition, Line, Result, parse_error, parse_histogram_literal};

pub(crate) fn parse_histogram_repetition(
    token: &str,
    line: Line<'_>,
) -> Result<Option<HistogramRepetition>> {
    let Some(separator) = token.rfind('x') else {
        return Ok(None);
    };
    let literal = &token[..separator];
    let count = &token[separator + 1..];
    if !literal.starts_with("{{") || !literal.ends_with("}}") {
        return Ok(None);
    }
    let count = count.parse::<u32>().map_err(|err| {
        parse_error(
            line,
            format!("invalid expanding-point count `{count}`: {err}"),
        )
    })?;
    Ok(Some(HistogramRepetition {
        histogram: parse_histogram_literal(literal, line)?,
        count,
    }))
}
