use super::{HistogramExpansion, Line, Result, parse_error, parse_histogram_literal};

pub(crate) fn parse_histogram_expansion(
    token: &str,
    line: Line<'_>,
) -> Result<Option<HistogramExpansion>> {
    let Some(separator) = token.rfind('x') else {
        return Ok(None);
    };
    let count = &token[separator + 1..];
    let base = &token[..separator];
    let Some(separator) = base.find("}}+{{") else {
        return Ok(None);
    };
    let start = &base[..separator];
    let step = &base[separator + "}}+{{".len()..];
    let count = count.parse::<u32>().map_err(|err| {
        parse_error(
            line,
            format!("invalid expanding-point count `{count}`: {err}"),
        )
    })?;
    let mut start_literal = start.to_string();
    start_literal.push_str("}}");
    let mut step_literal = "{{".to_string();
    step_literal.push_str(step);
    let start = parse_histogram_literal(&start_literal, line)?;
    let step = parse_histogram_literal(&step_literal, line)?;
    Ok(Some(HistogramExpansion { start, step, count }))
}
