use super::*;

pub(crate) fn parse_histogram_expansion(
    token: &str,
    line: Line<'_>,
) -> Result<Option<(NativeHistogram, NativeHistogram, u32)>> {
    let Some((base, count)) = token.rsplit_once('x') else {
        return Ok(None);
    };
    let Some((start, step)) = base.split_once("}}+{{") else {
        return Ok(None);
    };
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
    Ok(Some((start, step, count)))
}
