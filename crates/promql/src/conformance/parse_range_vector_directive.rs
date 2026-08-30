use super::{Line, Result, RangeExpect, split_once_whitespace, parse_error, parse_duration_ms, Time, TimeExt};

pub(crate) fn parse_range_vector_directive(directive: &str, line: Line<'_>) -> Result<Option<RangeExpect>> {
    let Some(rest) = directive.trim().strip_prefix("range vector from ") else {
        return Ok(None);
    };
    let (start, rest) = split_once_whitespace(rest.trim(), line)?;
    let rest = rest
        .strip_prefix("to ")
        .ok_or_else(|| parse_error(line, "expected `to` in expect range vector"))?;
    let (_end, rest) = split_once_whitespace(rest.trim(), line)?;
    let rest = rest
        .strip_prefix("step ")
        .ok_or_else(|| parse_error(line, "expected `step` in expect range vector"))?;
    let step = rest.trim();
    Ok(Some(RangeExpect {
        start_ms: parse_duration_ms(start, line)?,
        step: Time::from_millis(parse_duration_ms(step, line)?),
    }))
}
