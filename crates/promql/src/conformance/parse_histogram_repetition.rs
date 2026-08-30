use super::{Line, Result, NativeHistogram, parse_error, parse_histogram_literal};

pub(crate) fn parse_histogram_repetition(
    token: &str,
    line: Line<'_>,
) -> Result<Option<(NativeHistogram, u32)>> {
    let Some((literal, count)) = token.rsplit_once('x') else {
        return Ok(None);
    };
    if !literal.starts_with("{{") || !literal.ends_with("}}") {
        return Ok(None);
    }
    let count = count.parse::<u32>().map_err(|err| {
        parse_error(
            line,
            format!("invalid expanding-point count `{count}`: {err}"),
        )
    })?;
    Ok(Some((parse_histogram_literal(literal, line)?, count)))
}
