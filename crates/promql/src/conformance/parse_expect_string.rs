use super::{Line, Result, parse_error};

pub(crate) fn parse_expect_string(src: &str, line: Line<'_>) -> Result<String> {
    let src = src.trim();
    if let Some(value) = src
        .strip_prefix('`')
        .and_then(|value| value.strip_suffix('`'))
    {
        return Ok(value.to_string());
    }
    let value = src
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .ok_or_else(|| parse_error(line, "expected quoted string assertion"))?;
    Ok(value.replace("\\\"", "\"").replace("\\\\", "\\"))
}
