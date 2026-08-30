use super::{Line, Result, parse_error};

pub(crate) fn split_metric_and_tail<'a>(src: &'a str, line: Line<'_>) -> Result<(&'a str, &'a str)> {
    let mut brace_depth = 0_u32;
    let mut in_quotes = false;
    let mut escaped = false;

    for (index, ch) in src.char_indices() {
        if escaped {
            escaped = false;
            continue;
        }
        match ch {
            '\\' if in_quotes => escaped = true,
            '"' => in_quotes = !in_quotes,
            '{' if !in_quotes => brace_depth += 1,
            '}' if !in_quotes => {
                brace_depth = brace_depth.saturating_sub(1);
            }
            _ if ch.is_whitespace() && !in_quotes && brace_depth == 0 => {
                let (metric, tail) = src.split_at(index);
                return Ok((metric, tail.trim()));
            }
            _ => {}
        }
    }

    Err(parse_error(
        line,
        "expected metric followed by whitespace-separated fields",
    ))
}
