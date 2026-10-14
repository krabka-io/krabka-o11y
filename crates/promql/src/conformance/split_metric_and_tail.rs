use super::{HeadTail, Line, Result, parse_error};

pub(crate) fn split_metric_and_tail<'a>(src: &'a str, line: Line<'_>) -> Result<HeadTail<'a>> {
    let mut brace_depth = 0_u32;
    let mut in_quotes = false;
    let mut escaped = false;

    for entry in src.char_indices() {
        let index = entry.0;
        let ch = entry.1;
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
                return Ok(HeadTail {
                    head: &src[..index],
                    tail: src[index..].trim(),
                });
            }
            _ => {}
        }
    }

    Err(parse_error(
        line,
        "expected metric followed by whitespace-separated fields",
    ))
}
