use super::{Line, Result, parse_error};

pub(crate) fn split_once_whitespace<'a>(
    src: &'a str,
    line: Line<'_>,
) -> Result<(&'a str, &'a str)> {
    let Some(index) = src.find(char::is_whitespace) else {
        return Err(parse_error(line, "expected whitespace-separated fields"));
    };
    let (head, tail) = src.split_at(index);
    Ok((head, tail.trim()))
}
