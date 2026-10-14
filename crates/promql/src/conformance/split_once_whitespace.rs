use super::{HeadTail, Line, Result, parse_error};

pub(crate) fn split_once_whitespace<'a>(src: &'a str, line: Line<'_>) -> Result<HeadTail<'a>> {
    let Some(index) = src.find(char::is_whitespace) else {
        return Err(parse_error(line, "expected whitespace-separated fields"));
    };
    Ok(HeadTail {
        head: &src[..index],
        tail: src[index..].trim(),
    })
}
