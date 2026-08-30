use super::*;

pub(crate) fn split_sample_tokens<'a>(src: &'a str, line: Line<'_>) -> Result<Vec<&'a str>> {
    let mut tokens = Vec::new();
    let mut index = 0;
    let bytes = src.as_bytes();

    loop {
        while index < src.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index == src.len() {
            break;
        }
        let start = index;
        if src[start..].starts_with("{{") {
            let Some(relative_end) = src[start + 2..].find("}}") else {
                return Err(parse_error(line, "unterminated native histogram literal"));
            };
            index = start + 2 + relative_end + 2;
            if src[index..].starts_with("+{{") {
                let second_start = index + 1;
                let Some(relative_end) = src[second_start + 2..].find("}}") else {
                    return Err(parse_error(line, "unterminated native histogram literal"));
                };
                index = second_start + 2 + relative_end + 2;
            }
            if index < src.len() && bytes[index] == b'x' {
                index += 1;
                while index < src.len() && bytes[index].is_ascii_digit() {
                    index += 1;
                }
            }
        } else {
            while index < src.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
        }
        tokens.push(&src[start..index]);
    }

    Ok(tokens)
}
