use super::*;

pub(crate) fn tokenize_template_command(command: &str) -> Result<Vec<String>, ParseError> {
    let mut tokens = Vec::new();
    let mut pos = 0;
    while let Some(rest) = command.get(pos..) {
        let Some((offset, ch)) = rest.char_indices().find(|(_, ch)| !ch.is_whitespace()) else {
            break;
        };
        pos = pos
            .checked_add(offset)
            .expect("template token offset cannot overflow");
        if matches!(ch, '"' | '`') {
            let (token, next) = parse_template_quoted_token(command, pos, ch)?;
            ensure_template_quoted_token(command, pos, &token, next, ch)?;
            tokens.push(token);
            pos = next;
        } else if ch == '(' {
            let (token, next) = parse_template_parenthesized_token(command, pos)?;
            ensure_template_parenthesized_token(command, pos, &token, next)?;
            tokens.push(token);
            pos = next;
        } else {
            let end = command
                .get(pos..)
                .and_then(|rest| rest.find(char::is_whitespace))
                .map_or(command.len(), |offset| {
                    pos.checked_add(offset)
                        .expect("template token end offset cannot overflow")
                });
            tokens.push(command[pos..end].to_string());
            pos = end;
        }
    }
    Ok(tokens)
}
