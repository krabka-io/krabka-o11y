use super::{
    ParseError, ensure_template_parenthesized_token, ensure_template_quoted_token,
    parse_template_parenthesized_token, parse_template_quoted_token,
};

pub(crate) fn tokenize_template_command(command: &str) -> Result<Vec<String>, ParseError> {
    let mut tokens = Vec::new();
    let mut pos = 0;
    while let Some(rest) = command.get(pos..) {
        let Some(positioned_char) = rest
            .char_indices()
            .find(|candidate| !candidate.1.is_whitespace())
        else {
            break;
        };
        let offset = positioned_char.0;
        let ch = positioned_char.1;
        pos = pos
            .checked_add(offset)
            .expect("template token offset cannot overflow");
        if matches!(ch, '"' | '\'' | '`') {
            let parsed = parse_template_quoted_token(command, pos, ch)?;
            ensure_template_quoted_token(command, pos, &parsed.token, parsed.next_position, ch)?;
            tokens.push(parsed.token);
            pos = parsed.next_position;
        } else if ch == '(' {
            let parsed = parse_template_parenthesized_token(command, pos)?;
            ensure_template_parenthesized_token(command, pos, &parsed.token, parsed.next_position)?;
            let end = if command.as_bytes().get(parsed.next_position) == Some(&b'.') {
                command[parsed.next_position..]
                    .find(char::is_whitespace)
                    .map_or(command.len(), |offset| parsed.next_position + offset)
            } else {
                parsed.next_position
            };
            tokens.push(format!(
                "{}{}",
                parsed.token,
                &command[parsed.next_position..end]
            ));
            pos = end;
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
