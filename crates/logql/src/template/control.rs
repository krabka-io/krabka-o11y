use super::{
    ParseError, ParsedTemplateValue, TemplateControlExpression, TemplatePart, TemplateWith,
    ensure_template_parenthesized_token, ensure_template_quoted_token, is_template_comment_action,
    parse_template_action, parse_template_control_body, parse_template_else_body,
    parse_template_parenthesized_token, parse_template_quoted_token, template_parse_error,
    template_quote_scanner::TemplateQuoteScanner, trim_template_body_end,
};

// Find Template Control Action
pub(crate) struct FoundTemplateControlAction<'a> {
    pub(crate) body_end: usize,
    pub(crate) expression: &'a str,
    pub(crate) next_position: usize,
}

pub(crate) fn find_template_control_action(
    template: &str,
    mut pos: usize,
) -> Result<Option<FoundTemplateControlAction<'_>>, ParseError> {
    let body_start = pos;
    let mut nested_controls = Vec::new();
    loop {
        let Some(rest) = template.get(pos..) else {
            return Ok(None);
        };
        if rest.is_empty() {
            return Ok(None);
        }
        let Some(open_offset) = rest.find("{{") else {
            return Ok(None);
        };
        let open = pos
            .checked_add(open_offset)
            .expect("template action offset cannot overflow");
        let action = parse_template_action(template, open)?;
        let expression = action.expression;
        if is_template_comment_action(expression) {
            pos = action.next_pos;
            continue;
        }
        match template_control_action(expression) {
            TemplateControlAction::If
            | TemplateControlAction::Range
            | TemplateControlAction::With
            | TemplateControlAction::Definition => {
                nested_controls.push(());
            }
            TemplateControlAction::End => {
                if nested_controls.is_empty() {
                    let body_end = if action.trim_left {
                        trim_template_body_end(template, body_start, open)
                    } else {
                        open
                    };
                    return Ok(Some(FoundTemplateControlAction {
                        body_end,
                        expression,
                        next_position: action.next_pos,
                    }));
                }
                nested_controls.pop();
            }
            TemplateControlAction::Else
            | TemplateControlAction::ElseIf
            | TemplateControlAction::ElseWith
                if nested_controls.is_empty() =>
            {
                let body_end = if action.trim_left {
                    trim_template_body_end(template, body_start, open)
                } else {
                    open
                };
                return Ok(Some(FoundTemplateControlAction {
                    body_end,
                    expression,
                    next_position: action.next_pos,
                }));
            }
            _ => {}
        }
        pos = action.next_pos;
    }
}

// Parse Template With
pub(crate) fn parse_template_with(
    template: &str,
    body_start: usize,
    with_expression: &str,
) -> Result<ParsedTemplateValue<TemplateWith>, ParseError> {
    let expression = TemplateControlExpression::parse(with_expression.trim())?;
    let control = parse_template_control_body(template, body_start)?;
    if control.expression == "end" {
        return Ok(ParsedTemplateValue {
            value: TemplateWith {
                expression,
                parts: control.parts,
                else_parts: Vec::new(),
            },
            next_position: control.next_position,
        });
    }
    if control.expression != "else" {
        if let Some(with_expression) = control.expression.strip_prefix("else with ") {
            let parsed_with =
                parse_template_with(template, control.next_position, with_expression)?;
            return Ok(ParsedTemplateValue {
                value: TemplateWith {
                    expression,
                    parts: control.parts,
                    else_parts: vec![TemplatePart::With(parsed_with.value)],
                },
                next_position: parsed_with.next_position,
            });
        }
        return Err(template_parse_error("unexpected template control action"));
    }

    let parsed_else = parse_template_else_body(template, control.next_position)?;
    Ok(ParsedTemplateValue {
        value: TemplateWith {
            expression,
            parts: control.parts,
            else_parts: parsed_else.value,
        },
        next_position: parsed_else.next_position,
    })
}

// Split Template Pipeline
pub(crate) fn split_template_pipeline(expression: &str) -> Result<Vec<&str>, ParseError> {
    let mut commands = Vec::new();
    let mut start = 0;
    let mut quotes = TemplateQuoteScanner::default();
    let mut depth = 0usize;
    let mut index = 0;
    while index < expression.len() {
        let ch = expression[index..]
            .chars()
            .next()
            .expect("index is within expression");
        if quotes.consume_quoted(ch) {
            index += ch.len_utf8();
            continue;
        }
        if ch == '(' {
            depth += 1;
        } else if ch == ')' {
            depth = depth
                .checked_sub(1)
                .ok_or_else(|| template_parse_error("unexpected template parenthesis"))?;
        } else if ch == '|' && depth == 0 {
            let command = expression[start..index].trim();
            if command.is_empty() {
                return Err(template_parse_error("expected template command"));
            }
            commands.push(command);
            start = index + ch.len_utf8();
        }
        index += ch.len_utf8();
    }
    if quotes.is_inside_quote() {
        return Err(template_parse_error("unterminated template string"));
    }
    if depth != 0 {
        return Err(template_parse_error("unterminated template parenthesis"));
    }
    let command = expression[start..].trim();
    if command.is_empty() {
        return Err(template_parse_error("expected template command"));
    }
    commands.push(command);
    Ok(commands)
}

// Template Control Action
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TemplateControlAction {
    If,
    Range,
    With,
    Definition,
    Else,
    ElseIf,
    ElseWith,
    End,
    Other,
}

pub(crate) fn template_control_action(expression: &str) -> TemplateControlAction {
    match expression {
        "else" => TemplateControlAction::Else,
        "end" => TemplateControlAction::End,
        _ if expression.starts_with("if ") => TemplateControlAction::If,
        _ if expression.starts_with("range ") => TemplateControlAction::Range,
        _ if expression.starts_with("define ") || expression.starts_with("block ") => {
            TemplateControlAction::Definition
        }
        _ if expression.starts_with("with ") => TemplateControlAction::With,
        _ if expression.starts_with("else if ") => TemplateControlAction::ElseIf,
        _ if expression.starts_with("else with ") => TemplateControlAction::ElseWith,
        _ => TemplateControlAction::Other,
    }
}

// Tokenize Template Command
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
