use super::{
    ParseError, TemplateControlAction, is_template_comment_action, parse_template_action,
    template_control_action, trim_template_body_end,
};

pub(crate) fn find_template_control_action(
    template: &str,
    mut pos: usize,
) -> Result<Option<(usize, &str, usize)>, ParseError> {
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
            | TemplateControlAction::With => {
                nested_controls.push(());
            }
            TemplateControlAction::End => {
                if nested_controls.is_empty() {
                    let body_end = if action.trim_left {
                        trim_template_body_end(template, body_start, open)
                    } else {
                        open
                    };
                    return Ok(Some((body_end, expression, action.next_pos)));
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
                return Ok(Some((body_end, expression, action.next_pos)));
            }
            _ => {}
        }
        pos = action.next_pos;
    }
}
