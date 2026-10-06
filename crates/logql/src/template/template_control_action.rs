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
