use super::*;

pub(crate) fn is_unexpected_template_control_action(expression: &str) -> bool {
    matches!(
        template_control_action(expression),
        TemplateControlAction::Else
            | TemplateControlAction::ElseIf
            | TemplateControlAction::ElseWith
            | TemplateControlAction::End
    )
}
