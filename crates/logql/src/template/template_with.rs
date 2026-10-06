use super::{TemplateControlExpression, TemplatePart};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateWith {
    pub(crate) expression: TemplateControlExpression,
    pub(crate) parts: Vec<TemplatePart>,
    pub(crate) else_parts: Vec<TemplatePart>,
}
