use super::{TemplateControlExpression, TemplatePart};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateConditional {
    pub(crate) branches: Vec<(TemplateControlExpression, Vec<TemplatePart>)>,
    pub(crate) else_parts: Vec<TemplatePart>,
}
