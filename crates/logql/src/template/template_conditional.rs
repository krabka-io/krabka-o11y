use super::{TemplateControlExpression, TemplatePart};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateConditional {
    pub(crate) branches: Vec<TemplateConditionalBranch>,
    pub(crate) else_parts: Vec<TemplatePart>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateConditionalBranch {
    pub(crate) condition: TemplateControlExpression,
    pub(crate) parts: Vec<TemplatePart>,
}
