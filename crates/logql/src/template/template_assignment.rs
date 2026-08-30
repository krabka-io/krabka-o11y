use super::TemplateExpression;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateAssignment {
    pub(crate) variable: String,
    pub(crate) expression: TemplateExpression,
}
