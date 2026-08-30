use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplatePart {
    Literal(String),
    Comment,
    Expression(TemplateExpression),
    Conditional(TemplateConditional),
    Range(TemplateRange),
    With(TemplateWith),
    Assignment(TemplateAssignment),
}
