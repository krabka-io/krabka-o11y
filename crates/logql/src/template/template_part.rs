use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplatePart {
    Literal(String),
    Comment,
    Definition {
        name: String,
        parts: Vec<TemplatePart>,
        block: bool,
    },
    Invocation {
        name: String,
        argument: Option<TemplateExpression>,
    },
    Break,
    Continue,
    Expression(TemplateExpression),
    Conditional(TemplateConditional),
    Range(TemplateRange),
    With(TemplateWith),
    Assignment(TemplateAssignment),
}
