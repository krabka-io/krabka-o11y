use super::{
    ParseError, TemplateExpression, TemplateRenderContext, TemplateRuntimeValue,
    parse_template_control_assignment,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateControlExpression {
    pub(crate) variable: Option<String>,
    pub(crate) expression: TemplateExpression,
}

pub(crate) struct EvaluatedTemplateControl<'a> {
    pub(crate) value: TemplateRuntimeValue,
    pub(crate) context: TemplateRenderContext<'a>,
}

impl TemplateControlExpression {
    pub(crate) fn parse(expression: &str) -> Result<Self, ParseError> {
        let assignment = parse_template_control_assignment(expression)?;
        let variable;
        let expression = if let Some(assignment) = assignment {
            variable = Some(assignment.variable);
            assignment.expression
        } else {
            variable = None;
            expression.trim()
        };
        Ok(Self {
            variable,
            expression: TemplateExpression::parse(expression)?,
        })
    }

    pub(crate) fn evaluate<'a>(
        &self,
        context: &TemplateRenderContext<'a>,
    ) -> EvaluatedTemplateControl<'a> {
        let value = self.expression.evaluate(context);
        let context = self.variable.as_ref().map_or_else(
            || context.clone(),
            |variable| context.with_variable(variable.clone(), value.clone()),
        );
        EvaluatedTemplateControl { value, context }
    }
}
