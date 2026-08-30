use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateControlExpression {
    pub(crate) variable: Option<String>,
    pub(crate) expression: TemplateExpression,
}

impl TemplateControlExpression {
    pub(crate) fn parse(expression: &str) -> Result<Self, ParseError> {
        let (variable, expression) = parse_template_control_assignment(expression)?
            .map_or((None, expression.trim()), |(variable, expression)| {
                (Some(variable), expression)
            });
        Ok(Self {
            variable,
            expression: TemplateExpression::parse(expression)?,
        })
    }

    pub(crate) fn evaluate<'a>(
        &self,
        context: &TemplateRenderContext<'a>,
    ) -> (TemplateRuntimeValue, TemplateRenderContext<'a>) {
        let value = self.expression.evaluate(context);
        let context = self.variable.as_ref().map_or_else(
            || context.clone(),
            |variable| context.with_variable(variable.clone(), value.clone()),
        );
        (value, context)
    }
}
