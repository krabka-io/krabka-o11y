use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct TemplateExpression {
    pub(crate) commands: Vec<TemplateCommand>,
}

impl TemplateExpression {
    pub(crate) fn parse(expression: &str) -> Result<Self, ParseError> {
        let mut commands = Vec::new();
        for command in split_template_pipeline(expression)? {
            commands.push(TemplateCommand::parse(command.trim())?);
        }
        if commands.is_empty() {
            return Err(template_parse_error("expected template action"));
        }
        Ok(Self { commands })
    }

    pub(crate) fn render(&self, context: &TemplateRenderContext<'_>) -> String {
        self.evaluate(context).into_rendered_string()
    }

    pub(crate) fn evaluate(&self, context: &TemplateRenderContext<'_>) -> TemplateRuntimeValue {
        let mut input = None;
        for command in &self.commands {
            input = Some(command.evaluate(context, input));
        }
        input.unwrap_or_else(|| TemplateRuntimeValue::String(String::new()))
    }
}
