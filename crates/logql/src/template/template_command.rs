use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplateCommand {
    Value(TemplateValue),
    Function {
        name: String,
        args: Vec<TemplateValue>,
    },
}

impl TemplateCommand {
    pub(crate) fn parse(command: &str) -> Result<Self, ParseError> {
        let tokens = tokenize_template_command(command)?;
        let Some((head, tail)) = tokens.split_first() else {
            return Err(template_parse_error("expected template command"));
        };
        if tail.is_empty() && !is_template_function_name(head) {
            return Ok(Self::Value(TemplateValue::parse(head)?));
        }
        if !is_template_function_name(head) {
            return Err(template_parse_error("unsupported template action"));
        }
        Ok(Self::Function {
            name: head.clone(),
            args: tail
                .iter()
                .map(|token| TemplateValue::parse(token))
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    pub(crate) fn evaluate(
        &self,
        context: &TemplateRenderContext<'_>,
        input: Option<TemplateRuntimeValue>,
    ) -> TemplateRuntimeValue {
        match self {
            Self::Value(value) => value.evaluate(context),
            Self::Function { name, args } => {
                let mut values = args
                    .iter()
                    .map(|arg| arg.evaluate(context))
                    .collect::<Vec<_>>();
                if let Some(input) = input {
                    values.push(input);
                }
                evaluate_template_function(name, &values)
            }
        }
    }
}
