use super::*;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplateValue {
    Current,
    Field(String),
    Root { path: Vec<String> },
    Variable { name: String, path: Vec<String> },
    Line,
    Timestamp,
    String(String),
    Integer(i64),
    Expression(Box<TemplateExpression>),
    Bare(String),
}

impl TemplateValue {
    pub(crate) fn parse(token: &str) -> Result<Self, ParseError> {
        if token.starts_with('(') && token.ends_with(')') && token.len() >= 2 {
            return Ok(Self::Expression(Box::new(TemplateExpression::parse(
                token[1..token.len() - 1].trim(),
            )?)));
        }
        if token == "." {
            return Ok(Self::Current);
        }
        if let Some(field) = token.strip_prefix('.') {
            if field.is_empty() {
                return Err(template_parse_error("expected template field name"));
            }
            return Ok(Self::Field(field.to_string()));
        }
        if token == "$" {
            return Ok(Self::Root { path: Vec::new() });
        }
        if let Some(path) = token.strip_prefix("$.") {
            return Ok(Self::Root {
                path: path
                    .split('.')
                    .filter(|part| !part.is_empty())
                    .map(ToString::to_string)
                    .collect(),
            });
        }
        if let Some(variable) = token.strip_prefix('$') {
            if variable.is_empty() {
                return Err(template_parse_error("expected template variable name"));
            }
            let mut parts = variable.split('.');
            let Some(name) = parts.next() else {
                return Err(template_parse_error("expected template variable name"));
            };
            if name.is_empty() {
                return Err(template_parse_error("expected template variable name"));
            }
            return Ok(Self::Variable {
                name: name.to_string(),
                path: parts
                    .filter(|part| !part.is_empty())
                    .map(ToString::to_string)
                    .collect(),
            });
        }
        if matches!(token, "__line__" | "line") {
            return Ok(Self::Line);
        }
        if matches!(token, "__timestamp__" | "timestamp") {
            return Ok(Self::Timestamp);
        }
        if let Some(value) = quoted_template_token_value(token)? {
            return Ok(Self::String(value));
        }
        if let Ok(value) = token.parse::<i64>() {
            return Ok(Self::Integer(value));
        }
        Ok(Self::Bare(token.to_string()))
    }

    pub(crate) fn evaluate(&self, context: &TemplateRenderContext<'_>) -> TemplateRuntimeValue {
        match self {
            Self::Current => context
                .current_dot
                .clone()
                .unwrap_or_else(|| TemplateRuntimeValue::String(String::new())),
            Self::Field(name) => context
                .current_dot
                .as_ref()
                .and_then(|value| template_current_dot_field_value(value, name))
                .unwrap_or_else(|| {
                    TemplateRuntimeValue::String(
                        context.fields.get(name).cloned().unwrap_or_default(),
                    )
                }),
            Self::Root { path } => template_root_field_value(context.fields, path),
            Self::Variable { name, path } => context
                .variables
                .get(name)
                .and_then(|value| template_variable_path_value(value, path))
                .unwrap_or_else(|| TemplateRuntimeValue::String(String::new())),
            Self::Line => TemplateRuntimeValue::String(context.line.to_string()),
            Self::Timestamp => TemplateRuntimeValue::String(
                context
                    .timestamp_ns
                    .map_or_else(String::new, |value| value.to_string()),
            ),
            Self::String(value) | Self::Bare(value) => TemplateRuntimeValue::String(value.clone()),
            Self::Integer(value) => TemplateRuntimeValue::Integer(*value),
            Self::Expression(expression) => expression.evaluate(context),
        }
    }
}
