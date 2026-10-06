use num_traits::ToPrimitive;

use super::*;
pub(super) mod number;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplateValue {
    Current,
    Field(String),
    Root {
        path: Vec<String>,
    },
    Variable {
        name: String,
        path: Vec<String>,
    },
    Line,
    Timestamp,
    String(String),
    Bytes(Vec<u8>),
    Integer(i64),
    Unsigned(u64),
    Float(u64),
    Complex(u64, u64),
    Expression(Box<TemplateExpression>),
    ExpressionPath {
        expression: Box<TemplateExpression>,
        path: Vec<String>,
    },
    Bare(String),
    Function(String),
}

impl TemplateValue {
    pub(crate) fn parse(token: &str) -> Result<Self, ParseError> {
        if token.starts_with('(') {
            let (inner, next) = parse_template_parenthesized_token(token, 0)?;
            let expression = Box::new(TemplateExpression::parse(inner[1..inner.len() - 1].trim())?);
            let suffix = &token[next..];
            if suffix.is_empty() {
                return Ok(Self::Expression(expression));
            }
            let path = suffix
                .strip_prefix('.')
                .ok_or_else(|| template_parse_error("invalid expression chain"))?
                .split('.')
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            if path.iter().any(|part| {
                part.is_empty() || !part.chars().all(|ch| ch.is_alphanumeric() || ch == '_')
            }) {
                return Err(template_parse_error("invalid expression chain"));
            }
            return Ok(Self::ExpressionPath { expression, path });
        }
        if token == "." {
            return Ok(Self::Current);
        }
        if token.starts_with(['+', '-'])
            || token.as_bytes().first().is_some_and(u8::is_ascii_digit)
            || (token.starts_with('.') && token.as_bytes().get(1).is_some_and(u8::is_ascii_digit))
        {
            return number::literal(token)
                .map(|value| match value {
                    number::Literal::Signed(value) => Self::Integer(value),
                    number::Literal::Unsigned(value) => Self::Unsigned(value),
                    number::Literal::Float(value) => Self::Float(value),
                    number::Literal::Complex(real, imaginary) => Self::Complex(real, imaginary),
                })
                .ok_or_else(|| template_parse_error("invalid or overflowing numeric constant"));
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
        if super::is_wrapped_template_token(token, '\'') {
            let body = &token[1..token.len() - 1];
            let bytes = super::decode_quoted_bytes(body, '\'')?;
            if body.starts_with('\\') {
                let escape = body.as_bytes().get(1).copied().unwrap_or_default();
                let width = match escape {
                    b'x' | b'0'..=b'7' => 4,
                    b'u' => 6,
                    b'U' => 10,
                    _ => 2,
                };
                if body.len() != width {
                    return Err(template_parse_error(
                        "rune constant contains multiple escapes",
                    ));
                }
                if matches!(escape, b'x' | b'0'..=b'7') && bytes.len() == 1 {
                    return Ok(Self::Integer(i64::from(bytes[0])));
                }
            }
            let value = std::str::from_utf8(&bytes)
                .map_err(|_| template_parse_error("invalid rune constant"))?;
            let mut chars = value.chars();
            let ch = chars
                .next()
                .ok_or_else(|| template_parse_error("empty rune constant"))?;
            if chars.next().is_some() {
                return Err(template_parse_error(
                    "rune constant contains multiple characters",
                ));
            }
            return Ok(Self::Integer(i64::from(u32::from(ch))));
        }
        if super::is_wrapped_template_token(token, '"') {
            let bytes = super::decode_quoted_bytes(&token[1..token.len() - 1], '"')?;
            return Ok(match String::from_utf8(bytes) {
                Ok(value) => Self::String(value),
                Err(error) => Self::Bytes(error.into_bytes()),
            });
        }
        if let Some(value) = quoted_template_token_value(token)? {
            return Ok(Self::String(if token.starts_with('`') {
                value.replace('\r', "")
            } else {
                value
            }));
        }
        if is_template_function_name(token) {
            return Ok(Self::Function(token.to_string()));
        }
        if matches!(token, "true" | "false" | "nil") {
            return Ok(Self::Bare(token.to_string()));
        }
        Err(template_parse_error(
            "unknown template function or identifier",
        ))
    }

    pub(crate) fn evaluate(&self, context: &TemplateRenderContext<'_>) -> TemplateRuntimeValue {
        match self {
            Self::Current => context
                .current_dot
                .clone()
                .unwrap_or_else(|| TemplateRuntimeValue::String(String::new())),
            Self::Field(_)
            | Self::Root { .. }
            | Self::Variable { .. }
            | Self::ExpressionPath { .. } => self.evaluate_with_args(context, &[]),
            Self::Line => TemplateRuntimeValue::String(context.line.to_string()),
            Self::Timestamp => TemplateRuntimeValue::Time(super::TemplateTime::from_unix_nanos(
                context.timestamp_ns.unwrap_or(0),
            )),
            Self::Bare(value) if value == "nil" => {
                TemplateRuntimeValue::Json(serde_json::Value::Null)
            }
            Self::Bare(value) if matches!(value.as_str(), "true" | "false") => {
                TemplateRuntimeValue::Json(serde_json::Value::Bool(value == "true"))
            }
            Self::String(value) | Self::Bare(value) => TemplateRuntimeValue::String(value.clone()),
            Self::Bytes(value) => TemplateRuntimeValue::Bytes(value.clone()),
            Self::Integer(value) => TemplateRuntimeValue::Integer(*value),
            Self::Unsigned(value) => {
                *context.error.borrow_mut() = Some(format!("{value} overflows int"));
                TemplateRuntimeValue::String(String::new())
            }
            Self::Float(bits) => TemplateRuntimeValue::Float(f64::from_bits(*bits)),
            Self::Complex(real, imaginary) => {
                TemplateRuntimeValue::Complex(f64::from_bits(*real), f64::from_bits(*imaginary))
            }
            Self::Expression(expression) => expression.evaluate(context),
            Self::Function(name) => TemplateCommand::Function {
                name: name.clone(),
                args: Vec::new(),
            }
            .evaluate(context, None),
        }
    }
    pub(crate) fn evaluate_with_args(
        &self,
        context: &TemplateRenderContext<'_>,
        args: &[TemplateRuntimeValue],
    ) -> TemplateRuntimeValue {
        let (mut value, path) = match self {
            Self::Field(name) => (
                context
                    .current_dot
                    .clone()
                    .unwrap_or_else(|| TemplateRuntimeValue::Labels(context.fields.clone())),
                name.split('.').map(ToString::to_string).collect::<Vec<_>>(),
            ),
            Self::Root { path } => (
                context
                    .root_dot
                    .clone()
                    .unwrap_or_else(|| TemplateRuntimeValue::Labels(context.fields.clone())),
                path.clone(),
            ),
            Self::Variable { name, path } => (
                context.variables.get(name).map_or_else(
                    || TemplateRuntimeValue::String(String::new()),
                    |value| value.borrow().clone(),
                ),
                path.clone(),
            ),
            Self::ExpressionPath { expression, path } => {
                (expression.evaluate(context), path.clone())
            }
            _ if args.is_empty() => return self.evaluate(context),
            _ => {
                *context.error.borrow_mut() = Some("value is not a function".into());
                return TemplateRuntimeValue::String(String::new());
            }
        };
        for (index, name) in path.iter().enumerate() {
            let arguments = if index + 1 == path.len() { args } else { &[] };
            let result = if matches!(
                value,
                TemplateRuntimeValue::Time(_)
                    | TemplateRuntimeValue::TimePointer(_)
                    | TemplateRuntimeValue::DurationPointer(_)
                    | TemplateRuntimeValue::Duration(_)
                    | TemplateRuntimeValue::Month(_)
                    | TemplateRuntimeValue::Weekday(_)
                    | TemplateRuntimeValue::TimeLocation(_)
            ) {
                super::template_time::methods::call(&value, name, arguments)
            } else if let TemplateRuntimeValue::FloatHistogram(histogram) = &value {
                histogram
                    .field(name)
                    .filter(|_| arguments.is_empty())
                    .map_or_else(|| histogram.call(name, arguments), Ok)
            } else if let TemplateRuntimeValue::HistogramIterator(iterator) = &value {
                iterator.call(name, arguments)
            } else if arguments.is_empty() {
                template_variable_path_value(&value, std::slice::from_ref(name))
                    .ok_or_else(|| format!("cannot evaluate field {name}"))
            } else {
                Err(format!("{name} is not a method"))
            };
            match result {
                Ok(next) => value = next,
                Err(error) => {
                    *context.error.borrow_mut() = Some(error);
                    return TemplateRuntimeValue::String(String::new());
                }
            }
        }
        if path.is_empty() && !args.is_empty() {
            *context.error.borrow_mut() = Some("value is not a function".into());
        }
        value
    }
    pub(crate) fn method_name(&self) -> Option<&str> {
        match self {
            Self::Field(name) => name.rsplit('.').next(),
            Self::Variable { path, .. }
            | Self::Root { path }
            | Self::ExpressionPath { path, .. } => path.last().map(String::as_str),
            _ => None,
        }
    }
    pub(crate) fn evaluate_method_argument(
        &self,
        context: &TemplateRenderContext<'_>,
        parameter: Option<&str>,
    ) -> TemplateRuntimeValue {
        let number = match self {
            Self::Integer(value) => Some(*value),
            Self::Float(bits) => {
                let value = f64::from_bits(*bits);
                (value.is_finite()
                    && value.fract() == 0.0
                    && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value))
                .then(|| value.to_i64().expect("checked integer literal range"))
            }
            Self::Complex(real, imaginary) if f64::from_bits(*imaginary) == 0.0 => {
                let value = f64::from_bits(*real);
                (value.is_finite()
                    && value.fract() == 0.0
                    && (-9_223_372_036_854_775_808.0..9_223_372_036_854_775_808.0).contains(&value))
                .then(|| value.to_i64().expect("checked integer literal range"))
            }
            _ => None,
        };
        match (parameter, number) {
            (Some("time.Duration"), Some(value)) => TemplateRuntimeValue::Duration(value),
            (Some("int"), Some(value)) => TemplateRuntimeValue::Integer(value),
            (Some("int32"), Some(value)) => i32::try_from(value)
                .map_or_else(|_| self.evaluate(context), TemplateRuntimeValue::Integer32),
            (Some("float64"), Some(value)) => {
                TemplateRuntimeValue::Float(value.to_f64().expect("i64 fits finite f64"))
            }
            _ => self.evaluate(context),
        }
    }
}
