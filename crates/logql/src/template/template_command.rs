use num_traits::ToPrimitive;

use super::{
    ParseError, TemplateRenderContext, TemplateRuntimeValue, TemplateValue,
    evaluate_template_function, is_template_function_name, template_parse_error,
    tokenize_template_command,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum TemplateCommand {
    Value(TemplateValue),
    Call {
        callee: TemplateValue,
        args: Vec<TemplateValue>,
    },
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
            return Ok(Self::Call {
                callee: TemplateValue::parse(head)?,
                args: tail
                    .iter()
                    .map(|token| TemplateValue::parse(token))
                    .collect::<Result<Vec<_>, _>>()?,
            });
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
        if context.error.borrow().is_some() {
            return TemplateRuntimeValue::String(String::new());
        }
        match self {
            Self::Value(TemplateValue::Bare(value)) if value == "nil" => {
                *context.error.borrow_mut() = Some("nil is not a command".into());
                TemplateRuntimeValue::String(String::new())
            }
            Self::Value(value) => {
                value.evaluate_with_args(context, &input.into_iter().collect::<Vec<_>>())
            }
            Self::Call { callee, args } => {
                let mut values = args
                    .iter()
                    .enumerate()
                    .map(|(index, arg)| {
                        arg.evaluate_method_argument(
                            context,
                            callee.method_name().and_then(|name| {
                                histogram_method_signature(name, index).or_else(|| {
                                    super::template_time::methods::signature(name, index)
                                })
                            }),
                        )
                    })
                    .collect::<Vec<_>>();
                values.extend(input);
                let values = values
                    .iter()
                    .map(TemplateRuntimeValue::resolved)
                    .collect::<Vec<_>>();
                if context.error.borrow().is_some() {
                    return TemplateRuntimeValue::String(String::new());
                }
                callee.evaluate_with_args(context, &values)
            }
            Self::Function { name, args } => {
                if matches!(name.as_str(), "and" | "or") {
                    if args.is_empty() && input.is_none() {
                        *context.error.borrow_mut() =
                            Some(format!("wrong number of args for {name}"));
                        return TemplateRuntimeValue::String(String::new());
                    }
                    let mut value = TemplateRuntimeValue::String(String::new());
                    for arg in args {
                        value = arg.evaluate(context);
                        if context.error.borrow().is_some() {
                            return TemplateRuntimeValue::String(String::new());
                        }
                        if value.is_truthy() == (name == "or") {
                            return value;
                        }
                    }
                    return input.unwrap_or(value);
                }
                let mut values = args
                    .iter()
                    .enumerate()
                    .map(|(index, arg)| {
                        let parameter = template_signature(name)
                            .and_then(|(parameters, _)| {
                                parameters.get(index).or_else(|| parameters.last())
                            })
                            .copied();
                        let value = match (parameter, arg) {
                            (Some("[]float64"), TemplateValue::Unsigned(number)) => {
                                TemplateRuntimeValue::Float(
                                    number.to_f64().expect("u64 fits finite f64"),
                                )
                            }
                            (Some("[]float64"), TemplateValue::Complex(real, imaginary))
                                if f64::from_bits(*imaginary) == 0.0 =>
                            {
                                TemplateRuntimeValue::Float(f64::from_bits(*real))
                            }
                            (Some("int"), TemplateValue::Complex(real, imaginary))
                                if f64::from_bits(*imaginary) == 0.0
                                    && (-9_223_372_036_854_775_808.0
                                        ..9_223_372_036_854_775_808.0)
                                        .contains(&f64::from_bits(*real))
                                    && f64::from_bits(*real).fract() == 0.0 =>
                            {
                                TemplateRuntimeValue::Integer(
                                    f64::from_bits(*real)
                                        .to_i64()
                                        .expect("checked integer literal range"),
                                )
                            }
                            _ => arg.evaluate(context),
                        };
                        match (parameter, arg, &value) {
                            (Some("[]float64"), TemplateValue::Integer(value), _) => {
                                TemplateRuntimeValue::Float(
                                    value
                                        .to_string()
                                        .parse()
                                        .expect("integer literal fits float"),
                                )
                            }
                            (
                                Some("int"),
                                TemplateValue::Float(_),
                                TemplateRuntimeValue::Float(number),
                            ) if number.is_finite() && number.fract() == 0.0 => {
                                format!("{number:.0}")
                                    .parse::<i64>()
                                    .map_or(value.clone(), TemplateRuntimeValue::Integer)
                            }
                            _ => value,
                        }
                    })
                    .collect::<Vec<_>>();
                if let Some(input) = input {
                    values.push(input);
                }
                if context.error.borrow().is_some() {
                    return TemplateRuntimeValue::String(String::new());
                }
                let values = values
                    .iter()
                    .map(TemplateRuntimeValue::resolved)
                    .collect::<Vec<_>>();
                if context.prometheus_timestamp_ms.is_some()
                    && let Some(result) =
                        super::prometheus_functions::evaluate(name, &values, context)
                {
                    return result.unwrap_or_else(|error| {
                        *context.error.borrow_mut() = Some(error);
                        TemplateRuntimeValue::String(String::new())
                    });
                }
                if let Err(error) = validate_template_function(name, &values) {
                    *context.error.borrow_mut() = Some(error);
                    return TemplateRuntimeValue::String(String::new());
                }
                if name == "query" {
                    return values.first().map_or_else(
                        || TemplateRuntimeValue::Json(serde_json::Value::Array(Vec::new())),
                        |query| {
                            let value = if context.discover_queries {
                                let index = context.query_index.get();
                                context.query_index.set(index + 1);
                                context
                                    .query_results
                                    .get(index)
                                    .cloned()
                                    .unwrap_or_else(|| {
                                        context
                                            .pending_query
                                            .borrow_mut()
                                            .get_or_insert_with(|| query.as_rendered_string());
                                        *context.error.borrow_mut() =
                                            Some("query result unavailable".into());
                                        TemplateRuntimeValue::Array(Vec::new())
                                    })
                            } else {
                                context
                                    .queries
                                    .get(&query.as_rendered_string())
                                    .cloned()
                                    .unwrap_or_else(|| TemplateRuntimeValue::Array(Vec::new()))
                            };
                            if let TemplateRuntimeValue::QueryError(error) = value {
                                *context.error.borrow_mut() = Some(error);
                                TemplateRuntimeValue::String(String::new())
                            } else if context.prometheus_timestamp_ms.is_some() {
                                super::prometheus_functions::query_result(value)
                            } else {
                                value
                            }
                        },
                    );
                }
                evaluate_template_function(name, &values)
            }
        }
    }
}

fn validate_template_function(name: &str, values: &[TemplateRuntimeValue]) -> Result<(), String> {
    validate_template_arguments(name, values)?;
    let count = values.len();
    let expected = match name {
        "not" | "len" | "int" | "float64" | "fromJson" | "lower" | "upper" | "b64enc"
        | "b64dec" | "title" | "trim" | "bytes" | "duration" | "duration_seconds"
        | "unixToTime" | "urlencode" | "urldecode" => Some(1),
        "ne" | "lt" | "le" | "gt" | "ge" | "contains" | "hasPrefix" | "hasSuffix" | "count"
        | "div" | "mod" | "sub" => Some(2),
        "regexReplaceAll" | "regexReplaceAllLiteral" | "substr" | "replace" => Some(3),
        _ => None,
    };
    if expected.is_some_and(|expected| expected != count) || (name == "eq" && count < 2) {
        return Err(format!("wrong number of args for {name}"));
    }
    if matches!(name, "eq" | "ne" | "lt" | "le" | "gt" | "ge") {
        let kind = |value: &TemplateRuntimeValue| match value {
            TemplateRuntimeValue::Integer(_)
            | TemplateRuntimeValue::Integer64(_)
            | TemplateRuntimeValue::Byte(_)
            | TemplateRuntimeValue::Month(_)
            | TemplateRuntimeValue::Weekday(_)
            | TemplateRuntimeValue::Duration(_)
            | TemplateRuntimeValue::Integer32(_)
            | TemplateRuntimeValue::Unsigned32(_)
            | TemplateRuntimeValue::CounterResetHint(_) => "int",
            TemplateRuntimeValue::Json(serde_json::Value::Bool(_)) => "bool",
            TemplateRuntimeValue::Float(_)
            | TemplateRuntimeValue::Json(serde_json::Value::Number(_)) => "float",
            TemplateRuntimeValue::Time(_) => "time",
            TemplateRuntimeValue::TimePointer(_) => "pointer-time",
            TemplateRuntimeValue::DurationPointer(_) => "pointer-duration",
            TemplateRuntimeValue::Sample(_) => "pointer-sample",
            TemplateRuntimeValue::FloatHistogram(_) => "pointer-histogram",
            TemplateRuntimeValue::HistogramError(_) => "pointer-error",
            TemplateRuntimeValue::HistogramIterator(_) => "pointer-iterator",
            TemplateRuntimeValue::HistogramBucket(_) => "bucket",
            TemplateRuntimeValue::HistogramSpan(_) => "span",
            TemplateRuntimeValue::NilError => "nil",
            TemplateRuntimeValue::Complex(_, _) => "complex",
            _ if value.is_template_string() => "string",
            _ => "collection",
        };
        let first = kind(&values[0]);
        let null = |value: &TemplateRuntimeValue| {
            matches!(
                value,
                TemplateRuntimeValue::Json(serde_json::Value::Null)
                    | TemplateRuntimeValue::NilError
            )
        };
        let nil_equality = matches!(name, "eq" | "ne")
            && values[1..]
                .iter()
                .all(|value| null(&values[0]) || null(value));
        if !nil_equality
            && (first == "collection"
                || ((matches!(first, "bool" | "time" | "complex" | "bucket" | "span")
                    || first.starts_with("pointer-"))
                    && !matches!(name, "eq" | "ne"))
                || values[1..].iter().any(|value| kind(value) != first))
        {
            return Err("incompatible types for comparison".to_string());
        }
    }
    if matches!(name, "addf" | "subf" | "mulf" | "divf") {
        if name != "addf" && values.is_empty() {
            return Err(format!("wrong number of args for {name}"));
        }
        for (index, value) in values.iter().enumerate() {
            let value = value
                .as_numeric_string(false)
                .parse::<f64>()
                .unwrap_or_default();
            if !value.is_finite() || (name == "divf" && index > 0 && value == 0.0) {
                return Err("invalid decimal operand".to_string());
            }
        }
    }
    if name == "len"
        && !matches!(
            &values[0],
            TemplateRuntimeValue::FloatSlice(_)
                | TemplateRuntimeValue::HistogramSpans(_)
                | TemplateRuntimeValue::Labels(_)
                | TemplateRuntimeValue::ByteLabels(_)
                | TemplateRuntimeValue::Object(_)
                | TemplateRuntimeValue::Array(_)
                | TemplateRuntimeValue::QueryResult(_)
                | TemplateRuntimeValue::Bytes(_)
                | TemplateRuntimeValue::ByteSlice(_)
                | TemplateRuntimeValue::String(_)
                | TemplateRuntimeValue::Json(
                    serde_json::Value::String(_)
                        | serde_json::Value::Array(_)
                        | serde_json::Value::Object(_)
                )
        )
    {
        return Err("len of unsupported type".to_string());
    }
    if name == "index" {
        let Some((value, indexes)) = super::template_collection_first_args(values) else {
            return Err("index of unsupported type".to_string());
        };
        let mut current = value.clone();
        for index in indexes {
            if matches!(
                current,
                TemplateRuntimeValue::Labels(_)
                    | TemplateRuntimeValue::ByteLabels(_)
                    | TemplateRuntimeValue::Object(_)
                    | TemplateRuntimeValue::Json(serde_json::Value::Object(_))
            ) {
                if !index.is_template_string() {
                    return Err("map index must be string".to_string());
                }
            } else if !matches!(
                index,
                TemplateRuntimeValue::Integer(_)
                    | TemplateRuntimeValue::Integer64(_)
                    | TemplateRuntimeValue::Byte(_)
            ) {
                return Err("index must be integer".to_string());
            }
            current = super::template_index_value(&current, &index.as_rendered_string())
                .ok_or_else(|| "index out of range or unsupported type".to_string())?;
        }
    }
    if name == "slice" {
        let Some((value, bounds)) = super::template_collection_first_args(values) else {
            return Err("slice of unsupported type".to_string());
        };
        let (length, string) = match value {
            TemplateRuntimeValue::String(value)
            | TemplateRuntimeValue::Json(serde_json::Value::String(value)) => (value.len(), true),
            TemplateRuntimeValue::Json(serde_json::Value::Array(value)) => (value.len(), false),
            TemplateRuntimeValue::Array(value) => (value.len(), false),
            TemplateRuntimeValue::ByteSlice(value) => (value.len(), false),
            TemplateRuntimeValue::FloatSlice(value)
            | TemplateRuntimeValue::HistogramSpans(value) => (value.len(), false),
            TemplateRuntimeValue::QueryResult(value) => (value.len(), false),
            TemplateRuntimeValue::Bytes(value) => (value.len(), true),
            _ => return Err("slice of unsupported type".to_string()),
        };
        if (string && bounds.len() > 2)
            || bounds.iter().any(|bound| {
                !matches!(
                    bound,
                    TemplateRuntimeValue::Integer(_)
                        | TemplateRuntimeValue::Integer64(_)
                        | TemplateRuntimeValue::Byte(_)
                )
            })
            || super::template_slice_bounds(length, bounds).is_none()
        {
            return Err("invalid slice bounds".to_string());
        }
    }
    if matches!(name, "regexReplaceAll" | "regexReplaceAllLiteral" | "count") {
        let pattern = values[0].string_bytes().ok_or("regex must be a string")?;
        let pattern = std::str::from_utf8(pattern).map_err(|_| "invalid UTF-8 in regexp")?;
        regex::Regex::new(pattern).map_err(|error| error.to_string())?;
    }
    if name == "urldecode" {
        let value = values[0].as_rendered_string();
        let mut chars = value.bytes();
        while let Some(byte) = chars.next() {
            if byte == b'%'
                && (!chars.next().is_some_and(|byte| byte.is_ascii_hexdigit())
                    || !chars.next().is_some_and(|byte| byte.is_ascii_hexdigit()))
            {
                return Err("invalid URL escape".to_string());
            }
        }
    }
    if matches!(name, "div" | "mod")
        && super::parse_template_integer(&values[1].as_numeric_string(true)) == 0
    {
        return Err("integer divide by zero".to_string());
    }
    Ok(())
}

// Actual registered signatures of pinned Loki 3.7.7 and its Sprig allowlist.
fn template_signature(name: &str) -> Option<(&'static [&'static str], bool)> {
    match name {
        "Replace" => Some((&["string", "string", "string", "int"], false)),
        "ToLower" | "ToUpper" | "TrimSpace" | "b64dec" | "b64enc" | "bytes" | "duration"
        | "duration_seconds" | "fromJson" | "lower" | "title" | "trim" | "unixToTime" | "upper"
        | "urldecode" | "urlencode" => Some((&["string"], false)),
        "Trim" | "TrimLeft" | "TrimPrefix" | "TrimRight" | "TrimSuffix" | "contains" | "count"
        | "hasPrefix" | "hasSuffix" | "toDate" | "trimAll" | "trimPrefix" | "trimSuffix" => {
            Some((&["string", "string"], false))
        }
        "__line__" | "__timestamp__" | "now" => Some((&[], false)),
        "add" | "addf" => Some((&["[]interface {}"], true)),
        "alignLeft" | "alignRight" | "indent" | "nindent" | "repeat" | "trunc" => {
            Some((&["int", "string"], false))
        }
        "ceil" | "float64" | "floor" | "int" => Some((&["interface {}"], false)),
        "date" => Some((&["string", "interface {}"], false)),
        "default" | "divf" | "max" | "maxf" | "min" | "minf" | "mul" | "mulf" | "subf" => {
            Some((&["interface {}", "[]interface {}"], true))
        }
        "div" | "mod" | "sub" => Some((&["interface {}", "interface {}"], false)),
        "regexReplaceAll" | "regexReplaceAllLiteral" | "replace" | "toDateInZone" => {
            Some((&["string", "string", "string"], false))
        }
        "round" => Some((&["interface {}", "int", "[]float64"], true)),
        "substr" => Some((&["int", "int", "string"], false)),
        "unixEpoch" | "unixEpochMillis" | "unixEpochNanos" => Some((&["time.Time"], false)),
        _ => None,
    }
}

fn histogram_method_signature(name: &str, index: usize) -> Option<&'static str> {
    match name {
        "CopyToSchema" | "ReduceResolution" => Some("int32"),
        "Compact" => Some("int"),
        "Mul" | "Div" => Some("float64"),
        "TrimBuckets" if index == 0 => Some("float64"),
        _ => None,
    }
}

fn validate_template_arguments(name: &str, values: &[TemplateRuntimeValue]) -> Result<(), String> {
    let count = values.len();
    if let Some((parameters, variadic)) = template_signature(name) {
        let minimum = parameters.len().saturating_sub(usize::from(variadic));
        if count < minimum || (!variadic && count != parameters.len()) {
            return Err(format!("wrong number of args for {name}"));
        }
        for (index, value) in values.iter().enumerate() {
            let parameter = parameters
                .get(index)
                .or_else(|| parameters.last())
                .copied()
                .unwrap_or("interface {}");
            let valid = match parameter {
                "string" => value.is_template_string(),
                "int" => matches!(value, TemplateRuntimeValue::Integer(_)),
                "time.Time" => matches!(value, TemplateRuntimeValue::Time(_)),
                "[]float64" => matches!(value, TemplateRuntimeValue::Float(_)),
                _ => true,
            };
            if !valid {
                return Err(format!(
                    "wrong type for {name} argument {}: expected {parameter}",
                    index + 1
                ));
            }
        }
    }
    if name == "bytes" && super::parse_template_bytes(&values[0].as_rendered_string()).is_none() {
        return Err("invalid byte size".to_string());
    }
    if matches!(name, "duration" | "duration_seconds")
        && super::parse_template_duration(&values[0].as_rendered_string()).is_none()
    {
        return Err("invalid duration".to_string());
    }
    if matches!(name, "indent" | "nindent" | "repeat")
        && values[0].signed_integer().is_some_and(|value| value < 0)
    {
        return Err("negative string repeat count".into());
    }
    if name == "substr" {
        let length = values[2].string_bytes().map_or(0, <[u8]>::len);
        let start = values[0].signed_integer().unwrap_or_default();
        let end = values[1].signed_integer().unwrap_or_default();
        let end = usize::try_from(end).map_or(length, |end| end.min(length));
        if (start < 0
            && values[1]
                .signed_integer()
                .is_some_and(|end| end < 0 || usize::try_from(end).is_ok_and(|end| end > length)))
            || usize::try_from(start).is_ok_and(|start| start > end)
        {
            return Err("substring bounds out of range".into());
        }
    }
    if name == "unixToTime"
        && super::TemplateTime::from_epoch_string(&values[0].as_rendered_string()).is_none()
    {
        return Err("invalid unix epoch".into());
    }
    if name == "call" {
        return Err("call requires a function-valued argument".to_string());
    }
    if name == "printf"
        && values
            .first()
            .is_none_or(|value| !value.is_template_string())
    {
        return Err("printf format must be a string".to_string());
    }

    Ok(())
}
