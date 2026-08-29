#[allow(clippy::wildcard_imports)]
use super::*;

pub(crate) fn parse_vector_arithmetic_operator(
    query: &str,
    position: usize,
) -> Option<(&'static str, usize)> {
    for (raw, formatted) in [
        ("+", "+"),
        ("-", "-"),
        ("*", "*"),
        ("/", "/"),
        ("%", "%"),
        ("^", "^"),
    ] {
        if query[position..].starts_with(raw) {
            return Some((formatted, position + raw.len()));
        }
    }
    None
}

pub(crate) fn parse_formatted_vector_function(
    query: &str,
    position: usize,
) -> Option<(String, usize)> {
    if let Some(scalar) = query[position..].strip_prefix("vector(") {
        let scalar_end = scalar.find(')')?;
        let scalar_text = &scalar[..scalar_end];
        if scalar_text.starts_with(['+', '-']) {
            return None;
        }
        let sample = parse_scalar_sample(scalar_text)?.format_fixed_six();
        return Some((
            format!("vector({sample})"),
            position + "vector(".len() + scalar_end + 1,
        ));
    }

    let call_end = find_logql_function_call_end(query, position, "label_replace")?;
    let formatted = format_vector_label_replace_function(&query[position..call_end])?;
    Some((formatted, call_end))
}

pub(crate) fn find_logql_function_call_end(
    query: &str,
    position: usize,
    name: &str,
) -> Option<usize> {
    let rest = &query[position..];
    let rest = rest.strip_prefix(name)?;
    let mut chars = rest.char_indices();
    let (_, first) = chars.next()?;
    if first != '(' {
        return None;
    }

    let mut parens = 1_i32;
    let mut quote = None;
    let mut escaped = false;
    for (index, ch) in chars {
        if let Some(quote_ch) = quote {
            if escaped {
                escaped = false;
            } else if ch == '\\' {
                escaped = true;
            } else if ch == quote_ch {
                quote = None;
            }
            continue;
        }
        match ch {
            '"' | '`' => quote = Some(ch),
            '(' => parens += 1,
            ')' => {
                parens -= 1;
                if parens == 0 {
                    return Some(position + name.len() + index + ch.len_utf8());
                }
            }
            _ => {}
        }
    }
    None
}

pub(crate) fn format_stream_query(query: &StreamQuery) -> String {
    let mut formatted = format!(
        "{{{}}}",
        query
            .matchers
            .iter()
            .map(format_label_matcher)
            .collect::<Vec<_>>()
            .join(",")
    );
    for stage in &query.pipeline {
        if matches!(stage, PipelineStage::LineFilter(_)) {
            formatted.push(' ');
        } else {
            formatted.push_str(" | ");
        }
        formatted.push_str(&format_pipeline_stage(stage));
    }
    formatted
}

pub(crate) fn format_label_matcher(matcher: &krabka_logql::LabelMatcher) -> String {
    format!(
        "{}{}{}",
        matcher.name,
        match matcher.op {
            MatchOp::Equal => "=",
            MatchOp::NotEqual => "!=",
            MatchOp::RegexEqual => "=~",
            MatchOp::RegexNotEqual => "!~",
        },
        quote_logql_string(&matcher.value)
    )
}

pub(crate) fn format_pipeline_stage(stage: &PipelineStage) -> String {
    match stage {
        PipelineStage::LineFilter(filter) => {
            let value = if filter.is_ip_matcher() {
                format!("ip({})", quote_logql_string(&filter.pattern))
            } else {
                quote_logql_string(&filter.pattern)
            };
            format!(
                "{} {value}",
                match filter.op {
                    LineFilterOp::Contains => "|=",
                    LineFilterOp::NotContains => "!=",
                    LineFilterOp::Regex => "|~",
                    LineFilterOp::NotRegex => "!~",
                    LineFilterOp::Pattern => "|>",
                    LineFilterOp::NotPattern => "!>",
                }
            )
        }
        PipelineStage::Decolorize => "decolorize".to_string(),
        PipelineStage::Parser(ParserStage::Json) => "json".to_string(),
        PipelineStage::Parser(ParserStage::JsonSelected(config)) => {
            let extractions = config
                .extractions()
                .iter()
                .map(|extraction| {
                    format!(
                        "{}={}",
                        extraction.destination(),
                        quote_logql_string(extraction.expression())
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("json {extractions}")
        }
        PipelineStage::Parser(ParserStage::Logfmt) => "logfmt".to_string(),
        PipelineStage::Parser(ParserStage::LogfmtConfigured(config)) => {
            format!("logfmt{}", format_logfmt_parser_flags(config))
        }
        PipelineStage::Parser(ParserStage::LogfmtSelected(config)) => {
            let extractions = config
                .extractions()
                .iter()
                .map(|extraction| {
                    if extraction.destination() == extraction.source() {
                        extraction.destination().to_string()
                    } else {
                        format!(
                            "{}={}",
                            extraction.destination(),
                            quote_logql_string(extraction.source())
                        )
                    }
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("logfmt{} {extractions}", format_logfmt_parser_flags(config))
        }
        PipelineStage::Parser(ParserStage::Unpack) => "unpack".to_string(),
        PipelineStage::Parser(ParserStage::Pattern(pattern)) => {
            format!("pattern {}", quote_logql_string(pattern.pattern()))
        }
        PipelineStage::Parser(ParserStage::Regexp(parser)) => {
            format!("regexp {}", quote_logql_string(parser.pattern()))
        }
        PipelineStage::LineFormat(format) => {
            format!("line_format {}", quote_logql_string(format.template()))
        }
        PipelineStage::LabelFormat(format) => {
            let assignments = format
                .assignments()
                .iter()
                .map(|assignment| {
                    let value = match assignment.value() {
                        LabelFormatValue::Rename(source) => source.clone(),
                        LabelFormatValue::Template(template) => {
                            quote_logql_string(template.template())
                        }
                    };
                    format!("{}={value}", assignment.destination())
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("label_format {assignments}")
        }
        PipelineStage::DropLabels(selections) => {
            format!("drop {}", format_label_selection_set(selections))
        }
        PipelineStage::KeepLabels(selections) => {
            format!("keep {}", format_label_selection_set(selections))
        }
        PipelineStage::Unwrap(unwrap) => match unwrap.conversion() {
            UnwrapConversion::Raw => format!("unwrap {}", unwrap.label()),
            UnwrapConversion::Bytes => format!("unwrap bytes({})", unwrap.label()),
            UnwrapConversion::Duration => format!("unwrap duration({})", unwrap.label()),
        },
        PipelineStage::FieldFilter(filter) => format_field_filter(filter),
        PipelineStage::FieldFilterChain(chain) => {
            let mut formatted = format_field_filter(chain.first());
            for (op, filter) in chain.rest() {
                formatted.push_str(match op {
                    FieldFilterLogicOp::And => " and ",
                    FieldFilterLogicOp::Or => " or ",
                });
                formatted.push_str(&format_field_filter(filter));
            }
            formatted
        }
        PipelineStage::FieldFilterExpression(expression) => {
            format_field_filter_expression(expression)
        }
    }
}

pub(crate) fn format_logfmt_parser_flags(config: &LogfmtParserConfig) -> String {
    let mut flags = Vec::new();
    if config.keep_empty() {
        flags.push("--keep-empty");
    }
    if config.strict() {
        flags.push("--strict");
    }
    if flags.is_empty() {
        String::new()
    } else {
        format!(" {}", flags.join(" "))
    }
}

pub(crate) fn format_label_selection_set(selections: &LabelSelectionSet) -> String {
    selections
        .selections()
        .iter()
        .map(|selection| {
            let Some(matcher) = selection.matcher() else {
                return selection.name_str().to_string();
            };
            match matcher {
                LabelSelectionMatcher::Equal(value) => {
                    format!("{}={}", selection.name_str(), quote_logql_string(value))
                }
                LabelSelectionMatcher::Regex(pattern) => {
                    format!("{}=~{}", selection.name_str(), quote_logql_string(pattern))
                }
            }
        })
        .collect::<Vec<_>>()
        .join(", ")
}

pub(crate) fn format_field_filter(filter: &FieldFilter) -> String {
    format!(
        "{}{}{}",
        filter.name,
        match filter.op {
            ComparisonOp::Equal => "=",
            ComparisonOp::NotEqual => "!=",
            ComparisonOp::RegexEqual => "=~",
            ComparisonOp::RegexNotEqual => "!~",
            ComparisonOp::Greater => ">",
            ComparisonOp::GreaterEqual => ">=",
            ComparisonOp::Less => "<",
            ComparisonOp::LessEqual => "<=",
        },
        match &filter.value {
            FieldValue::Number(value) => value.to_string(),
            FieldValue::Duration(value) => format!("{value}ns"),
            FieldValue::Bytes(value) => format!("{}B", value.bytes_f64()),
            FieldValue::String(value) => quote_logql_string(value),
            FieldValue::Ip(value) => format!("ip({})", quote_logql_string(value.pattern())),
        }
    )
}

pub(crate) fn format_field_filter_expression(expression: &FieldFilterExpression) -> String {
    match expression {
        FieldFilterExpression::Filter(filter) => format_field_filter(filter),
        FieldFilterExpression::Group(expression) => {
            format!("({})", format_field_filter_expression(expression))
        }
        FieldFilterExpression::Chain { first, rest } => {
            let mut formatted = format_field_filter_expression(first);
            for (op, expression) in rest {
                formatted.push_str(match op {
                    FieldFilterLogicOp::And => " and ",
                    FieldFilterLogicOp::Or => " or ",
                });
                formatted.push_str(&format_field_filter_expression(expression));
            }
            formatted
        }
    }
}

pub(crate) fn quote_logql_string(value: &str) -> String {
    serde_json::to_string(value).expect("string serialization cannot fail")
}

pub(crate) fn validate_query_series_limit(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<(), HttpQueryError> {
    let Some(max_query_series) = state.max_query_series else {
        return Ok(());
    };
    let series = plan.fingerprints.len();
    if series > max_query_series {
        return Err(HttpQueryError::QuerySeriesTooLarge {
            series,
            max_series: max_query_series,
        });
    }
    Ok(())
}

pub(crate) fn validate_query_bytes_limit(
    state: &QuerierState,
    plan: &StreamPlan,
) -> Result<(), HttpQueryError> {
    let Some(max_query_read) = state.max_query_read else {
        return Ok(());
    };
    let planned = planned_block_bytes(plan);
    if planned > max_query_read {
        // The error carries plain integers so its rendered message is fixed by
        // the `#[error]` format string alone.
        return Err(HttpQueryError::QueryBytesTooLarge {
            planned_bytes: planned.bytes_u64(),
            max_bytes: max_query_read.bytes_u64(),
        });
    }
    Ok(())
}
