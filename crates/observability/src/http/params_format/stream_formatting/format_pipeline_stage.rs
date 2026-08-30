use super::{
    FieldFilterLogicOp, LabelFormatValue, LineFilterOp, ParserStage, PipelineStage,
    UnwrapConversion, format_field_filter, format_field_filter_expression,
    format_label_selection_set, format_logfmt_parser_flags, quote_logql_string,
};

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
