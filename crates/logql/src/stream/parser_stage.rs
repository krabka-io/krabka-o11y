use super::{
    JsonParserConfig, Labels, LogfmtParserConfig, PatternParser, RegexpParser,
    parse_configured_logfmt_fields, parse_json_fields, parse_logfmt_fields,
    parse_selected_logfmt_fields, unpack_json_line, variant_metadata::PipelineLabels,
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParserStage {
    Json,
    JsonSelected(JsonParserConfig),
    Logfmt,
    LogfmtConfigured(LogfmtParserConfig),
    LogfmtSelected(LogfmtParserConfig),
    Unpack,
    Pattern(PatternParser),
    Regexp(RegexpParser),
}

impl ParserStage {
    pub(crate) fn apply(&self, line: &mut String, fields: &mut Labels) {
        let mut categories = PipelineLabels::new(fields, Labels::new());
        self.apply_with_categories(line, fields, &mut categories);
    }

    pub(super) fn apply_with_categories(
        &self,
        line: &mut String,
        fields: &mut Labels,
        categories: &mut PipelineLabels,
    ) {
        let mut parsed = Labels::new();
        self.extract(line, &mut parsed, fields, categories);
        categories.insert_parser_fields(fields, parsed, self);
    }

    fn extract(
        &self,
        line: &mut String,
        fields: &mut Labels,
        existing: &Labels,
        categories: &PipelineLabels,
    ) {
        match self {
            Self::Json => parse_json_fields(line, fields),
            Self::JsonSelected(config) => {
                config.parse_selected_fields(line, fields, |name| {
                    existing.get(name).is_some_and(|value| !value.is_empty())
                        || (existing.contains_key(name)
                            && (categories.has_metadata(name) || categories.has_extracted(name)))
                });
            }
            Self::Logfmt => parse_logfmt_fields(line, fields),
            Self::LogfmtConfigured(config) => parse_configured_logfmt_fields(line, fields, config),
            Self::LogfmtSelected(config) => {
                parse_selected_logfmt_fields(line, fields, config, |name| {
                    categories.has_collision(name)
                });
            }
            Self::Unpack => unpack_json_line(line, fields, |name| {
                let name = if categories.has_collision(name) {
                    format!("{name}_extracted")
                } else {
                    name.to_string()
                };
                (!categories.has_extracted(&name)).then_some(name)
            }),
            Self::Pattern(parser) => parser.apply(line, fields),
            Self::Regexp(parser) => parser.apply(line, fields),
        }
    }
}
