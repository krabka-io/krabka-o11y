use super::{
    JsonParserConfig, Labels, LogfmtParserConfig, PatternParser, RegexpParser,
    parse_configured_logfmt_fields, parse_json_fields, parse_logfmt_fields,
    parse_selected_json_fields, parse_selected_logfmt_fields, unpack_json_line,
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
        match self {
            Self::Json => parse_json_fields(line, fields),
            Self::JsonSelected(config) => parse_selected_json_fields(line, fields, config),
            Self::Logfmt => parse_logfmt_fields(line, fields),
            Self::LogfmtConfigured(config) => parse_configured_logfmt_fields(line, fields, config),
            Self::LogfmtSelected(config) => parse_selected_logfmt_fields(line, fields, config),
            Self::Unpack => unpack_json_line(line, fields),
            Self::Pattern(parser) => parser.apply(line, fields),
            Self::Regexp(parser) => parser.apply(line, fields),
        }
    }
}
