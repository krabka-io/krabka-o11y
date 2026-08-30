use std::collections::BTreeSet;

use crate::{
    DestinationLabel, JsonExpressionPath, ParseError, SourceLabel, template::template_parse_error,
    util::decode_quoted_escape,
};

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::{
        DestinationLabel, JsonExpressionPath, JsonExtraction, JsonPath, JsonPathPart,
        LogfmtExtraction, LogfmtParserConfig, SourceLabel,
    };

    #[test]
    fn json_extraction_expression_returns_source_text() {
        let extraction = JsonExtraction::new(
            DestinationLabel("value".to_string()),
            JsonExpressionPath("trace:id.request-id".to_string()),
        )
        .unwrap();

        assert_eq!(extraction.expression(), "trace:id.request-id");
    }

    #[test]
    fn json_path_parse_advances_over_dot_separators() {
        assert_eq!(
            JsonPath::parse("request.headers").unwrap(),
            JsonPath {
                parts: vec![
                    JsonPathPart::Field("request".to_string()),
                    JsonPathPart::Field("headers".to_string()),
                ],
            }
        );
    }

    #[test]
    fn json_path_parse_rejects_empty_dot_field_segments() {
        for path in [".request", "request.", "request..headers"] {
            assert!(JsonPath::parse(path).is_err(), "{path}");
        }
    }

    #[test]
    fn json_path_parse_advances_over_array_indexes() {
        assert_eq!(
            JsonPath::parse("servers[10]").unwrap(),
            JsonPath {
                parts: vec![
                    JsonPathPart::Field("servers".to_string()),
                    JsonPathPart::Index(10),
                ],
            }
        );
    }

    #[test]
    fn json_path_parse_accepts_bracket_start_field() {
        assert_eq!(
            JsonPath::parse(r#"["request"].headers"#).unwrap(),
            JsonPath {
                parts: vec![
                    JsonPathPart::Field("request".to_string()),
                    JsonPathPart::Field("headers".to_string()),
                ],
            }
        );
    }

    #[test]
    fn json_path_bracket_strings_decode_escaped_characters() {
        assert_eq!(
            JsonPath::parse(r#"headers["quoted\"name"]"#).unwrap(),
            JsonPath {
                parts: vec![
                    JsonPathPart::Field("headers".to_string()),
                    JsonPathPart::Field("quoted\"name".to_string()),
                ],
            }
        );
    }

    #[test]
    fn json_path_field_names_accept_identifier_punctuation() {
        assert_eq!(
            JsonPath::parse("trace:id.request-id._meta").unwrap(),
            JsonPath {
                parts: vec![
                    JsonPathPart::Field("trace:id".to_string()),
                    JsonPathPart::Field("request-id".to_string()),
                    JsonPathPart::Field("_meta".to_string()),
                ],
            }
        );
    }

    #[test]
    fn logfmt_flags_preserve_non_strict_keep_empty_config() {
        let config = LogfmtParserConfig::flags(false, true).unwrap();

        assert!(!config.strict());
        assert!(config.keep_empty());
    }

    #[test]
    fn logfmt_extractions_reject_empty_destination_or_source() {
        check!(LogfmtExtraction::same("").is_err());
        check!(
            LogfmtExtraction::rename(
                DestinationLabel(String::new()),
                SourceLabel("source".into())
            )
            .is_err()
        );
        check!(
            LogfmtExtraction::rename(
                DestinationLabel("destination".into()),
                SourceLabel(String::new())
            )
            .is_err()
        );
    }
}

// === split-modules: generated submodules ===
mod is_json_path_field_name_char;
mod json_extraction;
mod json_parser_config;
mod json_path;
mod json_path_parser;
mod json_path_part;
mod logfmt_extraction;
mod logfmt_parser;
mod logfmt_parser_config;

use is_json_path_field_name_char::is_json_path_field_name_char;
pub use json_extraction::JsonExtraction;
pub use json_parser_config::JsonParserConfig;
use json_path::JsonPath;
use json_path_parser::JsonPathParser;
use json_path_part::JsonPathPart;
pub use logfmt_extraction::LogfmtExtraction;
pub (crate) use logfmt_parser::LogfmtParser;
pub use logfmt_parser_config::LogfmtParserConfig;
