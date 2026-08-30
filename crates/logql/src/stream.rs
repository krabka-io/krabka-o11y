use regex::Regex;

use crate::{
    FieldFilter, FieldFilterChain, FieldFilterExpression, JsonParserConfig, LabelFormat,
    LabelSelectionSet, Labels, LineFilter, LineFormat, LogfmtParserConfig, ParseError,
    UnwrapExpression, extract::LogfmtParser,
};

#[cfg(test)]
mod tests {
    use super::{
        PatternParser, PatternPart, parse_pattern_parts, sanitize_json_field_name,
        sanitize_logfmt_field_name,
    };

    /// `LabelMatcher::matches` decides which streams a selector admits, and
    /// `matches_empty_value` decides whether a *missing* label counts as
    /// matching -- the difference between `{job=""}` selecting streams with no
    /// `job` and selecting none. Neither was pinned across all four operators.
    ///
    /// Each operator is asserted for a present label that matches, a present
    /// one that does not, and an absent one, because the negated forms differ
    /// from the positive ones precisely on absence.
    #[test]
    fn label_matcher_covers_each_operator_including_absence() {
        use super::{LabelMatcher, MatchOp};

        let labels: std::collections::BTreeMap<String, String> =
            [("job".to_string(), "api".to_string())]
                .into_iter()
                .collect();

        let m = |op, value: &str| LabelMatcher::new("job", op, value).expect("valid matcher");
        let absent = |op, value: &str| LabelMatcher::new("missing", op, value).expect("valid");

        // Present and equal, present and different, absent.
        assert2::check!(m(MatchOp::Equal, "api").matches(&labels));
        assert2::check!(!m(MatchOp::Equal, "web").matches(&labels));
        assert2::check!(!absent(MatchOp::Equal, "api").matches(&labels));

        assert2::check!(!m(MatchOp::NotEqual, "api").matches(&labels));
        assert2::check!(m(MatchOp::NotEqual, "web").matches(&labels));
        assert2::check!(absent(MatchOp::NotEqual, "api").matches(&labels));

        assert2::check!(m(MatchOp::RegexEqual, "a.*").matches(&labels));
        assert2::check!(!m(MatchOp::RegexEqual, "w.*").matches(&labels));

        assert2::check!(!m(MatchOp::RegexNotEqual, "a.*").matches(&labels));
        assert2::check!(m(MatchOp::RegexNotEqual, "w.*").matches(&labels));
        // An absent label matches a negated regex, which is what lets
        // `{job!~"x"}` select streams carrying no `job` at all.
        assert2::check!(absent(MatchOp::RegexNotEqual, "a.*").matches(&labels));

        // `matches_empty_value`: does this matcher admit a stream lacking the
        // label entirely?
        assert2::check!(m(MatchOp::Equal, "").matches_empty_value());
        assert2::check!(!m(MatchOp::Equal, "api").matches_empty_value());
        assert2::check!(!m(MatchOp::NotEqual, "").matches_empty_value());
        assert2::check!(m(MatchOp::NotEqual, "api").matches_empty_value());
        assert2::check!(m(MatchOp::RegexEqual, ".*").matches_empty_value());
        assert2::check!(!m(MatchOp::RegexEqual, "a+").matches_empty_value());
        assert2::check!(!m(MatchOp::RegexNotEqual, ".*").matches_empty_value());
        assert2::check!(m(MatchOp::RegexNotEqual, "a+").matches_empty_value());
    }

    /// Both sanitizers turn an arbitrary field name into a valid label name,
    /// and neither was pinned. They differ in one way that matters: logfmt
    /// collapses a run of invalid characters into a single underscore, json
    /// replaces each one. Every other branch is shared -- the allowed set, the
    /// empty result, and the leading digit that must be pushed off the front.
    #[test]
    fn field_name_sanitizers_cover_each_branch() {
        // (input, logfmt, json)
        let cases = [
            ("ok_name", "ok_name", "ok_name"),
            ("keeps:colon", "keeps:colon", "keeps:colon"),
            ("Alpha9", "Alpha9", "Alpha9"),
            // A run of invalid characters: collapsed for logfmt, one-for-one
            // for json.
            ("a...b", "a_b", "a___b"),
            ("a b", "a_b", "a_b"),
            // Nothing survives the filter, so a bare underscore stands in.
            ("...", "_", "___"),
            ("", "_", "_"),
            // A leading digit is not a valid label start.
            ("9lives", "_9lives", "_9lives"),
            ("0", "_0", "_0"),
        ];

        for (input, want_logfmt, want_json) in cases {
            assert2::check!(
                sanitize_logfmt_field_name(input) == want_logfmt,
                "logfmt {input:?}"
            );
            assert2::check!(
                sanitize_json_field_name(input) == want_json,
                "json {input:?}"
            );
        }
    }

    #[test]
    fn parse_pattern_parts_omits_empty_literals_around_captures() {
        assert_eq!(
            parse_pattern_parts("<method>").unwrap(),
            vec![PatternPart::Capture("method".to_string())]
        );
        assert_eq!(
            parse_pattern_parts("prefix <value>").unwrap(),
            vec![
                PatternPart::Literal("prefix ".to_string()),
                PatternPart::Capture("value".to_string()),
            ]
        );
        assert_eq!(
            parse_pattern_parts("<method> <path>").unwrap(),
            vec![
                PatternPart::Capture("method".to_string()),
                PatternPart::Literal(" ".to_string()),
                PatternPart::Capture("path".to_string()),
            ]
        );
    }

    #[test]
    fn pattern_parser_captures_after_nonzero_prefix() {
        let parser = PatternParser::new("prefix <method> suffix <status>").unwrap();

        assert_eq!(
            parser.captures("prefix GET suffix 500").unwrap(),
            vec![
                ("method".to_string(), "GET".to_string()),
                ("status".to_string(), "500".to_string()),
            ]
        );
    }
}

// === split-modules: generated submodules ===
mod anchored_regex_pattern;
mod decolorize_line;
mod field_value_to_string;
mod flatten_json_field;
mod insert_extracted_field;
mod insert_json_parser_error;
mod insert_logfmt_parser_error;
mod insert_pattern_parser_error;
mod insert_regexp_parser_error;
mod label_matcher;
mod match_op;
mod parse_configured_logfmt_fields;
mod parse_json_fields;
mod parse_logfmt_fields;
mod parse_pattern_parts;
mod parse_selected_json_fields;
mod parse_selected_logfmt_fields;
mod parser_stage;
mod pattern_parse_error;
mod pattern_parser;
mod pattern_part;
mod pipeline_evaluation;
mod pipeline_stage;
mod regexp_parse_error;
mod regexp_parser;
mod sanitize_json_field_name;
mod sanitize_logfmt_field_name;
mod selected_json_value_to_string;
mod stream_query;
mod unpack_json_line;

pub (crate) use anchored_regex_pattern::anchored_regex_pattern;
use decolorize_line::decolorize_line;
use field_value_to_string::field_value_to_string;
use flatten_json_field::flatten_json_field;
pub (crate) use insert_extracted_field::insert_extracted_field;
use insert_json_parser_error::insert_json_parser_error;
use insert_logfmt_parser_error::insert_logfmt_parser_error;
use insert_pattern_parser_error::insert_pattern_parser_error;
use insert_regexp_parser_error::insert_regexp_parser_error;
pub use label_matcher::LabelMatcher;
pub use match_op::MatchOp;
use parse_configured_logfmt_fields::parse_configured_logfmt_fields;
use parse_json_fields::parse_json_fields;
use parse_logfmt_fields::parse_logfmt_fields;
use parse_pattern_parts::parse_pattern_parts;
use parse_selected_json_fields::parse_selected_json_fields;
use parse_selected_logfmt_fields::parse_selected_logfmt_fields;
pub use parser_stage::ParserStage;
use pattern_parse_error::pattern_parse_error;
pub use pattern_parser::PatternParser;
use pattern_part::PatternPart;
pub use pipeline_evaluation::PipelineEvaluation;
pub use pipeline_stage::PipelineStage;
use regexp_parse_error::regexp_parse_error;
pub use regexp_parser::RegexpParser;
use sanitize_json_field_name::sanitize_json_field_name;
use sanitize_logfmt_field_name::sanitize_logfmt_field_name;
use selected_json_value_to_string::selected_json_value_to_string;
pub use stream_query::StreamQuery;
use unpack_json_line::unpack_json_line;
