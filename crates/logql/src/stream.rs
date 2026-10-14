use regex::Regex;

use crate::{
    FieldFilter, FieldFilterChain, FieldFilterExpression, JsonParserConfig, LabelFormat,
    LabelSelectionSet, Labels, LineFilter, LineFormat, LogfmtParserConfig, ParseError,
    UnwrapExpression, extract::LogfmtParser,
};

#[cfg(test)]
mod tests {
    use super::{
        PatternParser, PatternPart, parse_pattern_parts, pattern_parser::PatternCapture,
        sanitize_json_field_name, sanitize_logfmt_field_name,
    };

    struct FieldNameCase {
        input: &'static str,
        logfmt: &'static str,
        json: &'static str,
    }

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

        let mut labels = std::collections::BTreeMap::new();
        labels.insert("job".to_string(), "api".to_string());

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
        let cases = [
            FieldNameCase {
                input: "ok_name",
                logfmt: "ok_name",
                json: "ok_name",
            },
            FieldNameCase {
                input: "keeps:colon",
                logfmt: "keeps:colon",
                json: "keeps:colon",
            },
            FieldNameCase {
                input: "Alpha9",
                logfmt: "Alpha9",
                json: "Alpha9",
            },
            // A run of invalid characters: collapsed for logfmt, one-for-one
            // for json.
            FieldNameCase {
                input: "a...b",
                logfmt: "a_b",
                json: "a___b",
            },
            FieldNameCase {
                input: "a b",
                logfmt: "a_b",
                json: "a_b",
            },
            // Nothing survives the filter, so a bare underscore stands in.
            FieldNameCase {
                input: "...",
                logfmt: "_",
                json: "___",
            },
            FieldNameCase {
                input: "",
                logfmt: "_",
                json: "_",
            },
            // A leading digit is not a valid label start.
            FieldNameCase {
                input: "9lives",
                logfmt: "_9lives",
                json: "_9lives",
            },
            FieldNameCase {
                input: "0",
                logfmt: "_0",
                json: "_0",
            },
        ];

        for case in cases {
            assert2::check!(sanitize_logfmt_field_name(case.input) == case.logfmt);
            assert2::check!(sanitize_json_field_name(case.input) == case.json);
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
                PatternCapture {
                    name: "method".to_string(),
                    value: "GET".to_string(),
                },
                PatternCapture {
                    name: "status".to_string(),
                    value: "500".to_string(),
                },
            ]
        );
    }

    #[test]
    fn pattern_parser_captures_the_remainder_when_the_closing_literal_is_missing() {
        let parser = PatternParser::new("status=<status> msg=\"<msg>\"").unwrap();

        assert_eq!(
            parser.captures("status=500 msg=\"unterminated").unwrap(),
            vec![
                PatternCapture {
                    name: "status".to_string(),
                    value: "500".to_string(),
                },
                PatternCapture {
                    name: "msg".to_string(),
                    value: "unterminated".to_string(),
                },
            ]
        );
    }
}

mod anchored_regex_pattern;
mod decolorize_line;
mod insert_extracted_field;
mod insert_json_parser_error;
mod insert_logfmt_parser_error;
mod insert_pattern_parser_error;
mod insert_regexp_parser_error;
mod label_matcher;
mod parse_configured_logfmt_fields;
mod parse_json_fields;
mod parse_logfmt_fields;
mod parse_pattern_parts;
mod parse_selected_json_fields;
mod parse_selected_logfmt_fields;
mod parser_stage;
mod pattern_parser;
mod pattern_part;
mod pipeline_evaluation;
mod pipeline_stage;
mod regexp_parser;
mod sanitize_json_field_name;
mod sanitize_logfmt_field_name;

fn sanitize_field_name(name: &str, collapse_invalid_runs: bool) -> String {
    let mut sanitized = String::new();
    let mut last_was_underscore = false;
    for ch in name.chars() {
        if ch == '_' || ch == ':' || ch.is_ascii_alphanumeric() {
            sanitized.push(ch);
            last_was_underscore = false;
        } else if !collapse_invalid_runs || !last_was_underscore {
            sanitized.push('_');
            last_was_underscore = true;
        }
    }
    if sanitized.is_empty() {
        sanitized.push('_');
    }
    if sanitized
        .chars()
        .next()
        .is_some_and(|ch| ch.is_ascii_digit())
    {
        sanitized.insert(0, '_');
    }
    sanitized
}
mod selected_json_value_to_string;
mod stream_query;
mod unpack_json_line;
mod variant_metadata;

pub(crate) use anchored_regex_pattern::anchored_regex_pattern;
use decolorize_line::decolorize_line;
pub(crate) use insert_extracted_field::insert_extracted_field;
use insert_extracted_field::insert_raw_parsed_field;
use insert_json_parser_error::insert_json_parser_error;
use insert_logfmt_parser_error::insert_logfmt_parser_error;
use insert_pattern_parser_error::insert_pattern_parser_error;
use insert_regexp_parser_error::insert_regexp_parser_error;
pub use krabka_blockstore::LogMatchOp as MatchOp;
pub use label_matcher::LabelMatcher;
use parse_configured_logfmt_fields::parse_configured_logfmt_fields;
use parse_json_fields::parse_json_fields;
use parse_logfmt_fields::parse_logfmt_fields;
use parse_pattern_parts::parse_pattern_parts;
use parse_selected_logfmt_fields::parse_selected_logfmt_fields;
pub use parser_stage::ParserStage;
pub use pattern_parser::PatternParser;
use pattern_part::PatternPart;
pub use pipeline_evaluation::PipelineEvaluation;
pub use pipeline_stage::PipelineStage;
pub use regexp_parser::RegexpParser;
use sanitize_json_field_name::sanitize_json_field_name;
use sanitize_logfmt_field_name::sanitize_logfmt_field_name;
use selected_json_value_to_string::selected_json_value_to_string;
pub use stream_query::StreamQuery;
use unpack_json_line::unpack_json_line;

use crate::error::{
    syntax_parse_error as pattern_parse_error, syntax_parse_error as regexp_parse_error,
};
