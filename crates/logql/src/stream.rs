use regex::Regex;

use crate::{
    FieldFilter, FieldFilterChain, FieldFilterExpression, JsonParserConfig, LabelFormat,
    LabelSelectionSet, Labels, LineFilter, LineFormat, LogfmtParserConfig, ParseError,
    UnwrapExpression, extract::LogfmtParser,
};

// Insert Extracted Field
pub(crate) fn insert_extracted_field(fields: &mut Labels, name: &str, value: String) {
    if fields.contains_key(name) {
        fields.entry(format!("{name}_extracted")).or_insert(value);
    } else {
        fields.insert(name.to_string(), value);
    }
}

// Parser collection is separate from insertion into the stream labels. This
// preserves the first parsed occurrence without mistaking an earlier parsed
// field for a stream-label collision.
pub(crate) fn insert_raw_parsed_field(fields: &mut Labels, name: &str, value: String) {
    fields.entry(name.to_string()).or_insert(value);
}

// Parse Configured Logfmt Fields
pub(crate) fn parse_configured_logfmt_fields(
    line: &str,
    fields: &mut Labels,
    config: &LogfmtParserConfig,
) {
    let mut parser = LogfmtParser::new(line);
    loop {
        let previous_pos = parser.pos;
        match parser.next_pair_with_options(config.keep_empty(), config.strict()) {
            Ok(Some(field)) => {
                if parser.pos <= previous_pos {
                    break;
                }
                insert_raw_parsed_field(
                    fields,
                    &sanitize_logfmt_field_name(&field.key),
                    field.value,
                );
            }
            Ok(None) => break,
            Err(details) => {
                insert_logfmt_parser_error(fields, details);
                break;
            }
        }
    }
}

// Parse Pattern Parts
pub(crate) fn parse_pattern_parts(pattern: &str) -> Result<Vec<PatternPart>, ParseError> {
    let mut pos = 0;
    let mut parts = Vec::new();
    let mut has_named_capture = false;
    let mut previous_capture = false;
    let mut separator_since_capture = String::new();

    while let Some(open_offset) = pattern[pos..].find('<') {
        let literal_start = pos;
        let open = pos.saturating_add(open_offset);
        let literal = &pattern[literal_start..open];
        if !literal.is_empty() {
            separator_since_capture.push_str(literal);
            parts.push(PatternPart::Literal(literal.to_string()));
        }

        let capture_start = open.saturating_add(1);
        let close_offset = pattern[capture_start..]
            .find('>')
            .ok_or_else(|| pattern_parse_error("expected closing pattern capture"))?;
        let close = capture_start.saturating_add(close_offset);
        let name = &pattern[capture_start..close];
        if name.is_empty() {
            return Err(pattern_parse_error("expected pattern capture name"));
        }
        if previous_capture && !separator_since_capture.chars().any(char::is_whitespace) {
            return Err(pattern_parse_error(
                "expected whitespace between pattern captures",
            ));
        }
        if name != "_" {
            has_named_capture = true;
        }
        parts.push(PatternPart::Capture(name.to_string()));
        previous_capture = true;
        separator_since_capture.clear();
        pos = close.saturating_add(1);
    }

    let literal = &pattern[pos..];
    if !literal.is_empty() {
        separator_since_capture.push_str(literal);
        parts.push(PatternPart::Literal(literal.to_string()));
    }

    if !has_named_capture {
        return Err(pattern_parse_error(
            "pattern parser requires at least one named capture",
        ));
    }
    Ok(parts)
}

// Parse Selected Logfmt Fields
pub(crate) fn parse_selected_logfmt_fields(
    line: &str,
    fields: &mut Labels,
    config: &LogfmtParserConfig,
    has_collision: impl Fn(&str) -> bool,
) {
    let mut parsed = Labels::new();
    let mut parser = LogfmtParser::new(line);
    loop {
        let previous_pos = parser.pos;
        match parser.next_pair_with_options(true, config.strict()) {
            Ok(Some(field)) => {
                if parser.pos <= previous_pos {
                    break;
                }
                parsed.entry(field.key).or_insert(field.value);
            }
            Ok(None) => break,
            Err(details) => {
                insert_logfmt_parser_error(fields, details);
                break;
            }
        }
    }

    for extraction in config.extractions() {
        let value = parsed.get(extraction.source()).cloned();
        if value.is_none() && has_collision(extraction.destination()) {
            continue;
        }
        insert_raw_parsed_field(fields, extraction.destination(), value.unwrap_or_default());
    }
}

// Sanitize Json Field Name
pub(crate) fn sanitize_json_field_name(name: &str) -> String {
    sanitize_field_name(name, false)
}

// Sanitize Logfmt Field Name
pub(crate) fn sanitize_logfmt_field_name(name: &str) -> String {
    sanitize_field_name(name, true)
}

// Selected Json Value To String
use serde_json::value::RawValue;

pub(crate) fn selected_json_value_to_string(value: &RawValue) -> String {
    let text = value.get();
    match text.as_bytes().first() {
        Some(b'{') => text.to_string(),
        Some(b'"') => serde_json::from_str::<String>(text)
            .unwrap_or_default()
            .replace('\u{fffd}', " "),
        Some(b'[') => {
            // Loki's unescapeJSONString receives the entire array token,
            // retaining its whitespace but unescaping embedded string escapes.
            let mut quoted = String::from("\"");
            let mut escaped = false;
            for character in text.chars() {
                if escaped {
                    quoted.push(character);
                    escaped = false;
                } else {
                    match character {
                        '\\' => {
                            quoted.push(character);
                            escaped = true;
                        }
                        '"' => quoted.push_str("\\\""),
                        '\n' => quoted.push_str("\\n"),
                        '\r' => quoted.push_str("\\r"),
                        '\t' => quoted.push_str("\\t"),
                        _ => quoted.push(character),
                    }
                }
            }
            quoted.push('"');
            serde_json::from_str::<String>(&quoted)
                .unwrap_or_default()
                .replace('\u{fffd}', " ")
        }
        _ if text == "null" => String::new(),
        _ => text.to_string(),
    }
}

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
mod label_matcher;
mod parse_json_fields;
mod parse_selected_json_fields;
mod parser_stage;
mod pattern_parser;
mod pattern_part;
mod pipeline_evaluation;
mod pipeline_stage;
mod regexp_parser;

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
mod stream_query;
mod unpack_json_line;
mod variant_metadata;
pub use krabka_blockstore::LogMatchOp as MatchOp;
pub use label_matcher::LabelMatcher;
use parse_json_fields::parse_json_fields;
pub use parser_stage::ParserStage;
pub use pattern_parser::PatternParser;
use pattern_part::PatternPart;
pub use pipeline_evaluation::PipelineEvaluation;
pub use pipeline_stage::PipelineStage;
pub use regexp_parser::RegexpParser;
pub use stream_query::StreamQuery;
use unpack_json_line::unpack_json_line;

use crate::error::{
    syntax_parse_error as pattern_parse_error, syntax_parse_error as regexp_parse_error,
};

pub(crate) fn anchored_regex_pattern(pattern: &str) -> String {
    format!("^(?:{pattern})$")
}

pub(crate) fn decolorize_line(line: &str) -> String {
    Regex::new(r"\x1b\[[0-?]*[ -/]*[@-~]")
        .expect("ANSI CSI regex is valid")
        .replace_all(line, "")
        .into_owned()
}

pub(crate) fn insert_json_parser_error(fields: &mut Labels) {
    insert_extracted_field(fields, "__error__", "JSONParserErr".to_string());
    insert_extracted_field(
        fields,
        "__error_details__",
        "Value looks like object, but can't find closing '}' symbol".to_string(),
    );
}

pub(crate) fn insert_logfmt_parser_error(fields: &mut Labels, details: String) {
    insert_extracted_field(fields, "__error__", "LogfmtParserErr".to_string());
    insert_extracted_field(fields, "__error_details__", details);
}

pub(crate) fn insert_pattern_parser_error(fields: &mut Labels) {
    insert_extracted_field(fields, "__error__", "PatternParserErr".to_string());
    insert_extracted_field(
        fields,
        "__error_details__",
        "pattern parser failed to match log line".to_string(),
    );
}

pub(crate) fn insert_regexp_parser_error(fields: &mut Labels) {
    insert_extracted_field(fields, "__error__", "RegexpParserErr".to_string());
    insert_extracted_field(
        fields,
        "__error_details__",
        "regexp parser failed to match log line".to_string(),
    );
}

pub(crate) fn parse_logfmt_fields(line: &str, fields: &mut Labels) {
    let mut parser = LogfmtParser::new(line);
    loop {
        let previous_pos = parser.pos;
        match parser.next_pair_with_options(false, false) {
            Ok(Some(field)) => {
                if parser.pos <= previous_pos {
                    break;
                }
                insert_raw_parsed_field(
                    fields,
                    &sanitize_logfmt_field_name(&field.key),
                    field.value,
                );
            }
            Ok(None) | Err(_) => break,
        }
    }
}
