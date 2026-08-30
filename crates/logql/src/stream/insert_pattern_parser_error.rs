use super::*;

pub(crate) fn insert_pattern_parser_error(fields: &mut Labels) {
    insert_extracted_field(fields, "__error__", "PatternParserErr".to_string());
    insert_extracted_field(
        fields,
        "__error_details__",
        "pattern parser failed to match log line".to_string(),
    );
}
