use super::*;

pub(crate) fn insert_regexp_parser_error(fields: &mut Labels) {
    insert_extracted_field(fields, "__error__", "RegexpParserErr".to_string());
    insert_extracted_field(
        fields,
        "__error_details__",
        "regexp parser failed to match log line".to_string(),
    );
}
