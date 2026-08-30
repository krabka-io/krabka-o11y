use super::*;

pub(crate) fn insert_logfmt_parser_error(fields: &mut Labels, details: String) {
    insert_extracted_field(fields, "__error__", "LogfmtParserErr".to_string());
    insert_extracted_field(fields, "__error_details__", details);
}
