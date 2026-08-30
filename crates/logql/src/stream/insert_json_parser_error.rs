use super::{Labels, insert_extracted_field};

pub(crate) fn insert_json_parser_error(fields: &mut Labels) {
    insert_extracted_field(fields, "__error__", "JSONParserErr".to_string());
    insert_extracted_field(
        fields,
        "__error_details__",
        "Value looks like object, but can't find closing '}' symbol".to_string(),
    );
}
