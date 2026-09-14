use super::{
    JsonParserConfig, Labels, insert_extracted_field, insert_json_parser_error,
    selected_json_value_to_string,
};

pub(crate) fn parse_selected_json_fields(
    line: &str,
    fields: &mut Labels,
    config: &JsonParserConfig,
) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
        insert_json_parser_error(fields);
        return;
    };

    for extraction in config.extractions() {
        insert_extracted_field(
            fields,
            extraction.destination(),
            extraction
                .evaluate(&value)
                .map_or_else(String::new, selected_json_value_to_string),
        );
    }
}
