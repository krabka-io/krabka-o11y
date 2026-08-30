use super::{Labels, JsonParserConfig, insert_json_parser_error, insert_extracted_field, selected_json_value_to_string};

pub(crate) fn parse_selected_json_fields(line: &str, fields: &mut Labels, config: &JsonParserConfig) {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
        insert_json_parser_error(fields);
        return;
    };

    for extraction in config.extractions() {
        if let Some(value) = extraction.evaluate(&value) {
            insert_extracted_field(
                fields,
                extraction.destination(),
                selected_json_value_to_string(value),
            );
        }
    }
}
