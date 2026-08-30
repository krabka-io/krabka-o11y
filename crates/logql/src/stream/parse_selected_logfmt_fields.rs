use super::{Labels, LogfmtParserConfig, LogfmtParser, insert_logfmt_parser_error, insert_extracted_field};

pub(crate) fn parse_selected_logfmt_fields(line: &str, fields: &mut Labels, config: &LogfmtParserConfig) {
    let mut parsed = Labels::new();
    let mut parser = LogfmtParser::new(line);
    loop {
        let previous_pos = parser.pos;
        match parser.next_pair_with_options(true, config.strict()) {
            Ok(Some((key, value))) => {
                if parser.pos <= previous_pos {
                    break;
                }
                parsed.entry(key).or_insert(value);
            }
            Ok(None) => break,
            Err(details) => {
                insert_logfmt_parser_error(fields, details);
                break;
            }
        }
    }

    for extraction in config.extractions() {
        let value = parsed.get(extraction.source()).cloned().unwrap_or_default();
        insert_extracted_field(fields, extraction.destination(), value);
    }
}
