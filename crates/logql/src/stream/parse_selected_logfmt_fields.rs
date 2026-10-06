use super::{
    Labels, LogfmtParser, LogfmtParserConfig, insert_logfmt_parser_error, insert_raw_parsed_field,
};

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
        let value = parsed.get(extraction.source()).cloned();
        if value.is_none() && has_collision(extraction.destination()) {
            continue;
        }
        insert_raw_parsed_field(fields, extraction.destination(), value.unwrap_or_default());
    }
}
