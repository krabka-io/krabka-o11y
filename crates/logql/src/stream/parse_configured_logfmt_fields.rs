use super::*;

pub(crate) fn parse_configured_logfmt_fields(line: &str, fields: &mut Labels, config: &LogfmtParserConfig) {
    let mut parser = LogfmtParser::new(line);
    loop {
        let previous_pos = parser.pos;
        match parser.next_pair_with_options(config.keep_empty(), config.strict()) {
            Ok(Some((key, value))) => {
                if parser.pos <= previous_pos {
                    break;
                }
                insert_extracted_field(fields, &sanitize_logfmt_field_name(&key), value);
            }
            Ok(None) => break,
            Err(details) => {
                insert_logfmt_parser_error(fields, details);
                break;
            }
        }
    }
}
