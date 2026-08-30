use super::*;

pub(crate) fn parse_logfmt_fields(line: &str, fields: &mut Labels) {
    let mut parser = LogfmtParser::new(line);
    loop {
        let previous_pos = parser.pos;
        match parser.next_pair_with_options(false, false) {
            Ok(Some((key, value))) => {
                if parser.pos <= previous_pos {
                    break;
                }
                insert_extracted_field(fields, &sanitize_logfmt_field_name(&key), value);
            }
            Ok(None) | Err(_) => break,
        }
    }
}
