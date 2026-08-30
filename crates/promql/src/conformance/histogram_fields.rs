use super::{BTreeMap, Line, Result, parse_error};

pub(crate) fn histogram_fields<'a>(content: &'a str, line: Line<'_>) -> Result<BTreeMap<&'a str, &'a str>> {
    let mut fields = BTreeMap::new();
    let mut index = 0;
    let bytes = content.as_bytes();
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index == bytes.len() {
            break;
        }
        let name_start = index;
        while index < bytes.len() && bytes[index] != b':' {
            if bytes[index].is_ascii_whitespace() {
                return Err(parse_error(
                    line,
                    "histogram field name must be followed by `:`",
                ));
            }
            index += 1;
        }
        if index == bytes.len() {
            return Err(parse_error(line, "histogram field missing `:`"));
        }
        let name = &content[name_start..index];
        index += 1;
        let value_start = index;
        if index < bytes.len() && bytes[index] == b'[' {
            let Some(end) = content[index..].find(']') else {
                return Err(parse_error(line, "unterminated histogram bucket list"));
            };
            index += end + 1;
        } else {
            while index < bytes.len() && !bytes[index].is_ascii_whitespace() {
                index += 1;
            }
        }
        fields.insert(name, &content[value_start..index]);
    }
    Ok(fields)
}
