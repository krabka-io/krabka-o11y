use super::{skip_ws, matching_delimiter, is_ident_start, consume_ident, consume_number_duration};

pub(crate) fn offset_operand(chars: &[char], after_keyword: usize) -> Option<(String, usize)> {
    let mut start = skip_ws(chars, after_keyword);
    if start >= chars.len() {
        return None;
    }

    let mut sign_start = None;
    if chars[start] == '+' || chars[start] == '-' {
        sign_start = Some(start);
        start = skip_ws(chars, start + 1);
    }
    if start >= chars.len() {
        return None;
    }

    if chars[start] == '(' {
        let end = matching_delimiter(chars, start, '(', ')').ok()? + 1;
        let mut operand = chars[start..end].iter().collect::<String>();
        if let Some(sign_start) = sign_start {
            operand.insert(0, chars[sign_start]);
        }
        return Some((operand, end));
    }

    let end = if is_ident_start(chars[start]) {
        let ident_end = consume_ident(chars, start);
        let call_start = skip_ws(chars, ident_end);
        if chars.get(call_start) == Some(&'(') {
            matching_delimiter(chars, call_start, '(', ')').ok()? + 1
        } else {
            ident_end
        }
    } else if chars[start].is_ascii_digit() || chars[start] == '.' {
        consume_number_duration(chars, start)
    } else {
        return None;
    };

    let operand_start = sign_start.unwrap_or(start);
    Some((chars[operand_start..end].iter().collect::<String>(), end))
}
