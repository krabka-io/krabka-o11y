use super::{
    DurationExprContext, DurationExprParser, Result, is_zero, matching_delimiter,
    normalize_range_duration_content, offset_operand, seconds_to_duration_literal,
    starts_offset_keyword,
};

pub(crate) fn normalize_duration_expressions(
    query: &str,
    context: DurationExprContext,
) -> Result<String> {
    let chars = query.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(query.len());
    let mut index = 0;
    let mut quote = None;

    while index < chars.len() {
        let ch = chars[index];
        if let Some(quote_ch) = quote {
            out.push(ch);
            if ch == '\\' {
                if let Some(next) = chars.get(index + 1) {
                    out.push(*next);
                    index += 2;
                    continue;
                }
            } else if ch == quote_ch {
                quote = None;
            }
            index += 1;
            continue;
        }

        if ch == '"' || ch == '\'' || ch == '`' {
            quote = Some(ch);
            out.push(ch);
            index += 1;
            continue;
        }

        if ch == '[' {
            let end = matching_delimiter(&chars, index, '[', ']')?;
            let bracket_content = chars[index + 1..end].iter().collect::<String>();
            out.push('[');
            out.push_str(&normalize_range_duration_content(
                &bracket_content,
                context,
            )?);
            out.push(']');
            index = end + 1;
            continue;
        }

        if starts_offset_keyword(&chars, index) {
            let after_keyword = index + "offset".len();
            if let Some((operand, end)) = offset_operand(&chars, after_keyword) {
                let seconds = DurationExprParser::new(&operand, context).parse()?;
                if !is_zero(seconds) {
                    out.push_str(&chars[index..after_keyword].iter().collect::<String>());
                    out.push(' ');
                    if seconds < 0.0 {
                        out.push('-');
                        out.push_str(&seconds_to_duration_literal(-seconds)?);
                    } else {
                        out.push_str(&seconds_to_duration_literal(seconds)?);
                    }
                }
                index = end;
                continue;
            }
        }

        out.push(ch);
        index += 1;
    }

    Ok(out)
}
