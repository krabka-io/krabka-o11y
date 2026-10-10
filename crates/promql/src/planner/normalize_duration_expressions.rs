use super::{
    DurationExprContext, DurationExprParser, QuotedCopy, Result, consume_ident, is_ident_char,
    is_zero, matching_delimiter, ms_to_seconds, normalize_range_duration_content, offset_operand,
    seconds_to_duration_literal, skip_ws, starts_offset_keyword,
};

pub(crate) fn normalize_duration_expressions(
    query: &str,
    context: DurationExprContext,
) -> Result<String> {
    let chars = query.chars().collect::<Vec<_>>();
    let mut out = String::with_capacity(query.len());
    let mut index = 0;
    let mut quoted = QuotedCopy::default();

    while index < chars.len() {
        if quoted.copy(&chars, &mut index, &mut out) {
            continue;
        }
        let ch = chars[index];

        if cfg!(feature = "experimental-functions")
            && matches!(ch, 's' | 'e')
            && (index == 0 || !is_ident_char(chars[index - 1]))
            && chars[..index].iter().rev().find(|ch| !ch.is_whitespace()) != Some(&'@')
        {
            let word_end = consume_ident(&chars, index);
            let name = chars[index..word_end].iter().collect::<String>();
            let boundary_ms = match name.as_str() {
                "start" => Some(context.start_ms),
                "end" => Some(context.end_ms),
                _ => None,
            };
            let open = skip_ws(&chars, word_end);
            let close = skip_ws(&chars, open + 1);
            if let Some(boundary_ms) = boundary_ms
                && chars.get(open) == Some(&'(')
                && chars.get(close) == Some(&')')
            {
                // Parentheses preserve precedence for bounds before the Unix epoch.
                out.push('(');
                out.push_str(&ms_to_seconds(boundary_ms).to_string());
                out.push(')');
                index = close + 1;
                continue;
            }
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
