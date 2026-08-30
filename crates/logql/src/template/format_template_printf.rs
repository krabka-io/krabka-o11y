use super::*;

pub(crate) fn format_template_printf(args: &[String]) -> String {
    let Some(format) = args.first() else {
        return String::new();
    };

    let mut formatted = String::new();
    let mut chars = format.chars().peekable();
    let mut values = args.iter().skip(1);
    while let Some(ch) = chars.next() {
        if ch != '%' {
            formatted.push(ch);
            continue;
        }
        if chars.peek() == Some(&'%') {
            chars.next();
            formatted.push('%');
            continue;
        }

        let left_align = if chars.peek() == Some(&'-') {
            chars.next();
            true
        } else {
            false
        };
        let width = consume_template_printf_number(&mut chars);
        let precision = if chars.peek() == Some(&'.') {
            chars.next();
            Some(consume_template_printf_number(&mut chars).unwrap_or(0))
        } else {
            None
        };

        let Some(verb) = chars.next() else {
            break;
        };
        if verb != 's' {
            formatted.push('%');
            if left_align {
                formatted.push('-');
            }
            if let Some(width) = width {
                formatted.push_str(&width.to_string());
            }
            if let Some(precision) = precision {
                formatted.push('.');
                formatted.push_str(&precision.to_string());
            }
            formatted.push(verb);
            continue;
        }

        let value = values.next().map(String::as_str).unwrap_or_default();
        formatted.push_str(&format_template_printf_string(
            value, width, precision, left_align,
        ));
    }
    formatted
}
