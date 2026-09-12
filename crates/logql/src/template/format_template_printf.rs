use super::{consume_template_printf_number, format_template_printf_string};

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
        let value = values.next().map(String::as_str).unwrap_or_default();

        if matches!(verb, 'f' | 'F') {
            let Ok(value) = value.parse::<f64>() else {
                formatted.push_str("%!f(string)");
                continue;
            };
            let precision = precision.unwrap_or(6);
            let value = format!("{value:.precision$}");
            formatted.push_str(&format_template_printf_string(
                &value, width, None, left_align,
            ));
            continue;
        }
        if !matches!(verb, 's' | 'v') {
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

        formatted.push_str(&format_template_printf_string(
            value, width, precision, left_align,
        ));
    }
    formatted
}
