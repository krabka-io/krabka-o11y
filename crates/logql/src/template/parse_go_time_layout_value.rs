use super::{
    ParsedTemplateDate, match_template_literal, parse_fixed_template_digits,
    parse_template_fractional_nanoseconds, parse_template_timezone_offset,
    parse_variable_template_digits,
};

pub(crate) fn parse_go_time_layout_value(layout: &str, value: &str) -> Option<ParsedTemplateDate> {
    let mut parsed = ParsedTemplateDate {
        year: 0,
        month: 1,
        day: 1,
        hour: 0,
        minute: 0,
        second: 0,
        nanosecond: 0,
        offset_seconds: None,
    };
    let mut value_pos = 0usize;
    let mut rest = layout;
    while !rest.is_empty() {
        if let Some(next) = rest.strip_prefix("2006") {
            parsed.year = parse_fixed_template_digits(value, &mut value_pos, 4)?.cast_signed();
            rest = next;
        } else if let Some(next) = rest.strip_prefix("06") {
            parsed.year =
                2000 + parse_fixed_template_digits(value, &mut value_pos, 2)?.cast_signed();
            rest = next;
        } else if let Some(next) = rest.strip_prefix("15") {
            parsed.hour = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("04") {
            parsed.minute = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("05") {
            parsed.second = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("01") {
            parsed.month = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix('1') {
            parsed.month = parse_variable_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("02") {
            parsed.day = parse_fixed_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix('2') {
            parsed.day = parse_variable_template_digits(value, &mut value_pos, 2)?;
            rest = next;
        } else if let Some(next) = rest.strip_prefix("Z07:00") {
            parsed.offset_seconds = Some(parse_template_timezone_offset(value, &mut value_pos)?);
            rest = next;
        } else if let Some(next) = rest.strip_prefix("-07:00") {
            parsed.offset_seconds = Some(parse_template_timezone_offset(value, &mut value_pos)?);
            rest = next;
        } else if let Some(fraction_rest) = rest.strip_prefix('.') {
            let digits = fraction_rest
                .chars()
                .take_while(|ch| *ch == '0' || *ch == '9')
                .count();
            if digits == 0 {
                match_template_literal(value, &mut value_pos, '.')?;
                rest = fraction_rest;
            } else {
                parsed.nanosecond =
                    parse_template_fractional_nanoseconds(value, &mut value_pos, digits)?;
                rest = &fraction_rest[digits..];
            }
        } else {
            let ch = rest.chars().next()?;
            match_template_literal(value, &mut value_pos, ch)?;
            rest = &rest[ch.len_utf8()..];
        }
    }
    (value_pos == value.len()).then_some(parsed)
}
