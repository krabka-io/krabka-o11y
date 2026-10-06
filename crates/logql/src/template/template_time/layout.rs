use std::fmt::{self, Write as _};

use super::{TemplateTime, calendar::Parts};

pub(in crate::template) const MONTHS: [&str; 12] = [
    "January",
    "February",
    "March",
    "April",
    "May",
    "June",
    "July",
    "August",
    "September",
    "October",
    "November",
    "December",
];
pub(in crate::template) const DAYS: [&str; 7] = [
    "Sunday",
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
];

#[derive(Clone, Copy, Debug)]
pub(super) enum Token {
    Year,
    Year2,
    MonthName(bool),
    Month(bool),
    Weekday(bool),
    Day(u8),
    YearDay(bool),
    Hour(bool, bool),
    Minute(bool),
    Second(bool),
    AmPm(bool),
    Zone,
    Offset {
        z: bool,
        colon: bool,
        seconds: bool,
        short: bool,
    },
    Fraction {
        digits: usize,
        trim: bool,
        separator: char,
    },
}

/// Go1.26.5 time.nextStdChunk tokens, including lowercase-word and mixed
/// fractional-digit guards. The caller emits one literal character otherwise.
pub(super) fn token(rest: &str) -> Option<(Token, usize)> {
    for (pattern, token) in [
        ("January", Token::MonthName(true)),
        ("Monday", Token::Weekday(true)),
        ("MST", Token::Zone),
        ("2006", Token::Year),
        ("002", Token::YearDay(false)),
        ("__2", Token::YearDay(true)),
        ("01", Token::Month(true)),
        ("02", Token::Day(2)),
        ("03", Token::Hour(true, true)),
        ("04", Token::Minute(true)),
        ("05", Token::Second(true)),
        ("06", Token::Year2),
        ("15", Token::Hour(false, true)),
        ("PM", Token::AmPm(true)),
        ("pm", Token::AmPm(false)),
    ] {
        if rest.starts_with(pattern) {
            return Some((token, pattern.len()));
        }
    }
    for (pattern, token) in [
        ("Jan", Token::MonthName(false)),
        ("Mon", Token::Weekday(false)),
    ] {
        if rest.starts_with(pattern)
            && !rest
                .as_bytes()
                .get(pattern.len())
                .is_some_and(u8::is_ascii_lowercase)
        {
            return Some((token, pattern.len()));
        }
    }
    if rest.starts_with("_2") && !rest.starts_with("_2006") {
        return Some((Token::Day(1), 2));
    }
    if let Some(prefix) = rest
        .as_bytes()
        .first()
        .filter(|byte| matches!(byte, b'-' | b'Z'))
    {
        for (pattern, colon, seconds, short) in [
            ("07:00:00", true, true, false),
            ("070000", false, true, false),
            ("07:00", true, false, false),
            ("0700", false, false, false),
            ("07", false, false, true),
        ] {
            if rest[1..].starts_with(pattern) {
                return Some((
                    Token::Offset {
                        z: *prefix == b'Z',
                        colon,
                        seconds,
                        short,
                    },
                    pattern.len() + 1,
                ));
            }
        }
    }
    if rest.starts_with(['.', ','])
        && let Some(&digit) = rest
            .as_bytes()
            .get(1)
            .filter(|digit| matches!(digit, b'0' | b'9'))
    {
        let count = rest.as_bytes()[1..]
            .iter()
            .take_while(|byte| **byte == digit)
            .count();
        if !rest
            .as_bytes()
            .get(count + 1)
            .is_some_and(u8::is_ascii_digit)
        {
            return Some((
                Token::Fraction {
                    digits: count,
                    trim: digit == b'9',
                    separator: char::from(rest.as_bytes()[0]),
                },
                count + 1,
            ));
        }
    }
    match rest.as_bytes().first()? {
        b'1' => Some((Token::Month(false), 1)),
        b'2' => Some((Token::Day(0), 1)),
        b'3' => Some((Token::Hour(true, false), 1)),
        b'4' => Some((Token::Minute(false), 1)),
        b'5' => Some((Token::Second(false), 1)),
        _ => None,
    }
}

pub(super) fn format(time: &TemplateTime, mut rest: &str) -> String {
    let fields = time.parts();
    let (zone, offset) = time.zone();
    let mut output = String::new();
    while !rest.is_empty() {
        let Some((token, length)) = token(rest) else {
            let ch = rest.chars().next().expect("nonempty layout");
            output.push(ch);
            rest = &rest[ch.len_utf8()..];
            continue;
        };
        rest = &rest[length..];
        write_token(&mut output, token, &fields, time.nanos, &zone, offset)
            .expect("writing to a String cannot fail");
    }
    output
}

fn write_token(
    output: &mut String,
    token: Token,
    fields: &Parts,
    nanos: u32,
    zone: &str,
    offset: i32,
) -> fmt::Result {
    match token {
        Token::Year => write!(
            output,
            "{}{:04}",
            if fields.year < 0 { "-" } else { "" },
            fields.year.unsigned_abs()
        ),
        Token::Year2 => write!(output, "{:02}", fields.year.unsigned_abs() % 100),
        Token::MonthName(long) => {
            let name = MONTHS[usize::try_from(fields.month - 1).expect("month index is in 0..12")];
            output.write_str(if long { name } else { &name[..3] })
        }
        Token::Month(zero) => write_number(output, fields.month, if zero { 2 } else { 0 }, false),
        Token::Weekday(long) => {
            let name = DAYS[usize::try_from(fields.weekday).expect("weekday is in 0..7")];
            output.write_str(if long { name } else { &name[..3] })
        }
        Token::Day(width) => write_number(output, fields.day, usize::from(width), width == 1),
        Token::YearDay(space) => write_number(output, fields.yearday, 3, space),
        Token::Hour(twelve, zero) => {
            let hour = if twelve {
                let h = fields.hour % 12;
                if h == 0 { 12 } else { h }
            } else {
                fields.hour
            };
            write_number(output, hour, if zero { 2 } else { 0 }, false)
        }
        Token::Minute(zero) => write_number(output, fields.minute, if zero { 2 } else { 0 }, false),
        Token::Second(zero) => write_number(output, fields.second, if zero { 2 } else { 0 }, false),
        Token::AmPm(upper) => output.write_str(match (upper, fields.hour >= 12) {
            (true, true) => "PM",
            (true, false) => "AM",
            (false, true) => "pm",
            (false, false) => "am",
        }),
        Token::Zone if !zone.is_empty() => output.write_str(zone),
        Token::Zone => output.write_str(&offset_format(offset, false, false, false)),
        Token::Offset { z: true, .. } if offset == 0 => output.write_char('Z'),
        Token::Offset {
            colon,
            seconds,
            short,
            ..
        } => output.write_str(&offset_format(offset, colon, seconds, short)),
        Token::Fraction {
            digits,
            trim,
            separator,
        } => {
            let fraction = format!("{nanos:09}");
            let fraction = &fraction[..digits.min(9)];
            let fraction = if trim {
                fraction.trim_end_matches('0')
            } else {
                fraction
            };
            if !fraction.is_empty() {
                output.write_char(separator)?;
                output.write_str(fraction)?;
            }
            Ok(())
        }
    }
}

fn write_number(output: &mut String, value: u32, width: usize, space: bool) -> fmt::Result {
    if space {
        write!(output, "{value:width$}", width = width.max(2))
    } else {
        write!(output, "{value:0width$}")
    }
}
fn offset_format(offset: i32, colon: bool, seconds: bool, short: bool) -> String {
    // Go chooses sign after truncating to whole minutes, so offsets in -59..0
    // have '+' sign even in the seconds form.
    let minutes = offset / 60;
    let sign = if minutes < 0 { "-" } else { "+" };
    let minutes = minutes.unsigned_abs();
    let separator = if colon { ":" } else { "" };
    let mut value = format!("{sign}{:02}", minutes / 60);
    if !short {
        write!(value, "{separator}{:02}", minutes % 60).expect("writing to a String cannot fail");
    }
    if seconds {
        write!(
            value,
            "{separator}{:02}",
            (if offset / 60 < 0 { -offset } else { offset }) % 60
        )
        .expect("writing to a String cannot fail");
    }
    value
}
