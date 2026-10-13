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
    Hour {
        twelve_hour: bool,
        zero_pad: bool,
    },
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

pub(super) struct TokenMatch {
    pub token: Token,
    pub length: usize,
}

struct TokenPattern {
    pattern: &'static str,
    token: Token,
}

struct OffsetPattern {
    pattern: &'static str,
    colon: bool,
    seconds: bool,
    short: bool,
}

/// Go1.26.5 time.nextStdChunk tokens, including lowercase-word and mixed
/// fractional-digit guards. The caller emits one literal character otherwise.
pub(super) fn token(rest: &str) -> Option<TokenMatch> {
    for entry in [
        TokenPattern {
            pattern: "January",
            token: Token::MonthName(true),
        },
        TokenPattern {
            pattern: "Monday",
            token: Token::Weekday(true),
        },
        TokenPattern {
            pattern: "MST",
            token: Token::Zone,
        },
        TokenPattern {
            pattern: "2006",
            token: Token::Year,
        },
        TokenPattern {
            pattern: "002",
            token: Token::YearDay(false),
        },
        TokenPattern {
            pattern: "__2",
            token: Token::YearDay(true),
        },
        TokenPattern {
            pattern: "01",
            token: Token::Month(true),
        },
        TokenPattern {
            pattern: "02",
            token: Token::Day(2),
        },
        TokenPattern {
            pattern: "03",
            token: Token::Hour {
                twelve_hour: true,
                zero_pad: true,
            },
        },
        TokenPattern {
            pattern: "04",
            token: Token::Minute(true),
        },
        TokenPattern {
            pattern: "05",
            token: Token::Second(true),
        },
        TokenPattern {
            pattern: "06",
            token: Token::Year2,
        },
        TokenPattern {
            pattern: "15",
            token: Token::Hour {
                twelve_hour: false,
                zero_pad: true,
            },
        },
        TokenPattern {
            pattern: "PM",
            token: Token::AmPm(true),
        },
        TokenPattern {
            pattern: "pm",
            token: Token::AmPm(false),
        },
    ] {
        if rest.starts_with(entry.pattern) {
            return Some(TokenMatch {
                token: entry.token,
                length: entry.pattern.len(),
            });
        }
    }
    for entry in [
        TokenPattern {
            pattern: "Jan",
            token: Token::MonthName(false),
        },
        TokenPattern {
            pattern: "Mon",
            token: Token::Weekday(false),
        },
    ] {
        if rest.starts_with(entry.pattern)
            && !rest
                .as_bytes()
                .get(entry.pattern.len())
                .is_some_and(u8::is_ascii_lowercase)
        {
            return Some(TokenMatch {
                token: entry.token,
                length: entry.pattern.len(),
            });
        }
    }
    if rest.starts_with("_2") && !rest.starts_with("_2006") {
        return Some(TokenMatch {
            token: Token::Day(1),
            length: 2,
        });
    }
    if let Some(prefix) = rest
        .as_bytes()
        .first()
        .filter(|byte| matches!(byte, b'-' | b'Z'))
    {
        for pattern in [
            OffsetPattern {
                pattern: "07:00:00",
                colon: true,
                seconds: true,
                short: false,
            },
            OffsetPattern {
                pattern: "070000",
                colon: false,
                seconds: true,
                short: false,
            },
            OffsetPattern {
                pattern: "07:00",
                colon: true,
                seconds: false,
                short: false,
            },
            OffsetPattern {
                pattern: "0700",
                colon: false,
                seconds: false,
                short: false,
            },
            OffsetPattern {
                pattern: "07",
                colon: false,
                seconds: false,
                short: true,
            },
        ] {
            if rest[1..].starts_with(pattern.pattern) {
                return Some(TokenMatch {
                    token: Token::Offset {
                        z: *prefix == b'Z',
                        colon: pattern.colon,
                        seconds: pattern.seconds,
                        short: pattern.short,
                    },
                    length: pattern.pattern.len() + 1,
                });
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
            return Some(TokenMatch {
                token: Token::Fraction {
                    digits: count,
                    trim: digit == b'9',
                    separator: char::from(rest.as_bytes()[0]),
                },
                length: count + 1,
            });
        }
    }
    match rest.as_bytes().first()? {
        b'1' => Some(TokenMatch {
            token: Token::Month(false),
            length: 1,
        }),
        b'2' => Some(TokenMatch {
            token: Token::Day(0),
            length: 1,
        }),
        b'3' => Some(TokenMatch {
            token: Token::Hour {
                twelve_hour: true,
                zero_pad: false,
            },
            length: 1,
        }),
        b'4' => Some(TokenMatch {
            token: Token::Minute(false),
            length: 1,
        }),
        b'5' => Some(TokenMatch {
            token: Token::Second(false),
            length: 1,
        }),
        _ => None,
    }
}

pub(super) fn format(time: &TemplateTime, mut rest: &str) -> String {
    let fields = time.parts();
    let zone = time.zone();
    let mut output = String::new();
    while !rest.is_empty() {
        let Some(token_match) = token(rest) else {
            let ch = rest.chars().next().expect("nonempty layout");
            output.push(ch);
            rest = &rest[ch.len_utf8()..];
            continue;
        };
        rest = &rest[token_match.length..];
        write_token(
            &mut output,
            token_match.token,
            &fields,
            time.nanos,
            &zone.name,
            zone.offset,
        )
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
        Token::Hour {
            twelve_hour,
            zero_pad,
        } => {
            let hour = if twelve_hour {
                let h = fields.hour % 12;
                if h == 0 { 12 } else { h }
            } else {
                fields.hour
            };
            write_number(output, hour, if zero_pad { 2 } else { 0 }, false)
        }
        Token::Minute(zero) => write_number(output, fields.minute, if zero { 2 } else { 0 }, false),
        Token::Second(zero) => write_number(output, fields.second, if zero { 2 } else { 0 }, false),
        Token::AmPm(upper) => {
            let is_pm = fields.hour >= 12;
            output.write_str(match upper {
                true if is_pm => "PM",
                true => "AM",
                false if is_pm => "pm",
                false => "am",
            })
        }
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
