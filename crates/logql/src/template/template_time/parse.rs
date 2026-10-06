use chrono::Datelike;

use super::{
    Location, TemplateTime, calendar,
    layout::{self, Token},
};

pub(super) fn parse(mut layout: &str, zone: &str, mut value: &str) -> Option<TemplateTime> {
    let mut year = 0;
    let mut month = None;
    let mut day = None;
    let mut year_day = None;
    let mut hour = 0;
    let mut minute = 0;
    let mut second = 0;
    let mut nanos = 0;
    let mut am_pm = None;
    let mut offset = None;
    let mut abbreviation = String::new();
    let mut explicit_utc = false;
    while !layout.is_empty() {
        let Some((token, length)) = layout::token(layout) else {
            let ch = layout.chars().next()?;
            if ch == ' ' {
                if !value.is_empty() && !value.starts_with(' ') {
                    return None;
                }
                layout = layout.trim_start_matches(' ');
                value = value.trim_start_matches(' ');
                continue;
            }
            value = value.strip_prefix(ch)?;
            layout = &layout[ch.len_utf8()..];
            continue;
        };
        layout = &layout[length..];
        match token {
            Token::Year => year = i32::try_from(digits(&mut value, 4, true)?).ok()?,
            Token::Year2 => {
                let y = digits(&mut value, 2, true)?;
                year = if y >= 69 {
                    1900 + i32::try_from(y).ok()?
                } else {
                    2000 + i32::try_from(y).ok()?
                };
            }
            Token::MonthName(long) => month = Some(name(&mut value, &layout::MONTHS, long)? + 1),
            Token::Month(zero) => {
                let m = digits(&mut value, 2, zero)?;
                if !(1..=12).contains(&m) {
                    return None;
                }
                month = Some(m);
            }
            Token::Weekday(long) => {
                name(&mut value, &layout::DAYS, long)?;
            }
            Token::Day(width) => {
                if width == 1 {
                    value = value.strip_prefix(' ').unwrap_or(value);
                }
                day = Some(digits(&mut value, 2, width == 2)?);
            }
            Token::YearDay(space) => {
                if space {
                    for _ in 0..2 {
                        value = value.strip_prefix(' ').unwrap_or(value);
                    }
                }
                year_day = Some(digits(&mut value, 3, !space)?);
            }
            Token::Hour(twelve, zero) => {
                hour = digits(&mut value, 2, zero && twelve)?;
                if hour >= 24 || (twelve && hour > 12) {
                    return None;
                }
            }
            Token::Minute(zero) => {
                minute = digits(&mut value, 2, zero)?;
                if minute >= 60 {
                    return None;
                }
            }
            Token::Second(zero) => {
                second = digits(&mut value, 2, zero)?;
                if second >= 60 {
                    return None;
                }
                if fraction_starts(value) && !next_fraction(layout) {
                    nanos = fraction(&mut value, None)?;
                }
            }
            Token::AmPm(upper) => {
                let (am, pm) = if upper { ("AM", "PM") } else { ("am", "pm") };
                if let Some(rest) = value.strip_prefix(am) {
                    value = rest;
                    am_pm = Some(false);
                } else {
                    value = value.strip_prefix(pm)?;
                    am_pm = Some(true);
                }
            }
            Token::Zone => {
                if let Some(rest) = value.strip_prefix("UTC") {
                    explicit_utc = true;
                    value = rest;
                } else {
                    let length = zone_name_length(value)?;
                    abbreviation = value[..length].into();
                    value = &value[length..];
                }
            }
            Token::Offset {
                z,
                colon,
                seconds,
                short,
            } => {
                if z && value.starts_with('Z') {
                    explicit_utc = true;
                    value = &value[1..];
                    continue;
                }
                let sign = if let Some(rest) = value.strip_prefix('+') {
                    value = rest;
                    1
                } else {
                    value = value.strip_prefix('-')?;
                    -1
                };
                let hr = digits(&mut value, 2, true)?;
                let min = if short {
                    0
                } else {
                    if colon {
                        value = value.strip_prefix(':')?;
                    }
                    digits(&mut value, 2, true)?
                };
                let sec = if seconds {
                    if colon {
                        value = value.strip_prefix(':')?;
                    }
                    digits(&mut value, 2, true)?
                } else {
                    0
                };
                if hr > 24 || min > 60 || sec > 60 {
                    return None;
                }
                offset = Some(sign * i32::try_from(hr * 3600 + min * 60 + sec).ok()?);
            }
            Token::Fraction { digits, trim, .. } => {
                if trim && !fraction_starts(value) {
                    continue;
                }
                nanos = fraction(&mut value, (!trim).then_some(digits))?;
            }
        }
    }
    if !value.is_empty() {
        return None;
    }
    if let Some(pm) = am_pm {
        if pm && hour < 12 {
            hour += 12;
        } else if !pm && hour == 12 {
            hour = 0;
        }
    }
    let date = if let Some(year_day) = year_day {
        let date = chrono::NaiveDate::from_yo_opt(year, year_day)?;
        if month.is_some_and(|month| month != date.month())
            || day.is_some_and(|day| day != date.day())
        {
            return None;
        }
        date
    } else {
        chrono::NaiveDate::from_ymd_opt(year, month.unwrap_or(1), day.unwrap_or(1))?
    };
    let seconds = calendar::days(i64::from(year), date.month(), date.day()) * 86400
        + i64::from(hour * 3600 + minute * 60 + second);
    Some(resolve_location(
        seconds,
        nanos,
        Location::load(zone),
        offset,
        abbreviation,
        explicit_utc,
    ))
}

fn resolve_location(
    seconds: i64,
    nanos: u32,
    location: Location,
    offset: Option<i32>,
    abbreviation: String,
    explicit_utc: bool,
) -> TemplateTime {
    if explicit_utc {
        return TemplateTime {
            seconds,
            nanos,
            monotonic: None,
            location: Location::Utc,
        };
    }
    if let Some(offset) = offset {
        let seconds = seconds - i64::from(offset);
        let (name, actual_offset) = location.zone(seconds);
        let location =
            if offset == actual_offset && (abbreviation.is_empty() || abbreviation == name) {
                location
            } else {
                Location::fixed(abbreviation, offset)
            };
        return TemplateTime {
            seconds,
            nanos,
            monotonic: None,
            location,
        };
    }
    if !abbreviation.is_empty() {
        if let Some(offset) = super::zone::lookup_name(&location, &abbreviation, seconds) {
            return TemplateTime {
                seconds: seconds - i64::from(offset),
                nanos,
                monotonic: None,
                location,
            };
        }
        let offset = abbreviation
            .strip_prefix("GMT")
            .and_then(|value| value.parse::<i32>().ok())
            .unwrap_or(0)
            * 3600;
        return TemplateTime {
            seconds,
            nanos,
            monotonic: None,
            location: Location::fixed(abbreviation, offset),
        };
    }
    // Go time.Date resolves DST overlaps/gaps by looking up the naive UTC
    // clock first, then retrying its adjusted instant if the offset changes.
    let (_, offset) = location.zone(seconds);
    let candidate = seconds - i64::from(offset);
    let (_, adjusted) = location.zone(candidate);
    TemplateTime {
        seconds: seconds - i64::from(adjusted),
        nanos,
        monotonic: None,
        location,
    }
}

fn digits(value: &mut &str, max: usize, fixed: bool) -> Option<u32> {
    let count = value
        .as_bytes()
        .iter()
        .take(max)
        .take_while(|ch| ch.is_ascii_digit())
        .count();
    if count == 0 || (fixed && count != max) {
        return None;
    }
    let number = value[..count].parse().ok()?;
    *value = &value[count..];
    Some(number)
}
fn name<const N: usize>(value: &mut &str, names: &[&str; N], long: bool) -> Option<u32> {
    for (index, name) in names.iter().enumerate() {
        let name = if long { *name } else { &name[..3] };
        if value
            .get(..name.len())
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case(name))
        {
            *value = &value[name.len()..];
            return u32::try_from(index).ok();
        }
    }
    None
}
fn fraction_starts(value: &str) -> bool {
    value.starts_with(['.', ',']) && value.as_bytes().get(1).is_some_and(u8::is_ascii_digit)
}
fn fraction(value: &mut &str, exact: Option<usize>) -> Option<u32> {
    if !value.starts_with(['.', ',']) {
        return None;
    }
    let available = value.as_bytes()[1..]
        .iter()
        .take_while(|ch| ch.is_ascii_digit())
        .count();
    let count = exact.unwrap_or(available);
    if count == 0 || available < count {
        return None;
    }
    let significant = count.min(9);
    let digits = value[1..=significant].parse::<u32>().ok()?;
    *value = &value[count + 1..];
    Some(digits * 10_u32.pow(u32::try_from(9 - significant).ok()?))
}
fn next_fraction(mut layout: &str) -> bool {
    while !layout.is_empty() {
        if let Some((token, _)) = layout::token(layout) {
            return matches!(token, Token::Fraction { .. });
        }
        let ch = layout.chars().next().expect("nonempty layout");
        layout = &layout[ch.len_utf8()..];
    }
    false
}
fn zone_name_length(value: &str) -> Option<usize> {
    if value.starts_with("ChST") || value.starts_with("MeST") {
        return Some(4);
    }
    if let Some(rest) = value.strip_prefix("GMT") {
        return Some(3 + signed_offset_length(rest).unwrap_or(0));
    }
    if value.starts_with(['+', '-']) {
        return signed_offset_length(value);
    }
    let count = value
        .as_bytes()
        .iter()
        .take_while(|ch| ch.is_ascii_uppercase())
        .count();
    match count {
        3 => Some(3),
        4 if value.as_bytes()[3] == b'T' || value.starts_with("WITA") => Some(4),
        5 if value.as_bytes()[4] == b'T' => Some(5),
        _ => None,
    }
}
fn signed_offset_length(value: &str) -> Option<usize> {
    if !value.starts_with(['+', '-']) {
        return None;
    }
    let count = value.as_bytes()[1..]
        .iter()
        .take_while(|ch| ch.is_ascii_digit())
        .count();
    if count == 0 {
        return None;
    }
    let hours = value[1..=count].parse::<u32>().ok()?;
    (hours <= 23).then_some(count + 1)
}
