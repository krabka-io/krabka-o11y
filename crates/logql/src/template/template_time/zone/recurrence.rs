//! POSIX `TZif` footer rules, following Go1.26.5 time.tzset and `tzruleTime`.
use super::super::calendar;

#[derive(Clone, Debug)]
pub(super) struct Recurrence {
    standard: (String, i32),
    daylight: Option<((String, i32), Rule, Rule)>,
}
#[derive(Clone, Debug)]
struct Rule {
    day: Day,
    clock: i32,
}
#[derive(Clone, Debug)]
enum Day {
    Julian(u32),
    Ordinal(u32),
    Month { month: u32, week: u32, weekday: u32 },
}

impl Recurrence {
    pub(super) fn parse(mut value: &str) -> Option<Self> {
        let standard = (name(&mut value)?.into(), -offset(&mut value)?);
        if value.is_empty() || value.starts_with(',') {
            return Some(Self {
                standard,
                daylight: None,
            });
        }
        let daylight_name = name(&mut value)?.to_string();
        let daylight_offset = if value.is_empty() || value.starts_with(',') {
            standard.1 + 3600
        } else {
            -offset(&mut value)?
        };
        if value.is_empty() {
            value = ",M3.2.0,M11.1.0";
        }
        value = value.strip_prefix([',', ';'])?;
        let start = rule(&mut value)?;
        value = value.strip_prefix(',')?;
        let end = rule(&mut value)?;
        if !value.is_empty() {
            return None;
        }
        Some(Self {
            standard,
            daylight: Some(((daylight_name, daylight_offset), start, end)),
        })
    }
    // Go time.tzset returns coarse year bounds outside DST transitions.
    pub(super) fn cache_bounds(&self, last: i64, seconds: i64) -> (i64, i64) {
        let Some((daylight, start, end)) = &self.daylight else {
            return (last, i64::MAX);
        };
        let parts = calendar::parts(seconds);
        let year_seconds = i64::from(parts.yearday - 1) * 86400 + seconds % 86400;
        let year_start = seconds.wrapping_sub(year_seconds);
        let a = start.seconds(parts.year, self.standard.1);
        let b = end.seconds(parts.year, daylight.1);
        let (start, end) = (a.min(b), a.max(b));
        let (start, end) = if year_seconds < start {
            (0, start)
        } else if year_seconds >= end {
            (end, 365 * 86400)
        } else {
            (start, end)
        };
        (year_start.wrapping_add(start), year_start.wrapping_add(end))
    }
    pub(super) fn lookup_full(&self, seconds: i64) -> (String, i32, bool) {
        let Some((daylight, start, end)) = &self.daylight else {
            return (self.standard.0.clone(), self.standard.1, false);
        };
        let parts = calendar::parts(seconds);
        let year_seconds = i64::from(parts.yearday - 1) * 86400 + seconds % 86400;
        let start = start.seconds(parts.year, self.standard.1);
        let end = end.seconds(parts.year, daylight.1);
        let is_daylight = if start < end {
            year_seconds >= start && year_seconds < end
        } else {
            year_seconds < end || year_seconds >= start
        };
        if is_daylight {
            (daylight.0.clone(), daylight.1, true)
        } else {
            (self.standard.0.clone(), self.standard.1, false)
        }
    }
}

impl Rule {
    fn seconds(&self, year: i64, offset: i32) -> i64 {
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let days = match self.day {
            Day::Ordinal(day) => i64::from(day),
            Day::Julian(day) => i64::from(day - 1 + u32::from(leap && day >= 60)),
            Day::Month {
                month,
                week,
                weekday,
            } => {
                let first = calendar::days(year, month, 1);
                let first_weekday =
                    u32::try_from((first + 4).rem_euclid(7)).expect("weekday is in 0..7");
                let mut day = (weekday + 7 - first_weekday) % 7 + 7 * (week - 1);
                let month_days = match month {
                    2 => {
                        if leap {
                            29
                        } else {
                            28
                        }
                    }
                    4 | 6 | 9 | 11 => 30,
                    _ => 31,
                };
                if day >= month_days {
                    day -= 7;
                }
                first - calendar::days(year, 1, 1) + i64::from(day)
            }
        };
        days * 86400 + i64::from(self.clock - offset)
    }
}

fn name<'a>(value: &mut &'a str) -> Option<&'a str> {
    if let Some(rest) = value.strip_prefix('<') {
        let end = rest.find('>')?;
        *value = &rest[end + 1..];
        Some(&rest[..end])
    } else {
        let length = value
            .find(|ch: char| ch.is_ascii_digit() || matches!(ch, ',' | '-' | '+'))
            .unwrap_or(value.len());
        if length < 3 {
            return None;
        }
        let result = &value[..length];
        *value = &value[length..];
        Some(result)
    }
}
fn number(value: &mut &str, min: u32, max: u32) -> Option<u32> {
    let count = value
        .as_bytes()
        .iter()
        .take_while(|byte| byte.is_ascii_digit())
        .count();
    if count == 0 {
        return None;
    }
    let number = value[..count].parse::<u32>().ok()?;
    if !(min..=max).contains(&number) {
        return None;
    }
    *value = &value[count..];
    Some(number)
}
fn offset(value: &mut &str) -> Option<i32> {
    let sign = if let Some(rest) = value.strip_prefix('-') {
        *value = rest;
        -1
    } else {
        *value = value.strip_prefix('+').unwrap_or(value);
        1
    };
    let mut seconds = number(value, 0, 168)? * 3600;
    if let Some(rest) = value.strip_prefix(':') {
        *value = rest;
        seconds += number(value, 0, 59)? * 60;
        if let Some(rest) = value.strip_prefix(':') {
            *value = rest;
            seconds += number(value, 0, 59)?;
        }
    }
    Some(sign * i32::try_from(seconds).expect("parsed zone offset is at most 168 hours"))
}
fn rule(value: &mut &str) -> Option<Rule> {
    let day = if let Some(rest) = value.strip_prefix('J') {
        *value = rest;
        Day::Julian(number(value, 1, 365)?)
    } else if let Some(rest) = value.strip_prefix('M') {
        *value = rest;
        let month = number(value, 1, 12)?;
        *value = value.strip_prefix('.')?;
        let week = number(value, 1, 5)?;
        *value = value.strip_prefix('.')?;
        Day::Month {
            month,
            week,
            weekday: number(value, 0, 6)?,
        }
    } else {
        Day::Ordinal(number(value, 0, 365)?)
    };
    let clock = if let Some(rest) = value.strip_prefix('/') {
        *value = rest;
        offset(value)?
    } else {
        7200
    };
    Some(Rule { day, clock })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn invalid_footer_cannot_be_silently_interpreted_as_fixed_time() {
        for footer in [
            "",
            "AB0",
            "UTC169",
            "EST5EDT,M13.2.0,M11.1.0",
            "EST5EDT,J0,J365",
            "EST5EDT,366,0",
            "EST5EDT,M3.2.0/2:60,M11.1.0",
            "EST5EDT,M3.2.0,M11.1.0junk",
        ] {
            assert2::check!(Recurrence::parse(footer).is_none());
        }
    }
}
