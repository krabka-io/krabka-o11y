/// Gregorian civil fields. Go's absolute epoch is March1 of year-292277022400
/// (Go1.26.5 src/time/time.go absoluteYears/absSec). Its unsigned arithmetic is
/// observable near i64 Unix limits, so preserve it before civil decomposition.
pub(super) struct Parts {
    pub year: i64,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub weekday: u32,
    pub yearday: u32,
}

pub(super) fn days(year: i64, month: u32, day: u32) -> i64 {
    let year = year - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let yoe = year - era * 400;
    let month = i64::from(month) + if month > 2 { -3 } else { 9 };
    let doy = (153 * month + 2) / 5 + i64::from(day) - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

pub(super) fn parts(seconds: i64) -> Parts {
    let absolute_base = u64::try_from(-i128::from(days(-292_277_022_400, 3, 1)) * 86400)
        .expect("Go absolute epoch fits u64");
    let absolute = u64::from_ne_bytes(seconds.to_ne_bytes()).wrapping_add(absolute_base);
    let day = i128::from(absolute / 86400) - i128::from(absolute_base / 86400);
    let clock = u32::try_from(absolute % 86400).expect("clock remainder is below 86400");
    let civil = day + 719_468;
    let era = civil.div_euclid(146_097);
    let doe = civil - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let mut year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day_of_month = doy - (153 * mp + 2) / 5 + 1;
    let month = mp + if mp < 10 { 3 } else { -9 };
    year += i128::from(month <= 2);
    let year = i64::try_from(year).expect("u64 absolute seconds bound the civil year");
    Parts {
        year,
        month: u32::try_from(month).expect("civil month is in 1..=12"),
        day: u32::try_from(day_of_month).expect("civil day is in 1..=31"),
        hour: clock / 3600,
        minute: clock % 3600 / 60,
        second: clock % 60,
        weekday: u32::try_from((day + 4).rem_euclid(7)).expect("weekday is in 0..7"),
        yearday: u32::try_from(day - i128::from(days(year, 1, 1)) + 1)
            .expect("year day is in 1..=366"),
    }
}

/// Go dateToAbsDays uses unsigned civil arithmetic across its full second domain.
pub(super) fn date_seconds(
    year: i64,
    month: u32,
    day: i64,
    hour: u32,
    minute: u32,
    second: u32,
) -> i64 {
    let jan_feb = u64::from(month < 3);
    let month = u64::from(month) + 12 * jan_feb;
    let year = u64::from_ne_bytes(year.to_ne_bytes())
        .wrapping_sub(jan_feb)
        .wrapping_add(292_277_022_400);
    let century = year / 100;
    let days = (century.wrapping_mul(146_097) / 4)
        .wrapping_add(1461 * (year % 100) / 4)
        .wrapping_add((979 * month - 2919) >> 5)
        .wrapping_add(u64::from_ne_bytes(day.to_ne_bytes()))
        .wrapping_sub(1);
    let seconds = days
        .wrapping_mul(86400)
        .wrapping_add(u64::from(hour) * 3600 + u64::from(minute) * 60 + u64::from(second))
        .wrapping_sub(9_223_372_028_741_760_000);
    i64::from_ne_bytes(seconds.to_ne_bytes())
}
