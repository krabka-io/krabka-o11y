use super::*;

#[derive(Clone, Copy)]
pub(crate) enum CalendarFn {
    Year,
    Month,
    DayOfMonth,
    DayOfWeek,
    DayOfYear,
    DaysInMonth,
    Hour,
    Minute,
}

impl CalendarFn {
    pub(crate) fn apply(self, unix_seconds: f64) -> f64 {
        if !unix_seconds.is_finite() {
            return f64::NAN;
        }
        let Some(unix_seconds) = unix_seconds.to_i64() else {
            return f64::NAN;
        };
        let Ok(timestamp) = OffsetDateTime::from_unix_timestamp(unix_seconds) else {
            return f64::NAN;
        };
        match self {
            Self::Year => f64::from(timestamp.year()),
            Self::Month => f64::from(timestamp.month() as u8),
            Self::DayOfMonth => f64::from(timestamp.day()),
            Self::DayOfWeek => f64::from(timestamp.weekday().number_days_from_sunday()),
            Self::DayOfYear => f64::from(timestamp.ordinal()),
            Self::DaysInMonth => {
                f64::from(days_in_month(timestamp.year(), timestamp.month() as u8))
            }
            Self::Hour => f64::from(timestamp.hour()),
            Self::Minute => f64::from(timestamp.minute()),
        }
    }
}
