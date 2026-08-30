use super::*;

pub(crate) fn calendar_function(name: &str) -> Option<CalendarFn> {
    Some(match name {
        "year" => CalendarFn::Year,
        "month" => CalendarFn::Month,
        "day_of_month" => CalendarFn::DayOfMonth,
        "day_of_week" => CalendarFn::DayOfWeek,
        "day_of_year" => CalendarFn::DayOfYear,
        "days_in_month" => CalendarFn::DaysInMonth,
        "hour" => CalendarFn::Hour,
        "minute" => CalendarFn::Minute,
        _ => return None,
    })
}
