use super::*;

/// Maps a `PromQL` calendar-function name to its `CalendarFn` variant.
///
/// The mapping mirrors the calendar arms of `PromqlEngine::eval_instant_call`.
/// This function returns `None` for any other function, so the planner dispatch
/// falls through.
pub(crate) fn calendar_fn_from_function_name(name: &str) -> Option<CalendarFn> {
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
