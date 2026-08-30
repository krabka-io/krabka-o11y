use super::*;

pub(crate) fn parse_go_time_layout_to_unix_nanos(layout: &str, zone: &str, value: &str) -> String {
    let Some(parsed) = parse_go_time_layout_value(layout, value) else {
        return String::new();
    };
    let Some(date) = NaiveDate::from_ymd_opt(parsed.year, parsed.month, parsed.day) else {
        return String::new();
    };
    let Some(time) =
        NaiveTime::from_hms_nano_opt(parsed.hour, parsed.minute, parsed.second, parsed.nanosecond)
    else {
        return String::new();
    };
    let datetime = NaiveDateTime::new(date, time);
    let Some(utc_datetime) = resolve_template_datetime(datetime, zone, parsed.offset_seconds)
    else {
        return String::new();
    };
    utc_datetime
        .timestamp_nanos_opt()
        .map_or_else(String::new, |value| value.to_string())
}
