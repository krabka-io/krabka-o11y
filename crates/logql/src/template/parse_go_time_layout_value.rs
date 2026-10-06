use super::{ParsedTemplateDate, TemplateTime};

pub(crate) fn parse_go_time_layout_value(layout: &str, value: &str) -> Option<ParsedTemplateDate> {
    let parsed = TemplateTime::parse(layout, "UTC", value)?;
    let (year, month, day, hour, minute, second, nanosecond, offset) = parsed.parsed_fields();
    Some(ParsedTemplateDate {
        year,
        month,
        day,
        hour,
        minute,
        second,
        nanosecond,
        offset_seconds: Some(offset),
    })
}
