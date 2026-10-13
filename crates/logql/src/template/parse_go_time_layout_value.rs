use super::{ParsedTemplateDate, TemplateTime};

pub(crate) fn parse_go_time_layout_value(layout: &str, value: &str) -> Option<ParsedTemplateDate> {
    let parsed = TemplateTime::parse(layout, "UTC", value)?;
    let fields = parsed.parsed_fields();
    let year = fields.year;
    let month = fields.month;
    let day = fields.day;
    let hour = fields.hour;
    let minute = fields.minute;
    let second = fields.second;
    let nanosecond = fields.nanosecond;
    let offset = fields.offset;
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
