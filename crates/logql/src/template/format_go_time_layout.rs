use super::{OffsetDateTime, TemplateTime};

pub(crate) fn format_go_time_layout(layout: &str, timestamp: OffsetDateTime) -> String {
    TemplateTime::from_offset(
        timestamp.unix_timestamp(),
        timestamp.nanosecond(),
        timestamp.offset().whole_seconds(),
    )
    .format(layout)
}
