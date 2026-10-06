use super::TemplateTime;

pub(crate) fn parse_go_time_layout_to_unix_nanos(layout: &str, zone: &str, value: &str) -> String {
    TemplateTime::parse(layout, zone, value)
        .unwrap_or_else(TemplateTime::zero)
        .unix_nanos()
        .to_string()
}
