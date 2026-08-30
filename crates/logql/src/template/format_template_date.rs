use super::*;

pub(crate) fn format_template_date(args: &[String]) -> String {
    if args.len() < 2 {
        return String::new();
    }
    let Ok(timestamp_ns) = args[1].parse::<i128>() else {
        return String::new();
    };
    let Ok(timestamp) = OffsetDateTime::from_unix_timestamp_nanos(timestamp_ns) else {
        return String::new();
    };
    format_go_time_layout(&args[0], timestamp)
}
