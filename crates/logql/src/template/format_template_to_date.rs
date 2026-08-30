use super::parse_go_time_layout_to_unix_nanos;

pub(crate) fn format_template_to_date(args: &[String]) -> String {
    if args.len() < 2 {
        return String::new();
    }
    parse_go_time_layout_to_unix_nanos(&args[0], "Local", &args[1])
}
