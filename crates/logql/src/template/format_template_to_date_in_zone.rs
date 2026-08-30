use super::*;

pub(crate) fn format_template_to_date_in_zone(args: &[String]) -> String {
    if args.len() < 3 {
        return String::new();
    }
    parse_go_time_layout_to_unix_nanos(&args[0], &args[1], &args[2])
}
