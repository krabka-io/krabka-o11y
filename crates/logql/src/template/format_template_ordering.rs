use super::*;

pub(crate) fn format_template_ordering(args: &[String], predicate: impl FnOnce(Ordering) -> bool) -> String {
    if args.len() < 2 {
        return "false".to_string();
    }
    template_compare_values(&args[0], &args[1])
        .is_some_and(predicate)
        .to_string()
}
