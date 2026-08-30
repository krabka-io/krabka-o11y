use super::{template_float_args, format_template_float};

pub(crate) fn format_template_float_sum(args: &[String]) -> String {
    let Some(values) = template_float_args(args) else {
        return String::new();
    };
    format_template_float(values.into_iter().sum())
}
