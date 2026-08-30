use super::*;

pub(crate) fn format_template_print(args: &[TemplateRuntimeValue], newline: bool) -> String {
    let mut rendered = String::new();
    let mut previous_was_string = false;
    for (index, arg) in args.iter().enumerate() {
        let current_is_string = arg.is_template_string();
        if index > 0 && (newline || (!previous_was_string && !current_is_string)) {
            rendered.push(' ');
        }
        rendered.push_str(&arg.as_rendered_string());
        previous_was_string = current_is_string;
    }
    if newline {
        rendered.push('\n');
    }
    rendered
}
