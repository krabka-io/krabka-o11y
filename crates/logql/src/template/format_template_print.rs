use super::TemplateRuntimeValue;

pub(crate) fn format_template_print(args: &[TemplateRuntimeValue], newline: bool) -> String {
    String::from_utf8_lossy(&format_template_print_bytes(args, newline)).into_owned()
}

pub(crate) fn format_template_print_bytes(args: &[TemplateRuntimeValue], newline: bool) -> Vec<u8> {
    let mut rendered = Vec::new();
    let mut previous_was_string = false;
    for (index, arg) in args.iter().enumerate() {
        let current_is_string = arg.is_template_string();
        if index > 0 && (newline || (!previous_was_string && !current_is_string)) {
            rendered.push(b' ');
        }
        rendered.extend(arg.rendered_bytes());
        previous_was_string = current_is_string;
    }
    if newline {
        rendered.push(b'\n');
    }
    rendered
}
