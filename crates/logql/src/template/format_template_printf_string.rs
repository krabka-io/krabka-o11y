use super::*;

pub(crate) fn format_template_printf_string(
    value: &str,
    width: Option<usize>,
    precision: Option<usize>,
    left_align: bool,
) -> String {
    let mut rendered = precision.map_or_else(
        || value.to_string(),
        |precision| value.chars().take(precision).collect(),
    );
    let Some(width) = width else {
        return rendered;
    };

    let len = rendered.chars().count();
    if len >= width {
        return rendered;
    }
    let padding = " ".repeat(width - len);
    if left_align {
        rendered.push_str(&padding);
        rendered
    } else {
        format!("{padding}{rendered}")
    }
}
