use super::*;

pub(crate) fn parse_template_bound(value: &TemplateRuntimeValue) -> Option<usize> {
    value.as_rendered_string().parse::<usize>().ok()
}
