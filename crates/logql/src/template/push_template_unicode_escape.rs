use super::*;

pub(crate) fn push_template_unicode_escape(output: &mut String, value: u32) {
    let _ = write!(output, "\\u{value:04X}");
}
