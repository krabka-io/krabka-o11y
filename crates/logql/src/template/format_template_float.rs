use super::*;

pub(crate) fn format_template_float(value: f64) -> String {
    if value == 0.0 {
        "0".to_string()
    } else {
        value.to_string()
    }
}
