use super::*;

pub(crate) fn template_string_truthy(value: &str) -> bool {
    !matches!(value, "" | "false" | "0")
}
