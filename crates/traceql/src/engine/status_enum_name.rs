use super::*;

pub(crate) fn status_enum_name(code: i32) -> &'static str {
    match code {
        1 => "ok",
        2 => "error",
        _ => "unset",
    }
}
