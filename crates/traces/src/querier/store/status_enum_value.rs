pub(crate) fn status_enum_value(name: &str) -> Option<i32> {
    match name {
        "unset" => Some(0),
        "ok" => Some(1),
        "error" => Some(2),
        _ => None,
    }
}
