use super::*;

pub(crate) fn kind_enum_name(code: i32) -> &'static str {
    match code {
        1 => "internal",
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => "unspecified",
    }
}
