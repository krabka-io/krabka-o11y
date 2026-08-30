use super::*;

pub(crate) fn detected_bytes_unit(unit: &str) -> Option<()> {
    match unit {
        "B" | "kB" | "KB" | "MB" | "GB" | "TB" | "KiB" | "MiB" | "GiB" | "TiB" => Some(()),
        _ => None,
    }
}
