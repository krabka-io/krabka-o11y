
/// The size units that the `LogQL` grammar itself admits.
///
/// The table is here and not in `krabka_units::parse::byte_size` because Loki
/// matches these units case-sensitively. Loki accepts `KiB`, `kB`, `KB`, and
/// `MB`, and it rejects `kib` and `mb`. The shared parser is case-insensitive,
/// and its use would widen the query language that this crate is a compatible
/// front-end for.
pub(crate) fn bytes_unit_multiplier(unit: &str) -> Option<f64> {
    match unit {
        "" | "B" => Some(1.0),
        "kB" | "KB" => Some(1_000.0),
        "MB" => Some(1_000_000.0),
        "GB" => Some(1_000_000_000.0),
        "TB" => Some(1_000_000_000_000.0),
        "KiB" => Some(1024.0),
        "MiB" => Some(1_048_576.0),
        "GiB" => Some(1_073_741_824.0),
        "TiB" => Some(1_099_511_627_776.0),
        _ => None,
    }
}
